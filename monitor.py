# -*- coding: utf-8 -*-
"""
时代峰峻商城 SKU 状态监控脚本（第一版）

作用：每隔一段时间（默认 60 秒）请求一次商品的款式（SKU）信息，
      把每个款的状态记录到 data/snapshots.csv，
      状态有变化时记录到 data/events.csv，并可通过 Bark 推送到手机。

运行：python monitor.py
停止：在窗口里按 Ctrl + C

只依赖 Python 标准库 和 requests（pip install requests）。
"""

import csv
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

import requests

# ============================================================
# 基本设置（一般不需要改）
# ============================================================

# 脚本所在的文件夹。config.json 和 data 文件夹都放在这里。
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

API_URL = "https://app.tfent.cn/appmobile/goods/shelves/getShelvesSku/{goods_id}"

# 请求头（照抄 App 的请求）。Authorization 会在有 token 时自动加上。
BASE_HEADERS = {
    "Host": "app.tfent.cn",
    "isStar": "0",
    "Accept": "*/*",
    "Client-Type": "iOS",
    "subjectId": "",
    "sysCode": "tf",
    "Client-Version": "5.2.3",
    "Accept-Language": "zh-Hans-CN;q=1, en-CN;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "User-Agent": "TFFanclub/5.2.3 (iPhone; iOS 17.6.1; Scale/3.00)",
}

REQUEST_TIMEOUT = 20          # 单次请求最多等多少秒
WAIT_AFTER_429 = 5 * 60       # 被限流（429）后等待 5 分钟
WAIT_AFTER_AUTH_FAIL = 10 * 60  # token 失效（401/403）后每 10 分钟重试一次
FAILS_BEFORE_PUSH = 5         # 连续失败几次后推送提醒

# 需要比较变化的字段
WATCH_FIELDS = ["stock", "sellOut", "hotSell", "sellNum", "putStatus", "appletPutStatus"]

# 字段的中文名（用于终端显示和推送）
FIELD_NAMES = {
    "stock": "库存",
    "sellOut": "售罄",
    "hotSell": "热度",
    "sellNum": "销量",
    "putStatus": "上架状态",
    "appletPutStatus": "小程序上架状态",
    "sku": "款式",
}

# 北京时间（UTC+8）。不用时区库，Windows 上也不会出问题。
BEIJING = timezone(timedelta(hours=8))

# CSV 表头
SNAPSHOT_HEADER = [
    "本地时间(北京)", "serverSystemTime", "响应头Date",
    "goodsId", "商品名", "商品putStatus", "startSelling",
    "skuId", "skuName",
    "stock", "sellOut", "hotSell", "sellNum", "putStatus", "appletPutStatus",
    "HTTP状态码", "耗时ms", "X-RateLimit-Remaining", "EO-Cache-Status",
]
EVENT_HEADER = [
    "本次本地时间", "本次serverSystemTime",
    "上次成功本地时间", "上次成功serverSystemTime",
    "goodsId", "商品名", "skuId", "skuName",
    "字段", "旧值", "新值",
]


# ============================================================
# 小工具
# ============================================================

def now_beijing():
    """返回当前北京时间字符串，精确到毫秒，例如 2026-11-02 12:25:28.123"""
    return datetime.now(BEIJING).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def now_for_filename():
    """用于文件名的时间戳，例如 20261102_122528_123"""
    return datetime.now(BEIJING).strftime("%Y%m%d_%H%M%S_%f")[:-3]


# 颜色（让变化在终端里更醒目）
RED = "\033[91m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RESET = "\033[0m"


def enable_windows_colors():
    """让 Windows 命令提示符支持彩色文字（Win10 及以上）。"""
    if os.name == "nt":
        os.system("")


def short_sku_name(sku_name):
    """'时代少年团:马嘉祺款' -> '马嘉祺'，让终端显示短一点。"""
    name = str(sku_name or "")
    for sep in (":", "："):
        if sep in name:
            name = name.split(sep)[-1]
    if name.endswith("款") and len(name) > 1:
        name = name[:-1]
    return name or str(sku_name)


def short_goods_name(goods_name):
    """去掉开头的【...】标签，例如 '【双十一活动商品】时代少年团特别纪念明信片' -> '时代少年团特别纪念明信片'"""
    return re.sub(r"^【[^】]*】", "", str(goods_name or "")).strip() or str(goods_name)


def log_error(message):
    """把错误写进 data/errors.log，同时在终端用红色显示。"""
    line = f"[{now_beijing()}] {message}"
    print(f"{RED}{line}{RESET}")
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(os.path.join(DATA_DIR, "errors.log"), "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass  # 连日志都写不了，就只在终端显示，绝不让脚本崩溃


# 如果 CSV 正被 Excel 打开（Windows 会锁住文件），写不进去的行先暂存在这里，
# 等文件关闭后下次自动补写，不会丢数据。
_pending_rows = {}


def append_csv(filename, header, rows):
    """往 data/ 下的 CSV 文件追加若干行。文件不存在时先写表头。
    使用 UTF-8 with BOM 编码，Excel 打开不会乱码。"""
    path = os.path.join(DATA_DIR, filename)
    pending = _pending_rows.setdefault(filename, [])
    pending.extend(rows)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        is_new = (not os.path.exists(path)) or os.path.getsize(path) == 0
        with open(path, "a", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            if is_new:
                writer.writerow(header)
            writer.writerows(pending)
        pending.clear()
    except PermissionError:
        log_error(f"{filename} 被其他程序（可能是 Excel）占用，"
                  f"已暂存 {len(pending)} 行，关闭文件后会自动补写")
    except Exception as e:
        log_error(f"写入 {filename} 失败（已暂存，稍后重试）：{e}")


def save_raw_json(goods_id, body, tag):
    """把完整的原始返回 JSON 存到 data/raw/ 下，文件名带时间戳。"""
    try:
        raw_dir = os.path.join(DATA_DIR, "raw")
        os.makedirs(raw_dir, exist_ok=True)
        path = os.path.join(raw_dir, f"{goods_id}_{now_for_filename()}_{tag}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(body, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log_error(f"保存原始 JSON 失败：{e}")


# ============================================================
# 读取配置
# ============================================================

def load_config():
    """读取 config.json。出错时给出中文提示并退出。"""
    if not os.path.exists(CONFIG_PATH):
        print(f"{RED}找不到 config.json！{RESET}")
        print("请把 config.example.json 复制一份，改名为 config.json，再按 README 填写。")
        input("按回车键退出...")
        sys.exit(1)
    try:
        # utf-8-sig：兼容 Windows 记事本保存时带的 BOM
        with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        print(f"{RED}config.json 格式有误：第 {e.lineno} 行附近。{RESET}")
        print("常见原因：少了英文逗号/引号，或用了中文引号。")
        input("按回车键退出...")
        sys.exit(1)

    token = str(cfg.get("token") or "").strip()
    if token.lower().startswith("bearer "):   # 防止把 "Bearer " 也粘贴进来
        token = token[7:].strip()

    bark_key = str(cfg.get("bark_key") or "").strip()
    # 防止把整个 Bark 链接粘贴进来，例如 https://api.day.app/abcd1234/
    if "api.day.app/" in bark_key:
        bark_key = bark_key.split("api.day.app/")[1].split("/")[0]

    goods_ids = cfg.get("goods_ids") or []
    if not goods_ids:
        print(f"{RED}config.json 里的 goods_ids 是空的，请至少填一个商品 ID。{RESET}")
        input("按回车键退出...")
        sys.exit(1)

    interval = cfg.get("interval_seconds") or 60
    try:
        interval = max(1, int(interval))    # 最少 1 秒，防止误填 0 或负数
    except (TypeError, ValueError):
        interval = 60

    return {
        "token": token,
        "goods_ids": [str(g).strip() for g in goods_ids],
        "interval_seconds": interval,
        "bark_key": bark_key,
    }


# ============================================================
# Bark 推送
# ============================================================

def bark_push(config, title, body, urgent=False):
    """发送 Bark 推送。没填 bark_key 就什么都不做。推送失败只记日志，不影响监控。"""
    key = config.get("bark_key")
    if not key:
        return
    payload = {"device_key": key, "title": title, "body": body, "group": "时代峰峻监控"}
    if urgent:
        payload["level"] = "timeSensitive"   # 时效性通知，专注模式下也能弹出
    try:
        r = requests.post("https://api.day.app/push", json=payload, timeout=10)
        if r.status_code != 200:
            log_error(f"Bark 推送失败：HTTP {r.status_code}")
    except Exception as e:
        log_error(f"Bark 推送失败：{type(e).__name__}")


# ============================================================
# 发送请求
# ============================================================

def fetch_goods(goods_id, token):
    """请求一次商品 SKU 信息。

    返回一个字典，其中 kind 表示结果类型：
      "ok"        成功，data 里是商品数据
      "auth"      401/403，token 可能过期
      "ratelimit" 429，请求太频繁
      "error"     其他任何失败（网络、超时、5xx、code 不是 200、数据格式不对…）
    除了 "ok"，其他情况都【绝不能】当成无货/售罄。
    """
    headers = dict(BASE_HEADERS)
    if token:
        headers["Authorization"] = "Bearer " + token

    result = {"kind": "error", "http_status": "", "elapsed_ms": "", "date": "",
              "ratelimit": "", "cache": "", "body": None, "data": None, "message": ""}

    start = time.perf_counter()
    try:
        resp = requests.get(API_URL.format(goods_id=goods_id), headers=headers,
                            timeout=REQUEST_TIMEOUT)
    except requests.Timeout:
        result["message"] = "请求超时"
        return result
    except requests.RequestException as e:
        result["message"] = f"网络错误（{type(e).__name__}）"
        return result
    result["elapsed_ms"] = int((time.perf_counter() - start) * 1000)

    status = resp.status_code
    result["http_status"] = status
    result["date"] = resp.headers.get("Date", "")
    result["ratelimit"] = resp.headers.get("X-RateLimit-Remaining", "")
    result["cache"] = resp.headers.get("EO-Cache-Status", "")

    if status in (401, 403):
        result["kind"] = "auth"
        result["message"] = f"HTTP {status}：token 可能已过期（或请求被拦截）"
        return result
    if status == 429:
        result["kind"] = "ratelimit"
        result["message"] = "HTTP 429：请求太频繁，被限流"
        return result
    if status != 200:
        result["message"] = f"HTTP {status}：服务器返回异常 {resp.text[:200]!r}"
        return result

    try:
        body = resp.json()
    except ValueError:
        result["message"] = f"返回内容不是 JSON：{resp.text[:200]!r}"
        return result
    result["body"] = body

    if not isinstance(body, dict):
        result["message"] = "返回的 JSON 格式不对"
        return result
    code = body.get("code")
    if code in (401, 403):
        result["kind"] = "auth"
        result["message"] = f"接口 code={code}：token 可能已过期 msg={body.get('msg')!r}"
        return result
    if code != 200:
        result["message"] = f"接口 code={code}（不是 200）msg={body.get('msg')!r}"
        return result

    data = body.get("data")
    if not isinstance(data, dict) or not isinstance(data.get("goodsSkuList"), list):
        result["message"] = "返回数据里没有 goodsSkuList"
        return result
    if len(data["goodsSkuList"]) == 0:
        result["message"] = "goodsSkuList 是空列表（不当作售罄处理）"
        return result

    result["kind"] = "ok"
    result["data"] = data
    return result


# ============================================================
# 处理一次成功的结果
# ============================================================

def describe_change(field, old, new):
    """把一个变化翻译成好懂的文字，用于终端和推送。"""
    text = f"{FIELD_NAMES.get(field, field)} {old} → {new}"
    if field == "sellOut":
        if str(new) == "1":
            text += "（售罄）"
        elif str(new) == "0":
            text += "（回流/补货！）"
    return text


def is_unusual_stock(value):
    """stock 不是 0 也不是 999 —— 这是这次测试最关心的情况。"""
    return value is not None and str(value) not in ("0", "999")


def handle_success(goods_id, result, config, state):
    local_time = now_beijing()
    data = result["data"]
    server_time = data.get("serverSystemTime") or ""
    goods_name = data.get("name") or ""

    # ---- 1. 整理当前每个款的状态，并写入 snapshots.csv ----
    current = {}   # skuId -> {字段: 值}
    rows = []
    for sku in data["goodsSkuList"]:
        if not isinstance(sku, dict):
            continue
        sku_id = str(sku.get("id"))
        info = {f: sku.get(f) for f in WATCH_FIELDS}
        info["skuName"] = sku.get("skuName") or ""
        current[sku_id] = info
        rows.append([
            local_time, server_time, result["date"],
            data.get("goodsId", goods_id), goods_name, data.get("putStatus"), data.get("startSelling"),
            sku_id, info["skuName"],
            info["stock"], info["sellOut"], info["hotSell"], info["sellNum"],
            info["putStatus"], info["appletPutStatus"],
            result["http_status"], result["elapsed_ms"], result["ratelimit"], result["cache"],
        ])
    append_csv("snapshots.csv", SNAPSHOT_HEADER, rows)

    # ---- 2. 和上一次成功的结果比较 ----
    prev = state["last"].get(goods_id)
    changes = []   # (skuId, skuName, 字段, 旧值, 新值)
    if prev is None:
        print(f"{CYAN}[{goods_id}] 启动后的第一次成功请求，作为基准，不算变化。{RESET}")
        save_raw_json(goods_id, result["body"], "baseline")
    else:
        old_skus = prev["skus"]
        for sku_id, info in current.items():
            if sku_id not in old_skus:
                changes.append((sku_id, info["skuName"], "sku", "（无）", "新出现"))
                continue
            for f in WATCH_FIELDS:
                old_v, new_v = old_skus[sku_id].get(f), info.get(f)
                if old_v != new_v:
                    changes.append((sku_id, info["skuName"], f, old_v, new_v))
        for sku_id, info in old_skus.items():
            if sku_id not in current:
                changes.append((sku_id, info["skuName"], "sku", "存在", "消失"))

    # ---- 3. 有变化：记录事件、保存原始 JSON、醒目打印、推送 ----
    if changes:
        event_rows = [[local_time, server_time, prev["local"], prev["server"],
                       goods_id, goods_name, sid, sname, f, old_v, new_v]
                      for (sid, sname, f, old_v, new_v) in changes]
        append_csv("events.csv", EVENT_HEADER, event_rows)
        save_raw_json(goods_id, result["body"], "change")

        print(f"{YELLOW}{'!' * 60}")
        print(f"  发现变化！ {short_goods_name(goods_name)}（{goods_id}）")
        print(f"  变化发生在 {prev['server']} 到 {server_time} 之间")
        for (sid, sname, f, old_v, new_v) in changes:
            print(f"  ▶ {short_sku_name(sname)}：{describe_change(f, old_v, new_v)}")
        print(f"{'!' * 60}{RESET}")

        # 推送：hotSell / sellOut / 款式增减 的变化
        push_lines = [f"{short_sku_name(sname)}：{describe_change(f, old_v, new_v)}"
                      for (sid, sname, f, old_v, new_v) in changes
                      if f in ("hotSell", "sellOut", "sku")]
        if push_lines:
            title = "回流！" if any(f == "sellOut" and str(n) == "0"
                                   for (_, _, f, _, n) in changes) else "状态变化"
            bark_push(config, f"{title} {short_goods_name(goods_name)}",
                      "\n".join(push_lines) + f"\n{server_time}")

    # ---- 4. stock 出现 0 和 999 以外的值：立刻推送（同一个值只推一次） ----
    for sku_id, info in current.items():
        stock = info["stock"]
        key = (goods_id, sku_id)
        if is_unusual_stock(stock) and state["stock_alerted"].get(key) != stock:
            state["stock_alerted"][key] = stock
            msg = f"{short_sku_name(info['skuName'])} 库存显示 {stock}"
            print(f"{RED}★★★ 特殊库存值！{short_goods_name(goods_name)} {msg} ★★★{RESET}")
            bark_push(config, f"特殊库存 {short_goods_name(goods_name)}",
                      f"{msg}\n{server_time}", urgent=True)
            if not changes:   # 如果上面没存过，这里也存一份原始 JSON 方便研究
                save_raw_json(goods_id, result["body"], "stock")
        elif not is_unusual_stock(stock):
            state["stock_alerted"].pop(key, None)

    # ---- 5. 记住这次的状态，下次用来比较 ----
    state["last"][goods_id] = {"local": local_time, "server": server_time, "skus": current}

    # ---- 6. 打印一行简短状态，说明脚本在正常运行 ----
    parts = []
    for info in current.values():
        name = short_sku_name(info["skuName"])
        if str(info["sellOut"]) == "1":
            s = f"{RED}{name}:售罄{RESET}"
        else:
            s = f"{GREEN}{name}:有货{RESET}"
        if info["hotSell"] not in (0, None):
            s += f"{YELLOW}热{info['hotSell']}{RESET}"
        if is_unusual_stock(info["stock"]):
            s += f"{RED}[库存{info['stock']}]{RESET}"
        parts.append(s)
    print(f"{server_time} [{goods_id}] " + " | ".join(parts)
          + f"  ({result['elapsed_ms']}ms, 剩余额度{result['ratelimit'] or '?'})")


# ============================================================
# 检查一个商品（包含错误处理）
# ============================================================

def check_goods(goods_id, config, state):
    """检查一个商品。返回值：之后至少需要等待的秒数（0 表示按正常间隔）。"""
    result = fetch_goods(goods_id, config["token"])

    if result["kind"] == "ok":
        # 如果之前出过问题并推送过，告诉一声已经恢复
        if state["fail_pushed"] or state["auth_pushed"]:
            print(f"{GREEN}请求已恢复正常。{RESET}")
            bark_push(config, "监控已恢复", f"请求恢复正常\n{now_beijing()[:19]}")
        state["fail_count"] = 0
        state["fail_pushed"] = False
        state["auth_pushed"] = False
        handle_success(goods_id, result, config, state)
        return 0

    # ---- 以下都是失败：只记录错误，不写快照、不比较变化 ----
    state["fail_count"] += 1
    log_error(f"[{goods_id}] 请求失败（连续第 {state['fail_count']} 次）：{result['message']}")

    if result["kind"] == "auth":
        if not state["auth_pushed"]:
            state["auth_pushed"] = True
            bark_push(config, "token 可能失效",
                      f"{result['message']}\n请更新 config.json 里的 token 后重启脚本\n"
                      f"{now_beijing()[:19]}", urgent=True)
        print(f"{YELLOW}将在 {WAIT_AFTER_AUTH_FAIL // 60} 分钟后重试。{RESET}")
        return WAIT_AFTER_AUTH_FAIL

    if state["fail_count"] >= FAILS_BEFORE_PUSH and not state["fail_pushed"]:
        state["fail_pushed"] = True
        bark_push(config, "监控连续失败",
                  f"已连续失败 {state['fail_count']} 次：{result['message']}\n{now_beijing()[:19]}")

    if result["kind"] == "ratelimit":
        print(f"{YELLOW}被限流，将在 {WAIT_AFTER_429 // 60} 分钟后重试。{RESET}")
        return WAIT_AFTER_429
    return 0


# ============================================================
# 主循环
# ============================================================

def new_state():
    return {
        "last": {},            # 每个商品上一次成功的状态
        "fail_count": 0,       # 连续失败次数
        "fail_pushed": False,  # 连续失败是否已经推送过（避免重复推送）
        "auth_pushed": False,  # token 失效是否已经推送过
        "stock_alerted": {},   # 已经推送过的特殊库存值
    }


def main():
    enable_windows_colors()
    config = load_config()
    state = new_state()
    interval = config["interval_seconds"]

    print("=" * 60)
    print(" 时代峰峻商城 SKU 监控 已启动")
    print(f" 商品：{', '.join(config['goods_ids'])}   间隔：{interval} 秒")
    print(f" token：{'已填写' if config['token'] else '未填写（不带 token 请求）'}"
          f"   Bark 推送：{'已开启' if config['bark_key'] else '未开启'}")
    print(f" 数据保存在：{DATA_DIR}")
    print(" 按 Ctrl + C 停止")
    print("=" * 60)

    while True:
        cycle_start = time.time()
        wait = interval
        for goods_id in config["goods_ids"]:
            try:
                extra_wait = check_goods(goods_id, config, state)
            except Exception:
                # 任何意外错误都只记录，不让脚本退出
                log_error("程序内部错误：\n" + traceback.format_exc())
                extra_wait = 0
            if extra_wait > wait:
                wait = extra_wait
                break   # 被限流或 token 失效时，本轮剩下的商品也先不请求了
        # 等到下一轮（保证请求频率不超过设置的间隔）
        sleep_seconds = cycle_start + wait - time.time()
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已停止监控。数据都保存在 data 文件夹里。")
