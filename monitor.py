# -*- coding: utf-8 -*-
"""
时代峰峻商城 SKU 状态监控脚本（第二版 · 第 1 部分：分阶段请求频率）

作用：定时请求商品的款式（SKU）信息，把每个款的状态记录到 data/snapshots.csv，
      状态有变化时记录到 data/events.csv，并可通过 Bark 推送到手机。
      填了开售时间（sale_time）的商品，会在开售前后自动切换请求频率：
        等待开售 → 准备 → 平稳 → 售罄期 → 回流观察期 → 结束

运行：python monitor.py
停止：在窗口里按 Ctrl + C

只依赖 Python 标准库 和 requests（pip install requests）。
"""

import csv
import json
import os
import re
import statistics
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests

# ============================================================
# 基本设置（一般不需要改）
# ============================================================

# 脚本所在的文件夹。config.json 和 data 文件夹都放在这里。
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config.json")
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

API_URL = "https://app.tfent.cn/appmobile/goods/shelves/getShelvesSku/{goods_id}"
LIST_URL = "https://app.tfent.cn/appmobile/goods/goodsSpu/pageListForShopMall"
LIST_INTERVAL_NORMAL = 5 * 60   # 商品列表接口：平时每 5 分钟查一次
LIST_INTERVAL_BUSY = 30         # 有商品在售罄期 / 回流观察期 / 售后观察时，每 30 秒查一次

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

REQUEST_TIMEOUT = 20            # 单次请求最多等多少秒
RATE_LIMIT_SLOWDOWN = 30        # 遇到 429 后，降到每秒最多 1 次，持续 30 秒
WAIT_AFTER_AUTH_FAIL = 10 * 60  # token 失效（401/403）后每 10 分钟重试一次
FAILS_BEFORE_PUSH = 5           # 连续失败几次后推送提醒
MIN_INTERVAL = 0.2              # 任何间隔都不能小于 0.2 秒（每秒最多 5 次）

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

# 各阶段的中文名
PHASE_NAMES = {
    "normal": "正常",
    "waiting": "等待开售",
    "prepare": "准备",
    "steady": "平稳",
    "fast": "售罄期",
    "window": "回流观察期",
    "watch": "售后观察",  # 全部最终售罄后，继续低频观察有没有补货
    "done": "已结束",
    "queued": "排队中",   # 想进售罄期/回流观察期，但另一个商品正在占用
}

# launch_profile 的默认值（config.json 里没写的项就用这些）
DEFAULT_PROFILE = {
    "prepare_before_seconds": 30,
    "prepare_interval": 1,
    "steady_interval": 2,
    "fast_after_seconds": 240,
    "fast_interval": 0.2,
    "window_interval": 1,
    "after_window_interval": 5,
    "max_launch_minutes": 90,
}

# 北京时间（UTC+8）。不用时区库，Windows 上也不会出问题。
BEIJING = timezone(timedelta(hours=8))

# CSV 表头
SNAPSHOT_HEADER = [
    "发出时间(本地)", "收到时间(本地)", "serverSystemTime", "响应头Date", "阶段",
    "goodsId", "商品名", "商品putStatus", "startSelling", "sellStartTime",
    "skuId", "skuName", "状态", "inconsistent",
    "stock", "sellOut", "hotSell", "sellNum", "putStatus", "appletPutStatus",
    "HTTP状态码", "耗时ms", "X-RateLimit-Remaining", "EO-Cache-Status",
]
# data/events.csv：售罄 / 回流事件
EVENT_HEADER = [
    "记录时间(本地)", "goodsId", "商品名", "skuId", "skuName", "事件",
    "区间开始", "区间结束", "区间中点", "区间开始serverSystemTime", "区间结束serverSystemTime", "区间长度秒",
    "距开售秒数", "距最近售罄秒数", "分类", "同一分钟内回流的其他款", "updatedTime变化", "stock",
    "回流持续秒数(估算)", "回流持续最短", "回流持续最长", "inconsistent", "说明",
]
# data/changes.csv：第一版的“任何字段变化”记录（保留）
CHANGE_HEADER = [
    "本次本地时间", "本次serverSystemTime",
    "上次成功本地时间", "上次成功serverSystemTime",
    "goodsId", "商品名", "skuId", "skuName",
    "字段", "旧值", "新值",
]
PHASE_HEADER = ["本地时间", "估算服务器时间", "goodsId", "旧阶段", "新阶段", "原因"]
UPDATED_HEADER = ["发现时间(本地)", "goodsId", "商品名", "旧updatedTime", "新updatedTime",
                  "列表stock", "上次查询时间(本地)", "附近的回流"]


# ============================================================
# 时间工具
# ============================================================

def time_now():
    """当前本地时间（秒，带小数）。单独写成函数，方便测试时替换。"""
    return time.time()


def fmt_time(ts, ms=True):
    """把时间戳格式化成北京时间字符串，例如 2026-11-02 12:25:28.123"""
    s = datetime.fromtimestamp(ts, BEIJING).strftime("%Y-%m-%d %H:%M:%S.%f")
    return s[:-3] if ms else s[:19]


def now_beijing():
    return fmt_time(time_now())


def now_for_filename():
    """用于文件名的时间戳，例如 20261102_122528_123"""
    return datetime.fromtimestamp(time_now(), BEIJING).strftime("%Y%m%d_%H%M%S_%f")[:-3]


def parse_beijing(text):
    """'2026-11-11 18:00:00'（北京时间）→ 时间戳。格式不对返回 None。"""
    try:
        return datetime.strptime(str(text).strip(), "%Y-%m-%d %H:%M:%S").replace(tzinfo=BEIJING).timestamp()
    except (TypeError, ValueError):
        return None


class ServerClock:
    """估算“服务器时间 - 本地时间”的差值，用来按服务器时间判断开售。

    serverSystemTime 只精确到秒，所以每次请求得到的估算值有 ±0.5 秒误差，
    这里取最近 30 次的中位数，减小误差。"""

    def __init__(self):
        self.samples = []

    def update(self, server_time_text, date_header, send_ts, recv_ts):
        server_ts = parse_beijing(server_time_text) if server_time_text else None
        if server_ts is None and date_header:
            try:
                server_ts = parsedate_to_datetime(date_header).timestamp()
            except (TypeError, ValueError, IndexError):
                server_ts = None
        if server_ts is None:
            return
        # 服务器时间只到秒，真实时刻在 [server_ts, server_ts+1) 之间，取中间 +0.5
        self.samples.append(server_ts + 0.5 - (send_ts + recv_ts) / 2)
        self.samples = self.samples[-30:]

    @property
    def offset(self):
        return statistics.median(self.samples) if self.samples else 0.0

    def now(self):
        """估算的当前服务器时间（时间戳）"""
        return time_now() + self.offset


# ============================================================
# 小工具
# ============================================================

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
    """去掉开头的【...】标签"""
    return re.sub(r"^【[^】]*】", "", str(goods_name or "")).strip() or str(goods_name)


def to_int(value):
    """把 0 / "0" / 999 之类转成整数；缺失或不是数字就返回 None。"""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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


def _read_header(path):
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            return next(csv.reader(f), None)
    except Exception:
        return None


def append_csv(filename, header, rows):
    """往 data/ 下的 CSV 文件追加若干行。文件不存在时先写表头。
    如果旧文件的表头和现在不一样（比如升级了脚本），把旧文件改名保存，再新建。
    使用 UTF-8 with BOM 编码，Excel 打开不会乱码。"""
    path = os.path.join(DATA_DIR, filename)
    pending = _pending_rows.setdefault(filename, [])
    pending.extend(rows)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        if os.path.exists(path) and os.path.getsize(path) > 0:
            old_header = _read_header(path)
            if old_header is not None and old_header != header:
                base, ext = os.path.splitext(filename)
                backup = f"{base}_旧版_{now_for_filename()}{ext}"
                os.replace(path, os.path.join(DATA_DIR, backup))
                print(f"{CYAN}{filename} 的表头变了，旧文件已改名为 {backup}{RESET}")
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

def _exit_with(msg):
    print(f"{RED}{msg}{RESET}")
    input("按回车键退出...")
    sys.exit(1)


def _num(value, default, minimum):
    """把配置里的数字读出来；填错就用默认值；不能小于 minimum。"""
    try:
        return max(minimum, float(value))
    except (TypeError, ValueError):
        return default


def parse_config(cfg):
    """把 config.json 的内容整理成程序内部用的格式。
    同时支持新写法 "goods": [{"id":..., "sale_time":...}] 和第一版的 "goods_ids": [...]"""
    token = str(cfg.get("token") or "").strip()
    if token.lower().startswith("bearer "):   # 防止把 "Bearer " 也粘贴进来
        token = token[7:].strip()

    bark_key = str(cfg.get("bark_key") or "").strip()
    if "api.day.app/" in bark_key:            # 防止把整个 Bark 链接粘贴进来
        bark_key = bark_key.split("api.day.app/")[1].split("/")[0]

    goods = []
    for item in cfg.get("goods") or []:
        if isinstance(item, dict) and item.get("id") is not None:
            gid = str(item["id"]).strip()
            sale_text = item.get("sale_time")
            sale_ts = parse_beijing(sale_text) if sale_text else None
            if sale_text and sale_ts is None:
                raise ValueError(f"商品 {gid} 的 sale_time 格式不对：{sale_text!r}，"
                                 f"应该像 \"2026-11-11 18:00:00\"")
            goods.append({"id": gid, "sale_time": sale_text if sale_ts else None, "sale_ts": sale_ts})
        elif item is not None and not isinstance(item, dict):
            goods.append({"id": str(item).strip(), "sale_time": None, "sale_ts": None})
    for gid in cfg.get("goods_ids") or []:     # 兼容第一版
        goods.append({"id": str(gid).strip(), "sale_time": None, "sale_ts": None})
    # 去掉重复的商品（保留第一次出现的）
    seen, unique = set(), []
    for g in goods:
        if g["id"] not in seen:
            seen.add(g["id"])
            unique.append(g)
    if not unique:
        raise ValueError("config.json 里没有要监控的商品，请在 goods 里至少填一个。")

    normal = _num(cfg.get("normal_interval", cfg.get("interval_seconds", 60)), 60, 1)

    profile = dict(DEFAULT_PROFILE)
    profile.update(cfg.get("launch_profile") or {})
    profile = {
        "prepare_before_seconds": _num(profile["prepare_before_seconds"], 30, 0),
        "prepare_interval": _num(profile["prepare_interval"], 1, MIN_INTERVAL),
        "steady_interval": _num(profile["steady_interval"], 2, MIN_INTERVAL),
        "fast_after_seconds": _num(profile["fast_after_seconds"], 240, 0),
        "fast_interval": _num(profile["fast_interval"], 0.2, MIN_INTERVAL),
        "window_interval": _num(profile["window_interval"], 1, MIN_INTERVAL),
        "after_window_interval": _num(profile["after_window_interval"], 5, MIN_INTERVAL),
        "max_launch_minutes": _num(profile["max_launch_minutes"], 90, 1),
    }

    return {
        "token": token,
        "bark_key": bark_key,
        "goods": unique,
        "normal_interval": normal,
        "payment_window_seconds": _num(cfg.get("payment_window_minutes", 10), 10, 0) * 60,
        "profile": profile,
    }


def load_config():
    """读取 config.json。出错时给出中文提示并退出。"""
    if not os.path.exists(CONFIG_PATH):
        _exit_with("找不到 config.json！请把 config.example.json 复制一份，改名为 config.json，再按 README 填写。")
    try:
        # utf-8-sig：兼容 Windows 记事本保存时带的 BOM
        with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        _exit_with(f"config.json 格式有误：第 {e.lineno} 行附近。常见原因：少了英文逗号/引号，或用了中文引号。")
    try:
        return parse_config(cfg)
    except ValueError as e:
        _exit_with(str(e))


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
    除了 "ok"，其他情况都【绝不能】当成无货/售罄/有货。
    send_ts / recv_ts 是请求发出和收到的本地时间。
    """
    headers = dict(BASE_HEADERS)
    if token:
        headers["Authorization"] = "Bearer " + token

    result = {"kind": "error", "http_status": "", "elapsed_ms": "", "date": "",
              "ratelimit": "", "cache": "", "body": None, "data": None, "message": "",
              "send_ts": time_now(), "recv_ts": None}

    start = time.perf_counter()
    try:
        resp = requests.get(API_URL.format(goods_id=goods_id), headers=headers,
                            timeout=REQUEST_TIMEOUT)
    except requests.Timeout:
        result["recv_ts"] = time_now()
        result["message"] = "请求超时"
        return result
    except requests.RequestException as e:
        result["recv_ts"] = time_now()
        result["message"] = f"网络错误（{type(e).__name__}）"
        return result
    result["recv_ts"] = time_now()
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
# 款式状态判断
# ============================================================

def sku_status(sku):
    """判断一个款是“有货”还是“售罄”。

    有货 = sellOut == 0 且 stock > 0
    售罄 = sellOut == 1 或 stock == 0
    两个字段互相矛盾时（比如 sellOut=1 但 stock=999）按售罄处理，并标记 inconsistent。
    如果某个字段缺失（平台可能隐藏），就只看另一个字段；两个都缺失，按售罄处理并标记 inconsistent。
    返回 (状态, inconsistent)。"""
    sell_out = to_int(sku.get("sellOut"))
    stock = to_int(sku.get("stock"))
    soldout_sign = (sell_out == 1) or (stock is not None and stock <= 0)
    instock_sign = (sell_out == 0) or (stock is not None and stock > 0)
    if soldout_sign:
        status = "售罄"
    elif instock_sign:
        status = "有货"
    else:
        status = "售罄"   # 两个字段都没有，无法判断
    inconsistent = (soldout_sign and instock_sign) or not (soldout_sign or instock_sign)
    return status, inconsistent


def is_unusual_stock(value):
    """stock 不是 0 也不是 999 —— 这是这次测试最关心的情况。"""
    v = to_int(value)
    return value not in (None, "") and v not in (0, 999)


# ============================================================
# 每个商品的运行状态
# ============================================================

def new_goods_state(g):
    return {
        "id": g["id"],
        "sale_ts": g["sale_ts"],
        "sale_time": g["sale_time"],
        "last": None,            # 上一次成功请求：{"local":..., "server":..., "skus": {...}}
        "sku": {},               # 每个款的跟踪信息：status / seen_in_stock / last_soldout_at
        "fast_started": False,   # 是否已经进过售罄期
        "fast_done": False,      # 售罄期是否已经结束（所有款都售罄过）
        "phase": None,
        "next_due": 0.0,         # 下一次请求的时间（本地时间戳）
        "last_print": 0.0,       # 上次在终端打印状态行的时间
        "auth_wait_until": 0.0,  # token 失效后，在这个时间之前不再请求
        "updated_changes": [],   # 商品列表接口发现的 updatedTime 变化 [{"ts":新值时间戳, "text":新值}]
        "list_checked_at": None, # 商品列表接口最近一次成功查询的估算服务器时间
        "name": "",              # 商品名（第一次成功请求后才知道）
        "summary_dirty": False,  # 有新事件，汇总文件需要重写
        "ranking_key": None,     # 上次打印的排名，排名变了才重新打印
        # 售罄期内的请求统计（售罄期结束后写进汇总文件末尾）
        "stats": {"requests": 0, "fails": 0, "r429": 0, "rtts": [], "rl_min": None,
                  "svt_last": None, "svt_changes": [], "start": None, "end": None},
    }


def new_state(config):
    return {
        "goods": {g["id"]: new_goods_state(g) for g in config["goods"]},
        "clock": ServerClock(),
        "active": None,          # 正在占用“售罄期/回流观察期”的商品 ID（同一时间最多 1 个）
        "rl_until": 0.0,         # 429 降速持续到这个时间
        "fail_count": 0,         # 连续失败次数
        "fail_pushed": False,
        "auth_pushed": False,
        "stock_alerted": {},     # 已经推送过的特殊库存值
        "list": {"next_due": 0.0, "last": {}, "last_local": ""},   # 商品列表接口的状态
    }


# ============================================================
# 阶段判断（决定请求频率）
# ============================================================

def decide_phase(gs, config, server_now):
    """根据开售时间和各款状态，决定这个商品现在处于哪个阶段。返回 (阶段, 原因)。

    不会处理“同一时间只能 1 个商品占用售罄期”的限制，那个在 pick_interval 里处理。"""
    if gs["sale_ts"] is None:
        return "normal", ""
    p = config["profile"]
    t = server_now - gs["sale_ts"]   # 距开售多少秒（负数 = 还没开售）

    if t < -p["prepare_before_seconds"]:
        return "waiting", ""
    if t < 0:
        return "prepare", f"开售前 {p['prepare_before_seconds']:.0f} 秒"
    if t >= p["max_launch_minutes"] * 60:
        return "done", f"开售后已满 {p['max_launch_minutes']:.0f} 分钟（保险）"

    skus = gs["sku"]
    any_seen_in_stock = any(s["seen_in_stock"] for s in skus.values())

    if not gs["fast_done"]:
        if not gs["fast_started"]:
            reason = ""
            # 只有开售后看到过有货，“售罄”和“满 N 秒”才算数；
            # 否则开售前本来就显示售罄（或开售推迟）时，会误进售罄期。
            if any(s["seen_in_stock"] and s["status"] == "售罄" for s in skus.values()):
                reason = "有款售罄"
            elif any(s["unusual_stock"] for s in skus.values()):
                reason = "库存出现 0 和 999 以外的数字"
            elif any_seen_in_stock and t >= p["fast_after_seconds"]:
                reason = f"开售后满 {p['fast_after_seconds']:.0f} 秒"
            if not reason:
                return "steady", "已开售"
            gs["fast_started"] = True
            gs["fast_reason"] = reason
        if skus and any_seen_in_stock and all(s["status"] == "售罄" for s in skus.values()):
            gs["fast_done"] = True
        else:
            return "fast", gs.get("fast_reason", "")

    # 售罄期已结束：只要还有款在“最近一次售罄后 N 分钟”以内，或者有款又有货了，就继续观察
    window = config["payment_window_seconds"]
    in_window = [s for s in skus.values()
                 if s["last_soldout_at"] is not None and server_now - s["last_soldout_at"] <= window]
    in_stock = [s for s in skus.values() if s["status"] == "有货"]
    if in_window or in_stock:
        return "window", ("有款在售罄后 %.0f 分钟内" % (window / 60)) if in_window else "有款回流有货"
    # 全部售罄且都超过付款窗口：继续低频观察（看平台会不会静默补货），直到 max_launch_minutes
    return "watch", "所有款都已售罄且超过付款窗口"


def phase_interval(phase, config):
    p = config["profile"]
    return {
        "prepare": p["prepare_interval"],
        "steady": p["steady_interval"],
        "fast": p["fast_interval"],
        "window": p["window_interval"],
        "watch": p["after_window_interval"],
    }.get(phase, config["normal_interval"])


def update_phase(gs, config, state):
    """重新判断阶段，处理“同一时间最多 1 个商品占用售罄期/回流观察期”，阶段变化时记录。"""
    server_now = state["clock"].now()
    phase, reason = decide_phase(gs, config, server_now)

    busy_phases = ("fast", "window")
    if phase in busy_phases:
        if state["active"] in (None, gs["id"]):
            state["active"] = gs["id"]
        else:
            reason = f"商品 {state['active']} 正在占用，先按正常频率请求"
            phase = "queued"
    elif state["active"] == gs["id"]:
        state["active"] = None   # 离开售罄期/回流观察期，释放占用

    if phase != gs["phase"]:
        old = gs["phase"]
        gs["phase"] = phase
        if phase == "fast":
            gs["stats"]["start"] = server_now
        if old == "fast":
            gs["stats"]["end"] = server_now
            write_summary(gs, config, state)
        append_csv("phases.csv", PHASE_HEADER, [[
            now_beijing(), fmt_time(server_now), gs["id"],
            PHASE_NAMES.get(old, "（启动）"), PHASE_NAMES[phase], reason]])
        if old is not None:
            print(f"{CYAN}>>> [{gs['id']}] 阶段：{PHASE_NAMES.get(old)} → {PHASE_NAMES[phase]}"
                  f"（{reason}），每 {phase_interval(phase, config):g} 秒请求一次{RESET}")
    return phase


def schedule_next(gs, config, state, send_ts):
    """算出这个商品下一次请求的时间。以“上次发出时间 + 间隔”为准，保证不超过设定的频率。"""
    phase = update_phase(gs, config, state)
    interval = phase_interval(phase, config)
    now = time_now()
    if now < state["rl_until"]:          # 429 降速期间：每秒最多 1 次
        interval = max(interval, 1.0)
    due = send_ts + interval
    # 还在等待开售时，别错过“准备”阶段的开始时刻
    if phase == "waiting":
        prepare_start = gs["sale_ts"] - config["profile"]["prepare_before_seconds"] - state["clock"].offset
        due = min(due, max(prepare_start, now))
    gs["next_due"] = max(due, gs["auth_wait_until"])


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


# ============================================================
# 售罄 / 回流事件
# ============================================================
#
# 每个款的状态只有“有货”和“售罄”两种。每次成功请求都是一次“观察”，
# 状态变化一定发生在“最后一次看到旧状态”和“第一次看到新状态”之间，
# 所以每个事件都记录一个时间区间 [区间开始, 区间结束] 和它的中点。
#
#   首次售罄：第一次从有货变成售罄
#   回流    ：售罄后又变回有货。按距离这个款最近一次售罄的时间分：
#             窗口内（可能是退单）：10 分钟内，可以用“下单没付款、订单取消”解释
#             窗口外（不是退单能解释的）：超过 10 分钟
#             注意：窗口内的回流也可能是补货，光看时间分不出来。
#             所以不给回流贴“补货”标签，只记录线索：同一分钟内回流的其他款、stock、
#             updatedTime 有没有变化、回流持续了多久，留给人来判断。
#   再次售罄：回流后又变成售罄，同时算出这次回流持续了多久
#   最终售罄：最后一次售罄后超过付款窗口（10 分钟）都没有再回流
#   取消最终售罄：已经“最终售罄”的款又出现了窗口外回流

SAME_MINUTE = 60   # 记录“前后 60 秒内也回流了的其他款”


def new_sku_track(name):
    return {
        "name": name,
        "status": None,          # 上一次看到的状态
        "inconsistent": False,
        "seen_in_stock": False,  # 开售后是否看到过有货（阶段判断用）
        "unusual_stock": False,
        "last_obs": None,        # 上一次观察 {"ts", "text"}
        "first_soldout": None,   # 首次售罄的区间
        "last_soldout": None,    # 最近一次售罄的区间（用来判断窗口内 / 窗口外）
        "last_soldout_at": None, # 最近一次“第一次看到售罄”的时刻（阶段判断用，偏保守）
        "reflow": None,          # 正在进行中的回流
        "reflows": [],           # 所有回流记录
        "final": False,          # 是否已确定为最终售罄
    }


def make_interval(a, b):
    """由两次观察组成一个时间区间"""
    return {"a": a["ts"], "b": b["ts"], "mid": (a["ts"] + b["ts"]) / 2,
            "a_text": a["text"], "b_text": b["text"]}


def write_event(gs, goods_name, sku_id, track, kind, iv, config, since=None, cls="", others="",
                updated="", stock="", dur=None, note=""):
    """往 events.csv 写一行，并在终端打印。"""
    from_sale = round(iv["mid"] - gs["sale_ts"], 1) if gs["sale_ts"] else ""
    row = [
        now_beijing(), gs["id"], goods_name, sku_id, track["name"], kind,
        fmt_time(iv["a"]), fmt_time(iv["b"]), fmt_time(iv["mid"]), iv["a_text"], iv["b_text"],
        round(iv["b"] - iv["a"], 2), from_sale,
        "" if since is None else round(since, 1), cls, others, updated, stock,
        "" if dur is None else round(dur["est"], 1),
        "" if dur is None else round(dur["min"], 1),
        "" if dur is None else round(dur["max"], 1),
        "true" if track["inconsistent"] else "", note,
    ]
    append_csv("events.csv", EVENT_HEADER, [row])
    gs["summary_dirty"] = True

    name = short_sku_name(track["name"])
    when = fmt_time(iv["mid"])[11:23]
    detail = []
    if from_sale != "":
        detail.append(f"开售后 {from_sale:g} 秒")
    if since is not None:
        detail.append(f"距上次售罄 {since:.0f} 秒")
    if cls:
        detail.append(cls)
    if others:
        detail.append(f"同一分钟内也回流：{others}")
    if updated:
        detail.append(f"updatedTime：{updated}")
    if dur is not None:
        detail.append(f"回流持续约 {dur['est']:.1f} 秒（{max(dur['min'], 0):.1f}～{dur['max']:.1f}）")
    if stock not in ("", None):
        detail.append(f"stock={stock}")
    if note:
        detail.append(note)
    loud = kind in ("首次售罄", "最终售罄", "取消最终售罄") or (kind == "回流" and cls.startswith("窗口外"))
    color = YELLOW if loud else CYAN
    mark = "★" if loud else "·"
    print(f"{color}{mark} [{gs['id']}] {name} {kind} @{when}（±{(iv['b'] - iv['a']) / 2:.1f}s） "
          + "，".join(detail) + RESET)


def track_skus(gs, current, obs, goods_name, config):
    """用这次观察更新每个款的状态，产生售罄 / 回流事件。"""
    window = config["payment_window_seconds"]
    after_sale = gs["sale_ts"] is None or obs["ts"] >= gs["sale_ts"]

    for sku_id, info in current.items():
        track = gs["sku"].setdefault(sku_id, new_sku_track(info["skuName"]))
        track["name"] = info["skuName"]
        track["inconsistent"] = info["inconsistent"]
        track["unusual_stock"] = is_unusual_stock(info["stock"])
        prev_status, now_status = track["status"], info["status"]

        if prev_status == "有货" and now_status == "售罄":
            iv = make_interval(track["last_obs"], obs)
            if track["first_soldout"] is None:
                track["first_soldout"] = iv
                write_event(gs, goods_name, sku_id, track, "首次售罄", iv, config)
            else:
                # 再次售罄：算出刚结束的这次回流持续了多久
                rf = track["reflow"]
                dur = None
                if rf is not None:
                    dur = {"est": iv["mid"] - rf["iv"]["mid"],
                           "min": iv["a"] - rf["iv"]["b"],    # 最晚开始、最早结束
                           "max": iv["b"] - rf["iv"]["a"]}    # 最早开始、最晚结束
                    rf["duration"] = dur
                    rf["end"] = iv
                write_event(gs, goods_name, sku_id, track, "再次售罄", iv, config, dur=dur)
            track["last_soldout"] = iv
            track["last_soldout_at"] = obs["ts"]
            track["reflow"] = None

        elif prev_status == "售罄" and now_status == "有货" and track["first_soldout"] is not None:
            iv = make_interval(track["last_obs"], obs)
            since = iv["mid"] - track["last_soldout"]["mid"]
            cls = "窗口内（可能是退单）" if since <= window else "窗口外（不是退单能解释的）"
            if track["final"]:
                track["final"] = False
                write_event(gs, goods_name, sku_id, track, "取消最终售罄", iv, config,
                            note="最终售罄后又回流了，重新等待")
            # 线索 1：前后 60 秒内也回流了的其他款（只能看到已经发生的）
            others = sorted({short_sku_name(t["name"]) for t in gs["sku"].values() if t is not track
                             for o in t["reflows"] if abs(o["iv"]["mid"] - iv["mid"]) <= SAME_MINUTE})
            # 线索 2：商品列表接口的 updatedTime 有没有变化（第 3 部分实现）
            soldout_mid = track["last_soldout"]["mid"]
            updated = updated_time_clue(gs, iv, soldout_mid)
            rf = {"iv": iv, "since": since, "cls": cls, "stock": info["stock"], "others": others,
                  "updated": updated, "sku_id": sku_id, "duration": None, "end": None,
                  "soldout_mid": soldout_mid}
            track["reflow"] = rf
            track["reflows"].append(rf)
            write_event(gs, goods_name, sku_id, track, "回流", iv, config, since=since, cls=cls,
                        others="、".join(others), updated=updated, stock=info["stock"])

        # 最终售罄：最近一次售罄后超过付款窗口都没有回流
        if (now_status == "售罄" and track["last_soldout"] is not None and not track["final"]
                and obs["ts"] - track["last_soldout"]["mid"] > window):
            track["final"] = True
            write_event(gs, goods_name, sku_id, track, "最终售罄", track["last_soldout"], config,
                        note=f"之后 {window / 60:g} 分钟内没有回流")

        if now_status == "有货" and after_sale:
            track["seen_in_stock"] = True
        track["status"] = now_status
        track["last_obs"] = obs

    for sku_id in list(gs["sku"]):
        if sku_id not in current:
            del gs["sku"][sku_id]   # 款式消失了，不再参与判断


UPDATED_SLACK = 5   # updatedTime 只精确到秒，再留几秒余量


def updated_time_clue(gs, iv, soldout_mid):
    """这次回流前后，商品列表接口的 updatedTime 有没有变化。

    看 updatedTime 的新值是否落在“这个款上次售罄”到“看到回流”之间（前后各留几秒）。
    列表接口最快 30 秒才查一次，可能还没查到；那就写“待确认”，之后查到会写进 updated_time.csv。"""
    hits = [c for c in gs["updated_changes"]
            if soldout_mid - UPDATED_SLACK <= c["ts"] <= iv["b"] + UPDATED_SLACK]
    if hits:
        c = hits[-1]
        return f"有变化（{c['text'][11:]}，回流前 {iv['mid'] - c['ts']:.0f} 秒）"
    if gs["list_checked_at"] is not None and gs["list_checked_at"] >= iv["b"]:
        return "未发现"
    return "待确认（见 updated_time.csv）"


def parse_updated_time(value):
    """updatedTime 实际是 '2026-11-06 04:24:21' 这样的文字；万一是毫秒时间戳也能处理。"""
    if isinstance(value, (int, float)):
        ts = value / 1000
        return ts, fmt_time(ts, ms=False)
    ts = parse_beijing(value)
    return ts, (str(value) if value is not None else "")


# ============================================================
# 商品列表接口（看 updatedTime 有没有变化）
# ============================================================

def fetch_list(goods_ids, token):
    """请求一次商品列表接口。成功返回 {goodsId: 记录}，失败返回 (kind, 说明)。"""
    headers = dict(BASE_HEADERS)
    headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = "Bearer " + token
    payload = {"goodsIds": [int(g) if str(g).isdigit() else g for g in goods_ids],
               "pageNum": 1, "pageSize": max(10, len(goods_ids)), "sortType": 1, "sortFieldType": 0}
    try:
        resp = requests.post(LIST_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
    except requests.Timeout:
        return "error", "请求超时"
    except requests.RequestException as e:
        return "error", f"网络错误（{type(e).__name__}）"
    if resp.status_code in (401, 403):
        return "auth", f"HTTP {resp.status_code}"
    if resp.status_code == 429:
        return "ratelimit", "HTTP 429"
    if resp.status_code != 200:
        return "error", f"HTTP {resp.status_code}"
    try:
        body = resp.json()
    except ValueError:
        return "error", "返回内容不是 JSON"
    if not isinstance(body, dict) or body.get("code") != 200:
        return "error", f"接口 code={body.get('code') if isinstance(body, dict) else '?'}"
    data = body.get("data")
    records = data.get("records") if isinstance(data, dict) else None
    if not isinstance(records, list):
        return "error", "返回数据里没有 records"
    return "ok", {str(r.get("goodsId")): r for r in records if isinstance(r, dict)}


def check_list(config, state):
    """查一次商品列表，发现 updatedTime 变化就记录、推送。失败只记错误，不改任何状态。"""
    lst = state["list"]
    busy = any(g["phase"] in ("fast", "window", "watch") for g in state["goods"].values())
    interval = LIST_INTERVAL_BUSY if busy else LIST_INTERVAL_NORMAL
    lst["next_due"] = time_now() + interval

    kind, result = fetch_list(list(state["goods"]), config["token"])
    if kind != "ok":
        log_error(f"[商品列表] 请求失败：{result}")
        if kind == "ratelimit":
            state["rl_until"] = time_now() + RATE_LIMIT_SLOWDOWN
        return

    now_local = now_beijing()
    server_now = state["clock"].now()
    for gid, gs in state["goods"].items():
        rec = result.get(gid)
        if rec is None:
            continue
        new_ts, new_text = parse_updated_time(rec.get("updatedTime"))
        old = lst["last"].get(gid)
        lst["last"][gid] = {"ts": new_ts, "text": new_text}
        gs["list_checked_at"] = server_now
        if old is None or old["text"] == new_text or new_ts is None:
            continue   # 第一次查询只作为基准；没变化就跳过

        # updatedTime 变了：找找附近有没有回流（回流可能比这次查询更早被记录）
        gs["updated_changes"].append({"ts": new_ts, "text": new_text})
        near = []
        for t in gs["sku"].values():
            for rf in t["reflows"]:
                if rf["soldout_mid"] - UPDATED_SLACK <= new_ts <= rf["iv"]["b"] + UPDATED_SLACK:
                    near.append(f"{short_sku_name(t['name'])}（回流 {fmt_time(rf['iv']['mid'])[11:19]}）")
        name = rec.get("name") or ""
        append_csv("updated_time.csv", UPDATED_HEADER, [[
            now_local, gid, name, old["text"], new_text, rec.get("stock"),
            lst.get("last_local", ""), "、".join(near)]])
        print(f"{YELLOW}★ [{gid}] 商品 updatedTime 变化：{old['text']} → {new_text}"
              f"（列表 stock={rec.get('stock')}）{('，附近的回流：' + '、'.join(near)) if near else ''}{RESET}")
        bark_push(config, f"商品更新 {short_goods_name(name)}",
                  f"updatedTime {old['text'][11:]} → {new_text[11:]}\n列表 stock={rec.get('stock')}"
                  + (f"\n附近的回流：{'、'.join(near)}" if near else ""))
    lst["last_local"] = now_local


# ============================================================
# 售罄汇总：data/soldout_<goodsId>.csv
# ============================================================

def count_fast_request(gs, result):
    """售罄期内每次请求都记一笔，用来评估请求质量和服务器数据的更新精度。"""
    st = gs["stats"]
    st["requests"] += 1
    if result["kind"] != "ok":
        st["fails"] += 1
    if result["kind"] == "ratelimit":
        st["r429"] += 1
    if result["recv_ts"] is not None and result["http_status"] != "":
        st["rtts"].append((result["recv_ts"] - result["send_ts"]) * 1000)
    rl = to_int(result["ratelimit"])
    if rl is not None:
        st["rl_min"] = rl if st["rl_min"] is None else min(st["rl_min"], rl)
    if result["kind"] == "ok":
        svt = result["data"].get("serverSystemTime")
        if svt and svt != st["svt_last"]:
            if st["svt_last"] is not None:
                st["svt_changes"].append(result["recv_ts"])   # 看到 serverSystemTime 跳变的时刻
            st["svt_last"] = svt


def reflow_has_update(gs, rf):
    """这次回流前后 updatedTime 有没有变化（用目前已知的全部变化重新判断）"""
    return any(rf["soldout_mid"] - UPDATED_SLACK <= c["ts"] <= rf["iv"]["b"] + UPDATED_SLACK
               for c in gs["updated_changes"])


def reflow_has_partner(gs, sku_id, rf):
    """前后 60 秒内有没有其他款也回流（两个方向都算）"""
    return any(abs(o["iv"]["mid"] - rf["iv"]["mid"]) <= SAME_MINUTE
               for sid, t in gs["sku"].items() if sid != sku_id for o in t["reflows"])


def build_summary(gs, config, state):
    """算出汇总表的每一行。返回 (表头, 行列表, 排名文字列表)。"""
    window = config["payment_window_seconds"]
    server_now = state["clock"].now()
    items = list(gs["sku"].items())
    sold = sorted([(sid, t) for sid, t in items if t["first_soldout"]],
                  key=lambda x: x[1]["first_soldout"]["mid"])
    unsold = [(sid, t) for sid, t in items if not t["first_soldout"]]

    def overlaps(a, b):
        return a["a"] < b["b"] and b["a"] < a["b"]

    header = ["首次售罄排名", "款式", "skuId", "首次售罄区间开始", "首次售罄区间结束", "区间中点",
              "距开售秒数", "排名说明", "窗口内回流次数", "窗口外回流次数",
              "updatedTime有变化的回流次数", "和其他款同一分钟回流的次数",
              "回流平均持续秒数", "回流最长持续秒数", "首次售罄后累计有货秒数", "当前状态", "最终售罄时间"]
    rows, ranking = [], []
    for rank, (sid, t) in enumerate(sold + unsold, start=1):
        fs = t["first_soldout"]
        reflows = t["reflows"]
        done = [rf for rf in reflows if rf["duration"] is not None]
        ongoing = [rf for rf in reflows if rf["duration"] is None]
        in_stock_secs = sum(rf["duration"]["est"] for rf in done) + \
            sum(max(0.0, server_now - rf["iv"]["mid"]) for rf in ongoing)
        if fs:
            tied = [short_sku_name(o["name"]) for osid, o in sold
                    if osid != sid and overlaps(fs, o["first_soldout"])]
            note = ("和 " + "、".join(tied) + " 无法分出先后") if tied else ""
        elif t["status"] == "售罄":
            note = "脚本启动时已经售罄（售罄时间不知道）"
        else:
            note = "尚未售罄"
        status = t["status"] or ""
        if ongoing:
            status += "（回流中）"
        final = fmt_time(t["last_soldout"]["mid"]) if t["final"] else ""
        rows.append([
            rank if fs else "", t["name"], sid,
            fmt_time(fs["a"]) if fs else "", fmt_time(fs["b"]) if fs else "", fmt_time(fs["mid"]) if fs else "",
            round(fs["mid"] - gs["sale_ts"], 1) if fs and gs["sale_ts"] else "",
            note,
            sum(1 for rf in reflows if rf["since"] <= window),
            sum(1 for rf in reflows if rf["since"] > window),
            sum(1 for rf in reflows if reflow_has_update(gs, rf)),
            sum(1 for rf in reflows if reflow_has_partner(gs, sid, rf)),
            round(sum(rf["duration"]["est"] for rf in done) / len(done), 1) if done else "",
            round(max(rf["duration"]["est"] for rf in done), 1) if done else "",
            round(in_stock_secs, 1) if fs else "",
            status, final,
        ])
        if fs:
            from_sale = f"开售后 {fs['mid'] - gs['sale_ts']:.1f}s" if gs["sale_ts"] else fmt_time(fs["mid"])[11:23]
            ranking.append(f"{rank}. {short_sku_name(t['name'])}  {from_sale}"
                           f"（±{(fs['b'] - fs['a']) / 2:.1f}s）{'  ← ' + note if note else ''}")
    return header, rows, ranking


def stats_rows(gs):
    """售罄期内的请求统计（售罄期结束后才有）。"""
    st = gs["stats"]
    if st["end"] is None or st["start"] is None:
        return []
    rtts = st["rtts"]
    jumps = [b - a for a, b in zip(st["svt_changes"], st["svt_changes"][1:])]
    span = st["end"] - st["start"]
    return [
        [],
        ["统计（售罄期内）", "数值", "说明"],
        ["售罄期开始", fmt_time(st["start"]), "估算服务器时间"],
        ["售罄期结束", fmt_time(st["end"]), f"共 {span:.0f} 秒"],
        ["请求次数", st["requests"], f"平均每秒 {st['requests'] / span:.2f} 次" if span > 0 else ""],
        ["失败次数", st["fails"], "超时、网络错误、5xx、429 等都算"],
        ["429 次数", st["r429"], ""],
        ["平均往返耗时ms", round(sum(rtts) / len(rtts)) if rtts else "", "发出到收到"],
        ["最大往返耗时ms", round(max(rtts)) if rtts else "", ""],
        ["serverSystemTime 平均多久跳一次(秒)", round(sum(jumps) / len(jumps), 2) if jumps else "",
         "约等于 1 说明服务器时间是实时的；明显大于 1 说明返回的数据可能有缓存"],
        ["X-RateLimit-Remaining 最小值", st["rl_min"] if st["rl_min"] is not None else "", "越小说明离限流越近"],
    ]


def write_summary(gs, config, state):
    """重写 data/soldout_<goodsId>.csv（整个文件覆盖）。文件被 Excel 占用时，下次有请求再试。"""
    if not gs["sku"]:
        return
    header, rows, ranking = build_summary(gs, config, state)
    sale = gs["sale_time"] or "（没填开售时间）"
    lines = [["商品", gs["name"], "goodsId", gs["id"], "开售时间", sale, "更新时间", now_beijing()], [],
             header] + rows + stats_rows(gs)
    path = os.path.join(DATA_DIR, f"soldout_{gs['id']}.csv")
    tmp = path + ".tmp"
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(tmp, "w", newline="", encoding="utf-8-sig") as f:
            csv.writer(f).writerows(lines)
        os.replace(tmp, path)
        gs["summary_dirty"] = False
    except PermissionError:
        gs["summary_dirty"] = True   # 被 Excel 打开了，下次再写
        log_error(f"soldout_{gs['id']}.csv 被其他程序（可能是 Excel）占用，关闭后会自动更新")
    except Exception as e:
        gs["summary_dirty"] = True
        log_error(f"写入 soldout_{gs['id']}.csv 失败：{e}")

    # 排名有变化才在终端打印，避免刷屏
    key = tuple(r.split("  ")[0] + r.split("  ")[1] for r in ranking) if ranking else None
    if ranking and key != gs["ranking_key"]:
        gs["ranking_key"] = key
        print(f"{CYAN}── [{gs['id']}] 首次售罄排名 ──")
        for r in ranking:
            print("   " + r)
        print(f"──────────────{RESET}")


def handle_success(gs, result, config, state):
    goods_id = gs["id"]
    data = result["data"]
    send_local = fmt_time(result["send_ts"])
    recv_local = fmt_time(result["recv_ts"])
    server_time = data.get("serverSystemTime") or ""
    goods_name = data.get("name") or ""
    clock = state["clock"]
    clock.update(server_time, result["date"], result["send_ts"], result["recv_ts"])
    server_now = clock.now()
    phase_label = PHASE_NAMES.get(gs["phase"] or "normal")

    # ---- 1. 整理当前每个款的状态，并写入 snapshots.csv ----
    current = {}   # skuId -> {字段: 值}
    rows = []
    for sku in data["goodsSkuList"]:
        if not isinstance(sku, dict):
            continue
        sku_id = str(sku.get("id"))
        info = {f: sku.get(f) for f in WATCH_FIELDS}   # 缺失的字段是 None，CSV 里写成空
        info["skuName"] = sku.get("skuName") or ""
        info["status"], info["inconsistent"] = sku_status(sku)
        current[sku_id] = info
        rows.append([
            send_local, recv_local, server_time, result["date"], phase_label,
            data.get("goodsId", goods_id), goods_name, data.get("putStatus"),
            data.get("startSelling"), data.get("sellStartTime"),
            sku_id, info["skuName"], info["status"], "true" if info["inconsistent"] else "",
            info["stock"], info["sellOut"], info["hotSell"], info["sellNum"],
            info["putStatus"], info["appletPutStatus"],
            result["http_status"], result["elapsed_ms"], result["ratelimit"], result["cache"],
        ])
    append_csv("snapshots.csv", SNAPSHOT_HEADER, rows)

    # ---- 2. 跟踪每个款的售罄 / 回流（写 events.csv） ----
    # 这次观察的时刻 = 请求发出和收到的中点，换算成服务器时间
    obs = {"ts": (result["send_ts"] + result["recv_ts"]) / 2 + clock.offset, "text": server_time}
    gs["name"] = goods_name
    if gs["last"] is None:
        gs["summary_dirty"] = True   # 启动后第一次成功请求：先生成汇总文件
    track_skus(gs, current, obs, goods_name, config)
    if gs["summary_dirty"]:
        write_summary(gs, config, state)

    # ---- 3. 和上一次成功的结果比较（第一版的字段变化记录，保留） ----
    prev = gs["last"]
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

    if changes:
        event_rows = [[recv_local, server_time, prev["local"], prev["server"],
                       goods_id, goods_name, sid, sname, f, old_v, new_v]
                      for (sid, sname, f, old_v, new_v) in changes]
        append_csv("changes.csv", CHANGE_HEADER, event_rows)
        save_raw_json(goods_id, result["body"], "change")

        # 字段变化只打一行（售罄 / 回流事件已经在上面醒目地打印过了）
        print(f"  字段变化 [{goods_id}] " + "；".join(
            f"{short_sku_name(sname)} {describe_change(f, old_v, new_v)}"
            for (sid, sname, f, old_v, new_v) in changes))

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
            if not changes:
                save_raw_json(goods_id, result["body"], "stock")
        elif not is_unusual_stock(stock):
            state["stock_alerted"].pop(key, None)

    # ---- 5. 记住这次的状态，下次用来比较 ----
    gs["last"] = {"local": recv_local, "server": server_time, "skus": current}

    # ---- 6. 打印一行简短状态（每秒最多一行，有变化时一定打印） ----
    if changes or prev is None or time_now() - gs["last_print"] >= 1:
        gs["last_print"] = time_now()
        parts = []
        for info in current.values():
            name = short_sku_name(info["skuName"])
            color = GREEN if info["status"] == "有货" else RED
            s = f"{color}{name}:{info['status']}{RESET}"
            if info["inconsistent"]:
                s += f"{YELLOW}?{RESET}"
            if is_unusual_stock(info["stock"]):
                s += f"{RED}[库存{info['stock']}]{RESET}"
            parts.append(s)
        print(f"{server_time} [{goods_id}·{phase_label}] " + " | ".join(parts)
              + f"  ({result['elapsed_ms']}ms, 剩余额度{result['ratelimit'] or '?'})")


# ============================================================
# 检查一个商品（包含错误处理）
# ============================================================

def check_goods(gs, config, state):
    """请求并处理一个商品，然后安排它的下一次请求时间。"""
    if gs["phase"] is None:          # 启动后第一次：先定好阶段，快照里才有阶段名
        update_phase(gs, config, state)
    result = fetch_goods(gs["id"], config["token"])
    if gs["phase"] == "fast":
        count_fast_request(gs, result)

    if result["kind"] == "ok":
        if state["fail_pushed"] or state["auth_pushed"]:
            print(f"{GREEN}请求已恢复正常。{RESET}")
            bark_push(config, "监控已恢复", f"请求恢复正常\n{now_beijing()[:19]}")
        state["fail_count"] = 0
        state["fail_pushed"] = False
        state["auth_pushed"] = False
        gs["auth_wait_until"] = 0.0
        handle_success(gs, result, config, state)
    else:
        # ---- 失败：只记录错误，不写快照、不改变任何款的状态 ----
        state["clock"].update(None, result["date"], result["send_ts"], result["recv_ts"])
        state["fail_count"] += 1
        log_error(f"[{gs['id']}] 请求失败（连续第 {state['fail_count']} 次）：{result['message']}")

        if result["kind"] == "auth":
            gs["auth_wait_until"] = time_now() + WAIT_AFTER_AUTH_FAIL
            if not state["auth_pushed"]:
                state["auth_pushed"] = True
                bark_push(config, "token 可能失效",
                          f"{result['message']}\n请更新 config.json 里的 token 后重启脚本\n"
                          f"{now_beijing()[:19]}", urgent=True)
            print(f"{YELLOW}[{gs['id']}] 将在 {WAIT_AFTER_AUTH_FAIL // 60} 分钟后重试。{RESET}")
        elif result["kind"] == "ratelimit":
            state["rl_until"] = time_now() + RATE_LIMIT_SLOWDOWN
            print(f"{YELLOW}被限流，接下来 {RATE_LIMIT_SLOWDOWN} 秒内每秒最多请求 1 次。{RESET}")

        if state["fail_count"] >= FAILS_BEFORE_PUSH and not state["fail_pushed"]:
            state["fail_pushed"] = True
            bark_push(config, "监控连续失败",
                      f"已连续失败 {state['fail_count']} 次：{result['message']}\n{now_beijing()[:19]}")

    schedule_next(gs, config, state, result["send_ts"])


# ============================================================
# 主循环
# ============================================================

def run_due(config, state):
    """找出最早该请求的商品（或商品列表接口），如果时间到了就请求它。返回距离下一次请求还要等几秒。"""
    if state["list"]["next_due"] <= time_now():
        try:
            check_list(config, state)
        except Exception:
            log_error("程序内部错误（商品列表）：\n" + traceback.format_exc())
        return 0
    gs = min(state["goods"].values(), key=lambda g: g["next_due"])
    wait = min(gs["next_due"], state["list"]["next_due"]) - time_now()
    if wait > 0:
        return wait
    if gs["next_due"] > time_now():
        return 0
    try:
        check_goods(gs, config, state)
    except Exception:
        # 任何意外错误都只记录，不让脚本退出；这个商品 1 秒后再试
        log_error("程序内部错误：\n" + traceback.format_exc())
        gs["next_due"] = time_now() + 1
    return 0


def main():
    enable_windows_colors()
    config = load_config()
    state = new_state(config)
    p = config["profile"]

    print("=" * 60)
    print(" 时代峰峻商城 SKU 监控 已启动")
    for g in config["goods"]:
        if g["sale_time"]:
            print(f"  · 商品 {g['id']}：开售时间 {g['sale_time']}（北京时间），开售前后自动加快请求")
        else:
            print(f"  · 商品 {g['id']}：每 {config['normal_interval']:g} 秒请求一次")
    if any(g["sale_time"] for g in config["goods"]):
        print(f"  开售阶段：准备 每{p['prepare_interval']:g}秒 → 平稳 每{p['steady_interval']:g}秒 → "
              f"售罄期 每{p['fast_interval']:g}秒 → 回流观察期 每{p['window_interval']:g}秒 → "
              f"售后观察 每{p['after_window_interval']:g}秒")
    print(f" token：{'已填写' if config['token'] else '未填写（不带 token 请求）'}"
          f"   Bark 推送：{'已开启' if config['bark_key'] else '未开启'}")
    print(f" 数据保存在：{DATA_DIR}")
    print(" 按 Ctrl + C 停止")
    print("=" * 60)

    while True:
        wait = run_due(config, state)
        if wait > 0:
            time.sleep(min(wait, 1.0))   # 最多睡 1 秒就重新检查，阶段切换更及时


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n已停止监控。数据都保存在 data 文件夹里。")
