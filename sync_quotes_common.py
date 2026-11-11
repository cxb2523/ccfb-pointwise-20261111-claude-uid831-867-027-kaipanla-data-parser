#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_quotes_common.py
=====================
sync_quotes_raw / sync_quotes_batch 共用的请求与落盘引擎。

职责划分（重试只管传输，幂等只认落盘）：

1. 请求层 ``request_with_retry``
   - 仅对 ConnectTimeout / 5xx / 空 body 重试；
   - 1s、2s、4s 指数退避并加抖动，最多 4 次尝试（即最多 3 次重试）；
   - 4xx、AES 解密失败直接抛出，不重试；
   - 超时发生时服务端可能已经处理成功，因此请求层不做任何"去重"，
     是否写入完全交给落盘侧的 symbol + trade_date 业务主键判断。

2. 链路追踪
   - ``normalize_url`` 剥掉 ts、sign 两个查询参数，归一化结果仅用于日志/追踪，
     不参与任何写入判定。

3. 落盘层 ``QuoteStore``
   - quotes.csv 固定表头、utf-8-sig，追加不重复写表头；
   - 状态写 .sync_state.json，所有更新走临时文件 + os.replace 原子替换；
   - 每个主键先置 processing，请求、校验、追加 CSV 并 flush 后再原子置 done；
   - 重跑遇到 processing 或缺 done 的主键，先回查 quotes.csv 同一业务主键：
     命中 -> 补记 done 并打日志（覆盖崩在 CSV 与 state 之间的窗口），
     未命中 -> 重新请求并追加；
   - 坏行（symbol 非 6 位 / trade_date 非 YYYYMMDD / close、turnover 不可转 float）
     写入 quarantine/quotes_bad.csv 并 warning，绝不进入 quotes.csv。
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import os
import random
import re
import tempfile
import time
from datetime import datetime
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import requests

# cryptography 是项目环境中可用的 AES 实现（无 pycryptodome）
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding as sym_padding

logger = logging.getLogger("sync_quotes")

# ── 请求层常量 ───────────────────────────────────────────────
MAX_ATTEMPTS = 4              # 最多 4 次尝试（首次 + 3 次重试）
RETRY_BASE_DELAYS = (1, 2, 4)  # 第 1/2/3 次重试的基准退避秒数
RETRY_JITTER_RATIO = 0.25     # 抖动幅度：基准延迟的 ±25%
DEFAULT_TIMEOUT = 15

# ── CSV 固定表头 ─────────────────────────────────────────────
CSV_HEADER = ["symbol", "trade_date", "close", "turnover"]
STATE_FILENAME = ".sync_state.json"
QUOTES_FILENAME = "quotes.csv"
QUARANTINE_DIRNAME = "quarantine"
BAD_ROWS_FILENAME = "quotes_bad.csv"

_SYMBOL_RE = re.compile(r"^\d{6}$")
_TRADE_DATE_RE = re.compile(r"^\d{8}$")


# ── 自定义异常 ───────────────────────────────────────────────
class AESDecryptError(Exception):
    """AES 解密失败：属确定性错误，请求层不重试，直接向上抛。"""


class QuarantineError(ValueError):
    """行数据校验失败，携带字段级原因，供落盘侧隔离坏行。"""

    def __init__(self, reason: str, row: dict):
        super().__init__(reason)
        self.reason = reason
        self.row = row


# ── AES 解密（确定性失败 -> AESDecryptError，不重试） ─────────
def decrypt_aes(ciphertext: bytes, key: bytes, iv: bytes) -> bytes:
    """AES-CBC + PKCS7 解密。key 支持 16/24/32 字节，iv 固定 16 字节。

    任何解密 / 去填充失败都包成 AESDecryptError，请求层据此直接抛出。
    """
    try:
        if len(iv) != 16:
            raise ValueError(f"iv 长度必须为 16，实际 {len(iv)}")
        if len(key) not in (16, 24, 32):
            raise ValueError(f"key 长度必须为 16/24/32，实际 {len(key)}")
        cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
        decryptor = cipher.decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = sym_padding.PKCS7(algorithms.AES.block_size).unpadder()
        return unpadder.update(padded) + unpadder.finalize()
    except Exception as exc:  # noqa: BLE001 - 统一收敛为领域异常
        raise AESDecryptError(f"AES 解密失败: {exc}") from exc


# ── URL 归一化（仅用于链路追踪） ─────────────────────────────
def normalize_url(url: str) -> str:
    """剥掉 ts、sign 查询参数后重组 URL。

    归一化结果只用于日志/链路追踪；是否写入只认 symbol + trade_date，
    与 URL 上的时间戳、签名无关。
    """
    parts = urlsplit(url)
    kept = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if k not in ("ts", "sign")]
    return urlunsplit((parts.scheme, parts.netloc, parts.path,
                       urlencode(kept), parts.fragment))


# ── 退避计时（可被测试 monkeypatch） ─────────────────────────
def _sleep_with_jitter(base_delay: float,
                       sleep_func=None,
                       rand_func=None) -> float:
    # 运行时再解析 random.uniform / time.sleep，保证 monkeypatch 生效
    rnd = rand_func or random.uniform
    sleep = sleep_func or time.sleep
    delay = rnd(base_delay * (1 - RETRY_JITTER_RATIO),
                base_delay * (1 + RETRY_JITTER_RATIO))
    sleep(delay)
    return delay


# ── 请求层：分类重试 ─────────────────────────────────────────
def request_with_retry(method: str,
                       url: str,
                       *,
                       session: requests.Session | None = None,
                       timeout: float = DEFAULT_TIMEOUT,
                       sleep_func=time.sleep,
                       **kwargs) -> requests.Response:
    """带分类重试的 HTTP 请求，返回最终 Response。

    重试条件（满足任一即重试，最多 ``MAX_ATTEMPTS`` 次）：
      - ``requests.ConnectTimeout``（连接/读取超时，服务端可能已处理成功）；
      - 5xx 响应；
      - 空 body 响应。

    直接抛出、绝不重试：
      - 4xx 响应（``HTTPError``）；
      - ``AESDecryptError``（由上层解密失败时透传，本函数不吞）；
      - 其它非超时传输异常。

    注意：本函数只保证"传输成功拿到响应"，不保证业务幂等。
    超时导致的潜在重复提交由 ``QuoteStore`` 的业务主键回查兜底。
    """
    http = session or requests
    trace_url = normalize_url(url)
    last_exc: Exception | None = None

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            resp = http.request(method, url, timeout=timeout, **kwargs)
        except requests.ConnectTimeout as exc:
            # 超时点服务端可能已经落库，重试只解决传输，幂等交给落盘侧
            last_exc = exc
            logger.warning("ConnectTimeout 第%s/%s次 url=%s err=%s",
                           attempt, MAX_ATTEMPTS, trace_url, exc)
            if attempt < MAX_ATTEMPTS:
                _sleep_with_jitter(RETRY_BASE_DELAYS[attempt - 1], sleep_func)
                continue
            raise

        if 500 <= resp.status_code < 600:
            last_exc = requests.HTTPError(
                f"5xx {resp.status_code}", response=resp)
            logger.warning("5xx 第%s/%s次 status=%s url=%s",
                           attempt, MAX_ATTEMPTS, resp.status_code, trace_url)
            if attempt < MAX_ATTEMPTS:
                _sleep_with_jitter(RETRY_BASE_DELAYS[attempt - 1], sleep_func)
                continue
            resp.raise_for_status()

        # 4xx：确定性拒绝，重试无意义，直接抛
        if 400 <= resp.status_code < 500:
            logger.error("4xx 不重试 status=%s url=%s",
                         resp.status_code, trace_url)
            resp.raise_for_status()

        # 空 body：可能是网关截断，按可恢复处理
        if not resp.content:
            last_exc = ValueError("空 body 响应")
            logger.warning("空 body 第%s/%s次 url=%s",
                           attempt, MAX_ATTEMPTS, trace_url)
            if attempt < MAX_ATTEMPTS:
                _sleep_with_jitter(RETRY_BASE_DELAYS[attempt - 1], sleep_func)
                continue
            raise ValueError(f"重试 {MAX_ATTEMPTS} 次后仍为空 body: {trace_url}")

        return resp

    # 理论上不会走到这里，防御性兜底
    raise last_exc if last_exc else RuntimeError("请求重试未知失败")


def decode_response_body(resp: requests.Response,
                         aes_key: bytes | None = None,
                         aes_iv: bytes | None = None) -> str:
    """提取响应文本；响应为二进制密文（提供了 aes_key）时先 AES-CBC 解密。

    解密失败抛 ``AESDecryptError``，调用方必须直接上抛、不得重试。
    """
    if aes_key is None:
        return resp.text
    if aes_iv is None:
        raise AESDecryptError("AES 解密需要 iv")
    plain = decrypt_aes(resp.content, aes_key, aes_iv)
    return plain.decode("utf-8")


# ── 行数据校验 ───────────────────────────────────────────────
def validate_row(row: dict) -> dict:
    """校验单行行情，返回规范化后的行；坏行抛 ``QuarantineError``。

    规则：
      - symbol 必须为 6 位数字代码；
      - trade_date 必须符合 YYYYMMDD（且是真实日历日期）；
      - close、turnover 必须可转 float。
    """
    if not isinstance(row, dict):
        raise QuarantineError(f"行不是 dict: {type(row).__name__}", row)

    symbol = str(row.get("symbol", "")).strip()
    if not _SYMBOL_RE.match(symbol):
        raise QuarantineError(f"非法 symbol（需 6 位数字代码）: {row.get('symbol')!r}", row)

    trade_date = str(row.get("trade_date", "")).strip()
    if not _TRADE_DATE_RE.match(trade_date):
        raise QuarantineError(f"非法 trade_date（需 YYYYMMDD）: {row.get('trade_date')!r}", row)
    try:
        datetime.strptime(trade_date, "%Y%m%d")
    except ValueError:
        raise QuarantineError(f"非法 trade_date（不是真实日期）: {trade_date!r}", row)

    normalized: dict = {"symbol": symbol, "trade_date": trade_date}
    for field in ("close", "turnover"):
        raw = row.get(field)
        try:
            normalized[field] = float(raw)
        except (TypeError, ValueError):
            raise QuarantineError(f"非法 {field}（不可转 float）: {raw!r}", row)
    return normalized


def business_key(row: dict) -> tuple[str, str]:
    """业务主键：是否写入只认 symbol + trade_date。"""
    return str(row["symbol"]).strip(), str(row["trade_date"]).strip()


# ── 落盘引擎：CSV + 状态机 + 隔离区 ──────────────────────────
class QuoteStore:
    """quotes.csv / .sync_state.json / quarantine 的原子化落盘管理器。

    状态机每个主键三态：
      - ``processing``：已开始（请求前落盘），崩溃后重跑需回查 CSV；
      - ``done``：CSV 已追加并 flush，状态原子替换完成；
      - ``quarantined``：坏数据终态，已写入隔离区；重跑直接跳过，
        不重复请求、不重复刷隔离文件（需要重试可手动删 state 条目）。

    所有 .sync_state.json 更新一律 临时文件 + os.replace，避免半截 JSON。
    """

    def __init__(self, base_dir: str):
        self.base_dir = os.path.abspath(base_dir)
        self.quotes_path = os.path.join(self.base_dir, QUOTES_FILENAME)
        self.state_path = os.path.join(self.base_dir, STATE_FILENAME)
        self.quarantine_dir = os.path.join(self.base_dir, QUARANTINE_DIRNAME)
        self.bad_rows_path = os.path.join(self.quarantine_dir, BAD_ROWS_FILENAME)
        self._state: dict[str, dict] = {}
        self._csv_keys: set[tuple[str, str]] | None = None
        self._load_state()
        self._ensure_csv()

    # ── 初始化与原子写 ──────────────────────────────────────
    def _load_state(self) -> None:
        if not os.path.exists(self.state_path):
            self._state = {}
            return
        with open(self.state_path, "r", encoding="utf-8") as fh:
            try:
                self._state = json.load(fh)
            except json.JSONDecodeError:
                # 状态文件损坏不应阻断恢复：CSV 才是落盘事实来源
                logger.warning("state 文件 JSON 损坏，按空状态重建: %s", self.state_path)
                self._state = {}

    def _write_state_atomic(self) -> None:
        """临时文件 + os.replace 原子更新 .sync_state.json。"""
        os.makedirs(self.base_dir, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".sync_state.", suffix=".tmp", dir=self.base_dir)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(self._state, fh, ensure_ascii=False, indent=2, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.state_path)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def _ensure_csv(self) -> None:
        """quotes.csv 固定表头 + utf-8-sig；已存在则不动，绝不重复写表头。"""
        os.makedirs(self.base_dir, exist_ok=True)
        if os.path.exists(self.quotes_path) and os.path.getsize(self.quotes_path) > 0:
            return
        with open(self.quotes_path, "w", encoding="utf-8-sig", newline="") as fh:
            csv.writer(fh).writerow(CSV_HEADER)
            fh.flush()
            os.fsync(fh.fileno())

    def _load_csv_keys(self) -> set[tuple[str, str]]:
        """扫描 quotes.csv 建立业务主键集合（崩溃窗口恢复的事实来源）。"""
        keys: set[tuple[str, str]] = set()
        if not os.path.exists(self.quotes_path):
            return keys
        # utf-8-sig 可同时兼容带/不带 BOM 的文件
        with open(self.quotes_path, "r", encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                sym = (row.get("symbol") or "").strip()
                tdate = (row.get("trade_date") or "").strip()
                if sym and tdate:
                    keys.add((sym, tdate))
        return keys

    @property
    def csv_keys(self) -> set[tuple[str, str]]:
        if self._csv_keys is None:
            self._csv_keys = self._load_csv_keys()
        return self._csv_keys

    # ── 状态查询（重跑恢复入口） ────────────────────────────
    def is_done(self, key: tuple[str, str]) -> bool:
        entry = self._state.get(self._key_id(key))
        return bool(entry and entry.get("status") == "done")

    @staticmethod
    def _key_id(key: tuple[str, str]) -> str:
        return f"{key[0]}|{key[1]}"

    def reconcile(self, key: tuple[str, str]) -> bool:
        """重跑恢复：processing 或缺 done 的主键先回查 CSV。

        命中 CSV -> 补记 done 并打日志，返回 True（无需再请求/追加）；
        未命中 -> 返回 False，由调用方重新请求后走正常追加流程。
        """
        key_id = self._key_id(key)
        if key in self.csv_keys:
            self._state[key_id] = {
                "status": "done",
                "symbol": key[0],
                "trade_date": key[1],
                "recovered_from": "csv_reconcile",
                "updated_at": _now_iso(),
            }
            self._write_state_atomic()
            logger.info("主键 %s 在 quotes.csv 已存在，补记 done（CSV/state 崩溃窗口恢复）", key_id)
            return True
        logger.info("主键 %s 状态未完成且 CSV 未命中，重新请求追加", key_id)
        return False

    # ── 状态机驱动的同步流程 ────────────────────────────────
    def mark_processing(self, key: tuple[str, str], trace_url: str = "") -> None:
        """请求前先置 processing 并原子落盘。"""
        key_id = self._key_id(key)
        self._state[key_id] = {
            "status": "processing",
            "symbol": key[0],
            "trade_date": key[1],
            "trace_url": trace_url,
            "updated_at": _now_iso(),
        }
        self._write_state_atomic()

    def append_validated(self, row: dict,
                         expected_key: tuple[str, str] | None = None
                         ) -> tuple[str, tuple[str, str]]:
        """校验 + 幂等追加 + 原子置 done。

        ``expected_key`` 为请求侧业务主键：状态机与 CSV 去重一律锚定它，
        避免响应里的异常 symbol/trade_date 把状态写到另一个主键下
        （坏值仍按响应原值校验并记录进隔离区）。不传时退化为按行内主键。

        返回 (动作, 主键)，动作取值：
          - ``appended``：通过校验且 CSV 无此主键，已追加并置 done；
          - ``duplicate``：CSV 已存在同主键，跳过追加，仅确保 done；
          - ``quarantined``：校验失败，已写入隔离区并 warning。
        """
        key = expected_key or business_key(row)
        key_id = self._key_id(key)
        try:
            valid = validate_row(row)
        except QuarantineError as bad:
            self._quarantine(bad)
            # 坏数据落终态，重跑不再重复请求/重复追加隔离行；
            # 身份锚定请求主键，隔离文件里保留响应原值用于排查
            self._state[key_id] = {
                "status": "quarantined",
                "symbol": key[0],
                "trade_date": key[1],
                "reason": bad.reason,
                "updated_at": _now_iso(),
            }
            self._write_state_atomic()
            return "quarantined", key

        # 响应主键与请求主键不一致但值合法：告警后按请求主键锚定落盘身份
        if expected_key is not None and (
                valid["symbol"] != key[0] or valid["trade_date"] != key[1]):
            logger.warning("响应主键与请求不一致 req=%s resp=(%s,%s)，按请求主键锚定",
                           key_id, valid["symbol"], valid["trade_date"])
            valid["symbol"], valid["trade_date"] = key

        if key in self.csv_keys:
            # 双保险：即便状态机漏判，CSV 主键也不允许重复
            logger.info("主键 %s 已在 quotes.csv，跳过追加", key_id)
            self._state[key_id] = {
                "status": "done",
                "symbol": key[0],
                "trade_date": key[1],
                "updated_at": _now_iso(),
            }
            self._write_state_atomic()
            return "duplicate", key

        with open(self.quotes_path, "a", encoding="utf-8-sig", newline="") as fh:
            csv.writer(fh).writerow([valid[h] for h in CSV_HEADER])
            fh.flush()
            os.fsync(fh.fileno())
        self._csv_keys = (self._csv_keys or set()) | {key}

        # CSV 已 flush 之后再原子置 done
        self._state[key_id] = {
            "status": "done",
            "symbol": valid["symbol"],
            "trade_date": valid["trade_date"],
            "close": valid["close"],
            "turnover": valid["turnover"],
            "updated_at": _now_iso(),
        }
        self._write_state_atomic()
        return "appended", key

    # ── 坏行隔离 ────────────────────────────────────────────
    def _quarantine(self, bad: QuarantineError) -> None:
        os.makedirs(self.quarantine_dir, exist_ok=True)
        need_header = (not os.path.exists(self.bad_rows_path)
                       or os.path.getsize(self.bad_rows_path) == 0)
        row = bad.row if isinstance(bad.row, dict) else {"raw": str(bad.row)}
        logger.warning("坏行隔离: %s | row=%s", bad.reason, row)
        with open(self.bad_rows_path, "a", encoding="utf-8-sig", newline="") as fh:
            writer = csv.writer(fh)
            if need_header:
                writer.writerow(CSV_HEADER + ["reason"])
            writer.writerow([
                row.get("symbol", ""), row.get("trade_date", ""),
                row.get("close", ""), row.get("turnover", ""), bad.reason,
            ])
            fh.flush()
            os.fsync(fh.fileno())

    # ── 便捷读取（供测试/巡检） ─────────────────────────────
    def read_quotes(self) -> list[dict]:
        with open(self.quotes_path, "r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))

    def read_bad_rows(self) -> list[dict]:
        if not os.path.exists(self.bad_rows_path):
            return []
        with open(self.bad_rows_path, "r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))

    def state_snapshot(self) -> dict:
        return json.loads(json.dumps(self._state))


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


# ── 高层编排：processing -> 请求 -> 校验/追加 -> done ────────
def sync_one(store: QuoteStore,
             key: tuple[str, str],
             fetch_func,
             trace_url: str = "") -> str:
    """单个业务主键的完整同步流程，返回 append_validated 的动作。

    ``fetch_func`` 由入口脚本提供，内部应使用 ``request_with_retry`` 发请求，
    签名为 ``fetch_func(key) -> dict 行数据``；4xx / AES 解密失败会直接抛出，
    本函数不捕获（processing 状态保留，重跑时由 reconcile 回查恢复）。
    """
    key_id = store._key_id(key)  # noqa: SLF001 - 同模块编排函数
    entry = store.state_snapshot().get(key_id)

    # 重跑路径：done / quarantined 均为终态直接跳过；
    # processing/缺失先回查 CSV 再决定
    if entry and entry.get("status") == "done":
        logger.info("主键 %s 已 done，跳过", key_id)
        return "duplicate"
    if entry and entry.get("status") == "quarantined":
        logger.info("主键 %s 此前已隔离（%s），跳过",
                    key_id, entry.get("reason", ""))
        return "quarantined"
    if entry is None or entry.get("status") == "processing":
        if store.reconcile(key):
            return "recovered"

    store.mark_processing(key, trace_url=normalize_url(trace_url) if trace_url else "")
    row = fetch_func(key)  # 超时重试在内部完成；异常则 processing 保留待下次恢复
    # 落盘身份锚定请求侧主键；响应原值仍参与 validate_row 的坏数据判定
    action, _ = store.append_validated(row, expected_key=key)
    return action


# ── 入口共用的行情请求构造器 ─────────────────────────────────
def _default_sign(secret: str, symbol: str, trade_date: str, ts: str) -> str:
    """默认签名：sha256(secret|symbol|date|ts)，仅用于本地/对接默认约定。"""
    digest = hashlib.sha256("|".join([secret, symbol, trade_date, ts]).encode("utf-8"))
    return digest.hexdigest()


def build_quote_fetcher(base_url: str,
                        *,
                        session: requests.Session | None = None,
                        timeout: float = DEFAULT_TIMEOUT,
                        aes_key: bytes | None = None,
                        aes_iv: bytes | None = None,
                        secret: str = "",
                        sign_func=None,
                        sleep_func=time.sleep,
                        row_extractor=None):
    """构造 ``fetch_func(key) -> row dict``，供 :func:`sync_one` 使用。

    请求 URL 携带 ts、sign；``normalize_url`` 会在链路追踪时剥掉它们，
    写入判定完全不看这两个参数，只认 symbol + trade_date。

    响应默认按 JSON 解析并取 ``data`` 字段；接口形态不同可传 ``row_extractor``
    （``(payload, key) -> dict``）覆盖。提供 ``aes_key`` 时响应体按
    AES-CBC 密文处理，解密失败抛 ``AESDecryptError``，请求层不重试。
    """
    signer = sign_func or (lambda *parts: _default_sign(secret, *parts))
    extractor = row_extractor or _default_row_extractor

    def fetch(key: tuple[str, str]) -> dict:
        symbol, trade_date = key
        ts = str(int(time.time()))
        sign = signer(symbol, trade_date, ts)
        url = (f"{base_url}?symbol={symbol}&trade_date={trade_date}"
               f"&ts={ts}&sign={sign}")
        # 归一化 URL 仅用于日志链路追踪
        logger.debug("请求行情 trace=%s", normalize_url(url))
        resp = request_with_retry(
            "GET", url, session=session, timeout=timeout, sleep_func=sleep_func)
        text = decode_response_body(resp, aes_key=aes_key, aes_iv=aes_iv)
        payload = json.loads(text)
        return extractor(payload, key)

    return fetch


def _default_row_extractor(payload: dict, key: tuple[str, str]) -> dict:
    """默认响应形态：``{"code": 0, "data": {symbol, trade_date, close, turnover}}``。

    data 缺字段时用主键补齐 symbol/trade_date，坏值留给 validate_row 隔离。
    """
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        raise QuarantineError(f"响应缺少 data 对象: {str(payload)[:120]}",
                              {"symbol": key[0], "trade_date": key[1]})
    return {
        "symbol": data.get("symbol", key[0]),
        "trade_date": data.get("trade_date", key[1]),
        "close": data.get("close"),
        "turnover": data.get("turnover"),
    }


# ── 两个入口共用的批量编排 ───────────────────────────────────
def run_quotes_sync(keys,
                    base_url: str,
                    out_dir: str,
                    *,
                    session: requests.Session | None = None,
                    timeout: float = DEFAULT_TIMEOUT,
                    aes_key: bytes | None = None,
                    aes_iv: bytes | None = None,
                    secret: str = "",
                    sign_func=None,
                    sleep_func=time.sleep,
                    row_extractor=None) -> dict:
    """对一批 (symbol, trade_date) 主键执行完整同步，返回统计结果。

    供 sync_quotes_raw / sync_quotes_batch 共同调用，保证两条入口的
    重试策略与落盘幂等逻辑完全一致。
    """
    store = QuoteStore(out_dir)
    fetch = build_quote_fetcher(
        base_url, session=session, timeout=timeout,
        aes_key=aes_key, aes_iv=aes_iv, secret=secret,
        sign_func=sign_func, sleep_func=sleep_func,
        row_extractor=row_extractor)

    stats = {"total": len(keys), "appended": 0, "duplicate": 0,
             "recovered": 0, "quarantined": 0, "failed": 0}
    for key in keys:
        try:
            action = sync_one(store, key, fetch, trace_url=base_url)
        except AESDecryptError:
            # 确定性错误：不重试、不隔离，直接上抛交给调用方
            raise
        except requests.HTTPError as exc:
            # 4xx 在此抛出（5xx 已在请求层重试耗尽）
            logger.error("主键 %s 请求失败: %s", key, exc)
            stats["failed"] += 1
            continue
        except requests.RequestException as exc:
            logger.error("主键 %s 传输失败: %s", key, exc)
            stats["failed"] += 1
            continue
        stats[action] = stats.get(action, 0) + 1
    logger.info("同步结束 stats=%s out_dir=%s", stats, out_dir)
    return stats
