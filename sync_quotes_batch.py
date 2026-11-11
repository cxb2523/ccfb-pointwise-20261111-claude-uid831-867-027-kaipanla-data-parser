#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_quotes_batch.py
====================
行情同步入口 —— 批量模式。

以一个 6 位代码清单（文件或命令行）+ 一个或多个交易日为输入，
笛卡尔展开成 (symbol, trade_date) 主键后调用公共同步引擎。

与 sync_quotes_raw.py 的区别仅在输入形态：
  - raw   ：显式主键 CSV（symbol, trade_date 成对给出）；
  - batch ：代码清单 + 日期列表自动展开。

重试、AES、状态机、CSV 回查幂等、隔离区机制完全一致（同一公共引擎）。
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from sync_quotes_common import (
    QuarantineError,
    run_quotes_sync,
    validate_row,
)

logger = logging.getLogger("sync_quotes.batch")


def load_symbols(path: str) -> list[str]:
    """读代码清单：每行一个 6 位代码，空行/# 注释跳过。"""
    symbols: list[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            code = line.strip()
            if not code or code.startswith("#"):
                continue
            symbols.append(code)
    return symbols


def expand_keys(symbols: list[str], trade_dates: list[str]) -> list[tuple[str, str]]:
    """代码 × 交易日笛卡尔展开，同时剔除重复代码并保序。"""
    keys: list[tuple[str, str]] = []
    seen: set[str] = set()
    for sym in symbols:
        if sym in seen:
            continue
        seen.add(sym)
        for tdate in trade_dates:
            keys.append((sym, tdate))
    return keys


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="开盘啦行情同步（batch 模式：代码清单 × 交易日展开）")
    parser.add_argument("--base-url",
                        default=os.environ.get("QUOTES_API_URL",
                                               "https://apphis.longhuvip.com/w1/api/quotes"),
                        help="URL 中的 ts/sign 仅用于请求签名与追踪，不参与写入判定")
    parser.add_argument("--out-dir", default=os.path.join(os.getcwd(), "data", "quotes"),
                        help="quotes.csv / .sync_state.json 输出目录")
    parser.add_argument("--symbols-file",
                        help="每行一个 6 位代码，支持空行和 # 注释")
    parser.add_argument("--symbols",
                        help="代码逗号分隔，如 600000,000001")
    parser.add_argument("--trade-dates",
                        help="交易日逗号分隔，如 20260508,20260511")
    parser.add_argument("--aes-key", default=os.environ.get("QUOTES_AES_KEY"),
                        help="AES key（十六进制）；不填则按明文 JSON 处理")
    parser.add_argument("--aes-iv", default=os.environ.get("QUOTES_AES_IV"),
                        help="AES iv（十六进制，16 字节）")
    parser.add_argument("--secret", default=os.environ.get("QUOTES_SIGN_SECRET", ""),
                        help="ts/sign 签名密钥")
    parser.add_argument("--timeout", type=float, default=15)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.symbols_file:
        symbols = load_symbols(args.symbols_file)
    elif args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    else:
        logger.error("必须提供 --symbols-file 或 --symbols")
        return 2

    if not args.trade_dates:
        logger.error("必须提供 --trade-dates（YYYYMMDD 逗号分隔）")
        return 2
    trade_dates = [d.strip() for d in args.trade_dates.split(",") if d.strip()]

    # 提前校验代码/日期形态，避免展开后产生大量无意义请求
    try:
        for sym in symbols:
            validate_row({"symbol": sym, "trade_date": trade_dates[0],
                          "close": "0", "turnover": "0"})
        for tdate in trade_dates:
            validate_row({"symbol": symbols[0], "trade_date": tdate,
                          "close": "0", "turnover": "0"})
    except QuarantineError as exc:
        logger.error("代码/交易日非法: %s", exc.reason)
        return 2

    keys = expand_keys(symbols, trade_dates)
    aes_key = bytes.fromhex(args.aes_key) if args.aes_key else None
    aes_iv = bytes.fromhex(args.aes_iv) if args.aes_iv else None

    logger.info("batch 模式启动，代码 %d 个 × 日期 %d 天 = 主键 %d 个，输出目录 %s",
                len(symbols), len(trade_dates), len(keys), args.out_dir)
    stats = run_quotes_sync(
        keys, args.base_url, args.out_dir,
        timeout=args.timeout, aes_key=aes_key, aes_iv=aes_iv,
        secret=args.secret)
    logger.info("batch 模式结束: %s", stats)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
