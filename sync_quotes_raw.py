#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
sync_quotes_raw.py
==================
行情同步入口 —— 原始/单批模式。

从 --input 指定的 CSV（至少含 symbol、trade_date 两列）或命令行
--symbol/--trade-date 读取业务主键，逐个同步到 quotes.csv。

重试与落盘幂等全部复用 sync_quotes_common：
  - 请求层只对 ConnectTimeout / 5xx / 空 body 重试（1s、2s、4s + 抖动，最多 4 次）；
  - 4xx、AES 解密失败直接抛；
  - 写入只认 symbol + trade_date，URL 上的 ts/sign 仅用于链路追踪；
  - .sync_state.json 走临时文件 + os.replace，processing -> done 状态机；
  - 坏行进 quarantine/quotes_bad.csv。
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys

from sync_quotes_common import (
    DEFAULT_TIMEOUT,
    QuarantineError,
    business_key,
    run_quotes_sync,
    validate_row,
)

logger = logging.getLogger("sync_quotes.raw")


def load_keys_from_csv(path: str) -> list[tuple[str, str]]:
    """从输入 CSV 读取主键；只取 symbol/trade_date 两列并去重保序。"""
    keys: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            key = business_key(row)
            if key not in seen:
                seen.add(key)
                keys.append(key)
    return keys


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="开盘啦行情同步（raw 模式：显式主键清单）")
    parser.add_argument("--base-url",
                        default=os.environ.get("QUOTES_API_URL",
                                               "https://apphis.longhuvip.com/w1/api/quotes"),
                        help="行情接口基地址（ts/sign 自动拼接）")
    parser.add_argument("--out-dir", default=os.path.join(os.getcwd(), "data", "quotes"),
                        help="quotes.csv / .sync_state.json 输出目录")
    parser.add_argument("--input",
                        help="主键清单 CSV，需含 symbol、trade_date 列")
    parser.add_argument("--symbol", help="单个 6 位股票代码（与 --trade-date 搭配）")
    parser.add_argument("--trade-date", help="单个交易日 YYYYMMDD")
    parser.add_argument("--aes-key", default=os.environ.get("QUOTES_AES_KEY"),
                        help="AES key（十六进制）；不填则按明文 JSON 处理")
    parser.add_argument("--aes-iv", default=os.environ.get("QUOTES_AES_IV"),
                        help="AES iv（十六进制，16 字节）")
    parser.add_argument("--secret", default=os.environ.get("QUOTES_SIGN_SECRET", ""),
                        help="ts/sign 签名密钥")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT)
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if args.input:
        keys = load_keys_from_csv(args.input)
    elif args.symbol and args.trade_date:
        # 走一遍校验，非法主键直接拒绝，不产生无意义请求
        try:
            validate_row({"symbol": args.symbol, "trade_date": args.trade_date,
                          "close": "0", "turnover": "0"})
        except QuarantineError as exc:
            logger.error("命令行主键非法: %s", exc.reason)
            return 2
        keys = [(args.symbol.strip(), args.trade_date.strip())]
    else:
        logger.error("必须提供 --input CSV 或 --symbol + --trade-date")
        return 2

    aes_key = bytes.fromhex(args.aes_key) if args.aes_key else None
    aes_iv = bytes.fromhex(args.aes_iv) if args.aes_iv else None

    logger.info("raw 模式启动，主键 %d 个，输出目录 %s", len(keys), args.out_dir)
    stats = run_quotes_sync(
        keys, args.base_url, args.out_dir,
        timeout=args.timeout, aes_key=aes_key, aes_iv=aes_iv,
        secret=args.secret)
    logger.info("raw 模式结束: %s", stats)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
