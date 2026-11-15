"""Convert Bybit BTC market CSV data into a NautilusTrader Parquet catalog.

Supported Nautilus market-event types
-------------------------------------
* BTCUSDT one-minute OHLCV candles          -> ``Bar``
* BTC option executions / public trades     -> ``TradeTick``
* Timestamped BTC option best bid/ask       -> ``QuoteTick``
* Greeks present on those timestamped rows  -> ``OptionGreeks``

Inputs may be separate CSV files or one tagged master CSV whose ``record_type``
column routes rows (``underlying_candle``, ``option_execution``,
``option_quote`` / ``option_ticker``).

Only public Bybit market endpoints are used. No API key is needed and no order
is ever submitted.

What changed compared with the first version
--------------------------------------------
* Prices/sizes are conformed to each instrument's precision and checked against
  its tick size / quantity step (instruments are resolved BEFORE normalizing).
* Instrument ``ts_event``/``ts_init`` are deterministic (0), not "now".
* ``tkinter`` is imported lazily, so the script runs headless with CLI paths.
* Network/API failures are no longer confused with "contract expired"; the
  fallback is used only when Bybit has no record AND the expiry is in the past.
* Requests use retries with backoff.
* Quote-file detection uses the same alias map as quote normalization.
* Two files for the same role are an error instead of silently overwritten.
* The catalog is written to a temp folder and renamed only on success; an
  existing catalog needs ``--overwrite``.
* One-sided quotes are skipped, but their greeks are still kept; blank bid/ask
  cells no longer abort the run; duplicate quotes are dropped.
* Bar timestamps follow Nautilus convention (``ts_event`` = bar close).
* Fractional epoch timestamps, configurable timezone for naive strings,
  locale-independent month parsing, hashed (not truncated) long trade IDs.
* Optional maker/taker fees for instruments.

Remaining limitation: an expired option may have no exact historical Bybit
spec. The fallback uses current BTC option rules only when every listed BTC
USDT option shares one tick size and quantity step, and the data conforms to
them. That is plausible, not proof. For exact specs supply a CSV with
``symbol,tickSize,qtyStep[,launchTime,deliveryTime]`` via ``--specs``.
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import os
import re
import shutil
import time
from datetime import datetime, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from nautilus_trader.model.currencies import Currency
from nautilus_trader.model.data import Bar, BarType, OptionGreeks, QuoteTick, TradeTick
from nautilus_trader.model.enums import AggressorSide, OptionKind
from nautilus_trader.model.identifiers import InstrumentId, Symbol, TradeId
from nautilus_trader.model.instruments import CryptoOption, CryptoPerpetual
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog


LOG = logging.getLogger("bybit_csv_to_catalog")

OPTION_RE = re.compile(
    r"^(?P<base>BTC)-(?P<expiry>\d{1,2}[A-Z]{3}\d{2})-"
    r"(?P<strike>\d+(?:\.\d+)?)-(?P<kind>[CP])-(?P<quote>USDT)$",
    re.IGNORECASE,
)
MONTHS = {m: i for i, m in enumerate(
    ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], start=1)}

UNDERLYING_ID = InstrumentId.from_str("BTCUSDT-LINEAR.BYBIT")
BAR_TYPE = BarType.from_str("BTCUSDT-LINEAR.BYBIT-1-MINUTE-LAST-EXTERNAL")
BAR_INTERVAL_NS = 60_000_000_000
BYBIT_API = "https://api.bybit.com"

# Instruments are static definitions, so they get a fixed timestamp that is
# earlier than any market data. This keeps every run reproducible.
INSTRUMENT_TS_NS = 0

# Timezone applied to timestamp strings without an offset (e.g. "2026-11-11 08:00:00").
NAIVE_TZ = "UTC"

MASTER_KINDS = {"underlying_candle", "option_execution", "option_quote", "option_ticker"}
CANDLE_COLUMNS = {"timestamp_ms", "open", "high", "low", "close", "volume_btc"}
EXECUTION_COLUMNS = {
    "exec_id", "order_id", "timestamp_ms", "symbol", "side",
    "exec_qty_btc", "exec_price_usdt", "exec_fee_usdt", "is_maker",
}
PUBLIC_TRADE_COLUMNS = {"symbol", "price", "size", "side", "time"}
QUOTE_COLUMNS = {"symbol", "bid_price", "bid_size", "ask_price", "ask_size"}
GREEK_NAMES = ("delta", "gamma", "vega", "theta")

QUOTE_ALIASES = {
    "ts": "timestamp", "time": "timestamp", "datetime": "timestamp",
    "timestamp_utc": "timestamp", "exchange_timestamp": "timestamp",
    "ts_event": "timestamp", "timestamp_ns": "timestamp", "timestamp_us": "timestamp",
    "ts_ms": "timestamp", "ts_ns": "timestamp",
    "bidPrice": "bid_price", "bid1Price": "bid_price", "bid": "bid_price",
    "bidSize": "bid_size", "bid1Size": "bid_size",
    "askPrice": "ask_price", "ask1Price": "ask_price", "ask": "ask_price",
    "askSize": "ask_size", "ask1Size": "ask_size",
    "markPriceIv": "mark_iv", "markIv": "mark_iv",
    "bidIv": "bid_iv", "askIv": "ask_iv",
    "underlyingPrice": "underlying_price", "markPrice": "mark_price",
    "openInterest": "open_interest",
}


# --------------------------------------------------------------------------
# Errors
# --------------------------------------------------------------------------
class OffGridError(ValueError):
    """A price or size is not a multiple of the instrument's increment."""


class SpecNotFound(LookupError):
    """Bybit answered successfully but has no record for the symbol."""


class BybitApiError(RuntimeError):
    """Bybit answered with a non-zero retCode."""


# --------------------------------------------------------------------------
# Small numeric / time helpers
# --------------------------------------------------------------------------
def text_decimal(value: Any, field: str) -> str:
    """Return a finite decimal as plain text (never via float)."""
    if value is None or pd.isna(value) or not str(value).strip():
        raise ValueError(f"{field} is empty")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} is not numeric: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    return format(number, "f")


def optional_decimal(value: Any, field: str) -> Decimal | None:
    """Like ``text_decimal`` but a blank cell returns None instead of failing."""
    if value is None or not str(value).strip():
        return None
    return Decimal(text_decimal(value, field))


def optional_float(row: dict[str, Any], name: str) -> float | None:
    text = str(row.get(name, "")).strip()
    return float(text_decimal(text, name)) if text else None


def to_ns(value: Any, field: str) -> int:
    """Convert a timestamp to Nautilus nanoseconds.

    Numeric input (integer or fractional) is interpreted as seconds, ms, us or
    ns by magnitude. Text is parsed as a date/time; if it carries no timezone,
    ``NAIVE_TZ`` is applied (default UTC).
    """
    raw = str(value).strip()
    if not raw:
        raise ValueError(f"{field} is empty")
    try:
        number = Decimal(raw)
    except (InvalidOperation, ValueError):
        try:
            stamp = pd.Timestamp(raw)
            if stamp.tzinfo is None:
                stamp = stamp.tz_localize(NAIVE_TZ)
            return int(stamp.tz_convert("UTC").value)
        except Exception as exc:
            raise ValueError(f"{field} is not a supported timestamp: {value!r}") from exc
    if not number.is_finite() or number <= 0:
        raise ValueError(f"{field} must be a positive timestamp")
    if number < 10**11:
        factor = 10**9      # seconds
    elif number < 10**14:
        factor = 10**6      # milliseconds
    elif number < 10**17:
        factor = 10**3      # microseconds
    else:
        factor = 1          # nanoseconds
    return int(number * factor)


def parse_bool(value: Any, field: str) -> bool:
    value = str(value).strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    raise ValueError(f"{field} is not a boolean: {value!r}")


def parse_expiry_ms(symbol: str) -> int:
    """Expiry of a Bybit option: 08:00 UTC on the date in the symbol (locale-free)."""
    match = OPTION_RE.fullmatch(symbol.strip().upper())
    if match is None:
        raise ValueError(f"unsupported BTC option contract symbol {symbol!r}")
    text = match.group("expiry").upper()
    day, month_name, year = int(text[:-5]), text[-5:-2], int(text[-2:])
    expiry = datetime(2000 + year, MONTHS[month_name], day, 8, tzinfo=timezone.utc)
    return int(expiry.timestamp() * 1000)


def is_expired(symbol: str) -> bool:
    return parse_expiry_ms(symbol) <= int(time.time() * 1000)


def option_instrument_id(symbol: str) -> InstrumentId:
    symbol = symbol.strip().upper()
    if not OPTION_RE.fullmatch(symbol):
        raise ValueError(f"not a supported BTC USDT option symbol: {symbol!r}")
    return InstrumentId.from_str(f"{symbol}-OPTION.BYBIT")


def make_trade_id(exec_id: str) -> str:
    """Nautilus trade IDs are limited to 36 chars; hash long ones, don't truncate."""
    if len(exec_id) <= 36:
        return exec_id
    return hashlib.sha256(exec_id.encode("utf-8")).hexdigest()[:36]


# --------------------------------------------------------------------------
# Conforming values to an instrument's grid
# --------------------------------------------------------------------------
def _conform(value: Decimal, increment: Decimal, precision: int, snap: bool, label: str) -> str:
    """Return ``value`` as text with ``precision`` decimals, on the increment grid.

    With ``snap=False`` an off-grid value raises ``OffGridError``; with
    ``snap=True`` it is rounded to the nearest increment.
    """
    if value % increment != 0:
        if not snap:
            raise OffGridError(f"{label} {value} is not a multiple of increment {increment}")
        value = (value / increment).quantize(Decimal(1), rounding=ROUND_HALF_UP) * increment
    return format(value.quantize(Decimal(1).scaleb(-precision)), "f")


def conform_price(instrument, value: Decimal, label: str, snap: bool = False) -> Price:
    return Price.from_str(_conform(
        value, Decimal(str(instrument.price_increment)), instrument.price_precision, snap, label))


def conform_qty(instrument, value: Decimal, label: str, snap: bool = False) -> Quantity:
    return Quantity.from_str(_conform(
        value, Decimal(str(instrument.size_increment)), instrument.size_precision, snap, label))


def is_fallback(instrument) -> bool:
    return bool((getattr(instrument, "info", None) or {}).get("historicalSpecFallback"))


# --------------------------------------------------------------------------
# Bybit public API
# --------------------------------------------------------------------------
def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "bybit-nautilus-csv-converter/2.0"})
    retry = Retry(total=4, backoff_factor=0.5, status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=("GET",))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def bybit_get(session: requests.Session, path: str, params: dict[str, str]) -> dict[str, Any]:
    response = session.get(f"{BYBIT_API}{path}", params=params, timeout=20)
    response.raise_for_status()
    body = response.json()
    if body.get("retCode") != 0:
        raise BybitApiError(f"{path} {params}: retCode={body.get('retCode')} {body.get('retMsg')}")
    return body


def fetch_instrument_spec(session: requests.Session, category: str, symbol: str) -> dict[str, Any]:
    """Fetch one instrument's public spec from Bybit ``instruments-info``.

    Raises ``SpecNotFound`` when Bybit answers but has no such symbol, and
    ``BybitApiError`` / ``requests`` exceptions for real failures.
    """
    body = bybit_get(session, "/v5/market/instruments-info", {"category": category, "symbol": symbol})
    for row in body.get("result", {}).get("list", []):
        if str(row.get("symbol", "")).upper() == symbol.upper():
            return {**row, "metadataSource": "Bybit instruments-info"}
    raise SpecNotFound(f"Bybit has no {category} instrument {symbol}")


def fetch_uniform_btc_option_rules(session: requests.Session) -> dict[str, Any]:
    """Current BTC USDT option increments, only if every listed contract agrees."""
    cursor = ""
    rows: list[dict[str, Any]] = []
    server_time = 0
    while True:
        params = {"category": "option", "baseCoin": "BTC", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        body = bybit_get(session, "/v5/market/instruments-info", params)
        result = body.get("result", {})
        rows.extend(r for r in result.get("list", []) if OPTION_RE.fullmatch(str(r.get("symbol", "")).upper()))
        server_time = int(body.get("time", 0))
        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break
    ticks = {str((r.get("priceFilter") or {}).get("tickSize", "")) for r in rows} - {""}
    steps = {str((r.get("lotSizeFilter") or {}).get("qtyStep", "")) for r in rows} - {""}
    if not rows or len(ticks) != 1 or len(steps) != 1:
        raise ValueError(
            "Expired option specs are not available and currently listed BTC USDT options do not "
            "share one tickSize/qtyStep. Provide exact specs with --specs."
        )
    return {"tickSize": ticks.pop(), "qtyStep": steps.pop(), "asOfMs": server_time, "symbolsChecked": len(rows)}


def expired_option_spec_from_common_rule(symbol: str, rules: dict[str, Any]) -> dict[str, Any]:
    """Clearly tagged approximation for an expired option (see module docstring)."""
    match = OPTION_RE.fullmatch(symbol.upper())
    if match is None:
        raise ValueError(f"unsupported BTC option contract symbol {symbol!r}")
    return {
        "symbol": symbol, "category": "option",
        "baseCoin": match.group("base"), "quoteCoin": match.group("quote"),
        "settleCoin": match.group("quote"),
        "launchTime": "0", "deliveryTime": str(parse_expiry_ms(symbol)),
        "priceFilter": {"tickSize": rules["tickSize"]},
        "lotSizeFilter": {"qtyStep": rules["qtyStep"]},
        "metadataSource": "Bybit current uniform BTC option instruments-info rule",
        "currentRulesAsOfMs": rules["asOfMs"],
        "currentRuleSymbolsChecked": rules["symbolsChecked"],
        "historicalLaunchTimeUnavailable": True,
        "historicalSpecFallback": True,
    }


def load_archived_option_specs(paths: list[Path]) -> dict[str, dict[str, Any]]:
    """Exact option increments from any CSV that provides symbol + tick + step columns."""
    specs: dict[str, dict[str, Any]] = {}
    tick_names = ("tickSize", "tick_size", "ticksize", "price_tick_size")
    step_names = ("qtyStep", "qty_step", "qtystep", "quantity_step", "size_increment")
    for path in paths:
        try:
            columns = set(pd.read_csv(path, dtype=str, nrows=0).columns)
            tick_col = next((n for n in tick_names if n in columns), None)
            step_col = next((n for n in step_names if n in columns), None)
            if "symbol" not in columns or tick_col is None or step_col is None:
                continue
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
        except (OSError, pd.errors.ParserError, UnicodeDecodeError):
            continue
        for row in frame.to_dict("records"):
            symbol = str(row.get("symbol", "")).strip().upper()
            tick = str(row.get(tick_col, "")).strip()
            step = str(row.get(step_col, "")).strip()
            if not OPTION_RE.fullmatch(symbol) or not tick or not step:
                continue
            match = OPTION_RE.fullmatch(symbol)
            specs[symbol] = {
                "symbol": symbol, "category": "option", "baseCoin": "BTC",
                "quoteCoin": match.group("quote"), "settleCoin": match.group("quote"),
                "launchTime": row.get("launchTime") or row.get("launch_time_ms") or "0",
                "deliveryTime": row.get("deliveryTime") or row.get("delivery_time_ms")
                                or str(parse_expiry_ms(symbol)),
                "priceFilter": {"tickSize": tick}, "lotSizeFilter": {"qtyStep": step},
                "metadataSource": f"archived CSV: {path.name}",
            }
    return specs


# --------------------------------------------------------------------------
# Instrument construction
# --------------------------------------------------------------------------
def _increment(value: Any, field: str, *, quantity: bool = False):
    text = text_decimal(value, field)
    if Decimal(text) <= 0:
        raise ValueError(f"{field} must be positive")
    return Quantity.from_str(text) if quantity else Price.from_str(text)


def _spec_qty(value: Any, precision: int) -> Quantity | None:
    if not value:
        return None
    number = Decimal(text_decimal(value, "quantity limit"))
    return Quantity.from_str(format(number.quantize(Decimal(1).scaleb(-precision)), "f"))


def build_native_instrument(category: str, symbol: str, spec: dict[str, Any],
                            maker_fee: Decimal | None = None, taker_fee: Decimal | None = None):
    """Build ``CryptoPerpetual`` (linear) or ``CryptoOption`` (option) from a spec."""
    price_filter = spec.get("priceFilter") or {}
    lot_filter = spec.get("lotSizeFilter") or {}
    price_increment = _increment(price_filter.get("tickSize"), "tickSize")
    size_increment = _increment(lot_filter.get("qtyStep"), "qtyStep", quantity=True)
    raw_symbol = Symbol(symbol)
    base = Currency.from_str(str(spec.get("baseCoin", "BTC")))
    quote = Currency.from_str(str(spec.get("quoteCoin", "USDT")))
    settlement = Currency.from_str(str(spec.get("settleCoin") or spec.get("quoteCoin", "USDT")))
    min_q = _spec_qty(lot_filter.get("minOrderQty"), size_increment.precision)
    max_q = _spec_qty(lot_filter.get("maxOrderQty") or lot_filter.get("maxMktOrderQty"),
                      size_increment.precision)
    fees: dict[str, Decimal] = {}
    if maker_fee is not None:
        fees["maker_fee"] = maker_fee
    if taker_fee is not None:
        fees["taker_fee"] = taker_fee

    if category == "linear":
        return CryptoPerpetual(
            UNDERLYING_ID, raw_symbol, base, quote, settlement, False,
            price_increment.precision, size_increment.precision,
            price_increment, size_increment,
            INSTRUMENT_TS_NS, INSTRUMENT_TS_NS,
            min_quantity=min_q, max_quantity=max_q, info=spec, **fees,
        )

    match = OPTION_RE.fullmatch(symbol.upper())
    if match is None:
        raise ValueError(f"unsupported option contract symbol {symbol!r}")
    launch_ms = int(spec.get("launchTime") or 0)
    delivery_ms = int(spec.get("deliveryTime") or 0)
    if delivery_ms <= 0:
        raise ValueError(f"Bybit option spec lacks a valid delivery time: {symbol}")
    option_kind = OptionKind.CALL if match.group("kind") == "C" else OptionKind.PUT
    return CryptoOption(
        option_instrument_id(symbol), raw_symbol, base, quote, settlement, False,
        option_kind, Price.from_str(match.group("strike")),
        launch_ms * 1_000_000 if launch_ms > 0 else 0, delivery_ms * 1_000_000,
        price_increment.precision, size_increment.precision,
        price_increment, size_increment, INSTRUMENT_TS_NS, INSTRUMENT_TS_NS,
        max_quantity=max_q, min_quantity=min_q, info=spec, **fees,
    )


def resolve_instruments(session: requests.Session, symbols: list[str], archived: dict[str, dict[str, Any]],
                        need_perp: bool, allow_fallback: bool,
                        maker_fee: Decimal | None, taker_fee: Decimal | None):
    """Resolve every instrument BEFORE any data is normalized or written.

    Spec priority per option: archived CSV spec -> live Bybit spec -> tagged
    fallback (only if Bybit has no record, or answered with an API error, AND
    the contract's expiry is in the past). Network failures always abort.
    """
    perp = None
    if need_perp:
        perp_spec = fetch_instrument_spec(session, "linear", "BTCUSDT")
        perp = build_native_instrument("linear", "BTCUSDT", perp_spec, maker_fee, taker_fee)

    options: dict[str, Any] = {}
    fallback_rules: dict[str, Any] | None = None
    fallback_count = 0
    for index, symbol in enumerate(symbols, start=1):
        if symbol in archived:
            spec = archived[symbol]
            LOG.info("Using archived option spec for %s", symbol)
        else:
            LOG.info("Fetching Bybit option spec %d/%d: %s", index, len(symbols), symbol)
            try:
                spec = fetch_instrument_spec(session, "option", symbol)
            except (SpecNotFound, BybitApiError) as exc:
                if not allow_fallback or not is_expired(symbol):
                    raise ValueError(
                        f"{symbol}: no exact spec ({exc}); fallback is only allowed for expired "
                        f"contracts and when not disabled with --no-fallback."
                    ) from exc
                if fallback_rules is None:
                    fallback_rules = fetch_uniform_btc_option_rules(session)
                spec = expired_option_spec_from_common_rule(symbol, fallback_rules)
                fallback_count += 1
                LOG.warning(
                    "%s: exact archived metadata unavailable; using current uniform rule tickSize=%s "
                    "qtyStep=%s. Historical launch time unknown.",
                    symbol, fallback_rules["tickSize"], fallback_rules["qtyStep"],
                )
            time.sleep(0.05)
        options[symbol] = build_native_instrument("option", symbol, spec, maker_fee, taker_fee)
    return perp, options, fallback_count


# --------------------------------------------------------------------------
# CSV reading and file identification
# --------------------------------------------------------------------------
def read_csv(path: Path, required: set[str], record_types: set[str] | None = None) -> pd.DataFrame:
    """Read a CSV as text; filter tagged master rows; check required columns."""
    if not path.is_file():
        raise FileNotFoundError(f"required CSV not found: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    if record_types and "record_type" in frame.columns:
        frame = frame[frame["record_type"].isin(record_types)].reset_index(drop=True)
        if frame.empty:
            raise ValueError(f"{path.name} contains no rows of type {sorted(record_types)}")
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
    if frame.empty:
        raise ValueError(f"{path.name} has no rows")
    return frame


def _header(path: Path) -> set[str]:
    return set(pd.read_csv(path, dtype=str, nrows=0).columns)


def select_csv_files(files: list[str]) -> list[Path]:
    """Use CLI paths, or open a file picker (tkinter is imported only here)."""
    if files:
        return [Path(name).expanduser().resolve() for name in files]
    try:
        import tkinter as tk
        from tkinter import filedialog
    except ImportError as exc:
        raise SystemExit("No CSV paths given and tkinter is unavailable; pass file paths.") from exc
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        selected = filedialog.askopenfilenames(
            title="Select Bybit BTC candles, option trades, and/or timestamped option quotes",
            initialdir=str(Path.cwd()),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
    finally:
        root.destroy()
    return [Path(name).resolve() for name in selected]


def show_error(title: str, message: str) -> None:
    """Best-effort GUI error popup; silently skipped when no GUI is available."""
    try:
        import tkinter as tk
        from tkinter import messagebox
        dialog = tk.Tk()
        dialog.withdraw()
        try:
            messagebox.showerror(title, message, parent=dialog)
        finally:
            dialog.destroy()
    except Exception:
        pass


def identify_input_csvs(paths: list[Path]) -> tuple[Path | None, Path | None, Path | None, Path | None]:
    """Return (candles, account executions, public trades, quotes) paths."""
    master = []
    for path in paths:
        if "record_type" in _header(path):
            kinds = set(pd.read_csv(path, dtype=str, usecols=["record_type"])["record_type"])
            if kinds & MASTER_KINDS:
                master.append((path, kinds))
    if master:
        if len(master) != 1:
            raise ValueError("Select only one tagged master market-data CSV.")
        path, kinds = master[0]
        others = [p.name for p in paths if p != path]
        if others:
            LOG.info("Master CSV selected; other files are used only for instrument specs: %s", others)
        return (
            path if "underlying_candle" in kinds else None,
            path if "option_execution" in kinds else None,
            None,
            path if kinds & {"option_quote", "option_ticker"} else None,
        )

    roles: dict[str, list[Path]] = {"candles": [], "executions": [], "public": [], "quotes": []}
    descriptions = []
    selected_order_history = False
    for path in paths:
        columns = _header(path)
        canonical = columns | {QUOTE_ALIASES[c] for c in columns if c in QUOTE_ALIASES}
        descriptions.append(f"{path.name}: {', '.join(list(pd.read_csv(path, dtype=str, nrows=0).columns)[:12])}")
        selected_order_history |= {"order_id", "order_status", "qty_btc"} <= columns
        if CANDLE_COLUMNS <= columns:
            roles["candles"].append(path)
        if EXECUTION_COLUMNS <= columns:
            roles["executions"].append(path)
        if PUBLIC_TRADE_COLUMNS <= columns:
            roles["public"].append(path)
        if QUOTE_COLUMNS <= canonical and canonical & {"timestamp", "timestamp_ms"}:
            roles["quotes"].append(path)

    for role, found in roles.items():
        if len(found) > 1:
            raise ValueError(f"Several files look like {role} data ({', '.join(p.name for p in found)}); "
                             f"select one per data type.")
    if roles["executions"] and roles["public"]:
        raise ValueError("Select one option execution/trade CSV at a time")
    trade_path = (roles["executions"] or roles["public"] or [None])[0]
    candle_path = (roles["candles"] or [None])[0]
    if candle_path is not None and candle_path == trade_path:
        raise ValueError("Underlying candles and option trades must be separate CSV files")
    quote_path = (roles["quotes"] or [None])[0]

    if candle_path is None and trade_path is None and quote_path is None:
        if selected_order_history:
            reason = ("account_orders_history.csv holds order lifecycle records, not market trades. Select "
                      "account_executions_history.csv, a candle CSV, or a timestamped option quote CSV.")
        else:
            reason = ("Choose an executions CSV, a Bybit public-trades CSV (symbol/price/size/side/time), "
                      "a candle CSV, or a timestamped option quote CSV.")
        raise ValueError(f"No supported market-event CSV was selected. {reason} Selected: "
                         + "; ".join(descriptions))
    return candle_path, roles["executions"][0] if roles["executions"] else None, \
        roles["public"][0] if roles["public"] else None, quote_path


# --------------------------------------------------------------------------
# Normalizers: CSV rows -> native Nautilus objects
# --------------------------------------------------------------------------
def normalize_underlying_bars(frame: pd.DataFrame, instrument, snap: bool = False,
                              csv_time: str = "open") -> tuple[list[Bar], list[dict[str, Any]]]:
    """Build one-minute ``Bar``s. Nautilus convention: ts_event = bar CLOSE time.

    ``csv_time='open'`` means the CSV timestamp is the bar's open time (Bybit
    kline start); the close time is then open + 60 s.
    """
    bars: list[Bar] = []
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    volume_quantum = Decimal(1).scaleb(-instrument.size_precision)
    volume_rounded = 0
    for idx, row in enumerate(frame.to_dict("records")):
        try:
            ts_csv = to_ns(row["timestamp_ms"], "timestamp_ms")
            ts_close = ts_csv + BAR_INTERVAL_NS if csv_time == "open" else ts_csv
            if ts_close in seen:
                raise ValueError("duplicate candle timestamp")
            seen.add(ts_close)
            o, h, l, c = (Decimal(text_decimal(row[k], k)) for k in ("open", "high", "low", "close"))
            volume = Decimal(text_decimal(row["volume_btc"], "volume_btc"))
            if min(o, h, l, c) <= 0 or volume < 0 or h < max(o, l, c) or l > min(o, h, c):
                raise ValueError("invalid OHLCV values")
            prices = [conform_price(instrument, v, name, snap)
                      for v, name in ((o, "open"), (h, "high"), (l, "low"), (c, "close"))]
            vol_q = volume.quantize(volume_quantum, rounding=ROUND_HALF_UP)
            volume_rounded += vol_q != volume
            volume_qty = Quantity.from_str(format(vol_q, "f"))
        except OffGridError as exc:
            raise OffGridError(f"candle row {idx}: {exc} (use --round-off-grid to snap candles)") from exc
        except ValueError as exc:
            raise ValueError(f"candle row {idx}: {exc}") from exc
        bars.append(Bar(BAR_TYPE, *prices, volume_qty, ts_close, ts_close))
        rows.append({
            "instrument_id": str(UNDERLYING_ID), "ts_event_ns": ts_close,
            "bar_open_ns": ts_close - BAR_INTERVAL_NS,
            "open": str(prices[0]), "high": str(prices[1]), "low": str(prices[2]),
            "close": str(prices[3]), "volume": str(volume_qty),
        })
    if volume_rounded:
        LOG.warning("%d candle volume(s) were rounded to the instrument size precision", volume_rounded)
    bars.sort(key=lambda b: b.ts_event)
    rows.sort(key=lambda r: r["ts_event_ns"])
    return bars, rows


def normalize_option_executions(frame: pd.DataFrame, instruments: dict[str, Any]
                                ) -> tuple[list[TradeTick], list[dict[str, Any]]]:
    """Build ``TradeTick``s. Off-grid prices/sizes are an error (never rounded)."""
    records: list[tuple[int, str, TradeTick, dict[str, Any]]] = []
    seen_exec_ids: set[str] = set()
    seen_trade_ids: set[str] = set()
    for idx, row in enumerate(frame.to_dict("records")):
        try:
            symbol = str(row["symbol"]).strip().upper()
            iid = option_instrument_id(symbol)
            instrument = instruments[symbol]
            exec_id = str(row["exec_id"]).strip()
            if not exec_id or exec_id in seen_exec_ids:
                raise ValueError("missing or duplicate execution ID")
            seen_exec_ids.add(exec_id)
            trade_id = make_trade_id(exec_id)
            if trade_id in seen_trade_ids:
                raise ValueError("trade ID collision after shortening")
            seen_trade_ids.add(trade_id)
            ts = to_ns(row["timestamp_ms"], "timestamp_ms")
            price_d = Decimal(text_decimal(row["exec_price_usdt"], "execution price"))
            size_d = Decimal(text_decimal(row["exec_qty_btc"], "execution quantity"))
            if price_d <= 0 or size_d <= 0:
                raise ValueError("price and quantity must be positive")
            side = str(row["side"]).strip().upper()
            if side not in {"BUY", "SELL"}:
                raise ValueError(f"unsupported order side {side!r}")
            maker = parse_bool(row["is_maker"], "is_maker")
            price = conform_price(instrument, price_d, f"{symbol} price")
            size = conform_qty(instrument, size_d, f"{symbol} size")
            fee = text_decimal(row["exec_fee_usdt"], "execution fee")
        except ValueError as exc:
            raise type(exc)(f"execution row {idx}: {exc}") from exc
        # A maker's fill was aggressed by the opposite side; a taker's by its own side.
        if maker:
            aggressor = AggressorSide.SELLER if side == "BUY" else AggressorSide.BUYER
        else:
            aggressor = AggressorSide.BUYER if side == "BUY" else AggressorSide.SELLER
        tick = TradeTick(iid, price, size, aggressor, TradeId(trade_id), ts, ts)
        csv_row = {
            "instrument_id": str(iid), "ts_event_ns": ts, "exec_id": exec_id, "trade_id": trade_id,
            "order_id": row.get("order_id", ""), "side": side, "is_maker": maker,
            "aggressor_side": str(aggressor), "price_usdt": str(price), "size_btc": str(size),
            "fee_usdt": fee, "mark_price_usdt": row.get("mark_price", ""),
            "mark_iv": row.get("mark_iv", ""),
        }
        records.append((ts, trade_id, tick, csv_row))
    records.sort(key=lambda r: (r[0], r[1]))
    return [r[2] for r in records], [r[3] for r in records]


def adapt_bybit_public_trades(frame: pd.DataFrame) -> pd.DataFrame:
    """Map Bybit public-trade columns to the execution schema (taker = aggressor, zero fee)."""
    rows = []
    for idx, row in enumerate(frame.to_dict("records")):
        symbol = str(row["symbol"]).strip().upper()
        option_instrument_id(symbol)
        side = str(row["side"]).strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported Bybit trade side at row {idx}: {side!r}")
        ts = str(row["time"]).strip()
        exec_id = str(row.get("execId") or row.get("exec_id") or f"{ts}-{idx}").strip()
        rows.append({
            "exec_id": exec_id, "order_id": "", "timestamp_ms": ts, "symbol": symbol, "side": side,
            "exec_qty_btc": text_decimal(row["size"], "size"),
            "exec_price_usdt": text_decimal(row["price"], "price"),
            "exec_fee_usdt": "0", "is_maker": "false",
            "mark_price": row.get("mP", ""), "trade_iv": row.get("iv", ""), "mark_iv": row.get("mIv", ""),
        })
    return pd.DataFrame(rows)


def normalize_bybit_option_ticker_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Add canonical column names for known vendor aliases (idempotent)."""
    out = frame.copy()
    for source, target in QUOTE_ALIASES.items():
        if source in out.columns and target not in out.columns:
            out[target] = out[source]
    if "timestamp_ms" not in out.columns:
        if "timestamp" not in out.columns:
            raise ValueError("Option ticker data needs timestamp_ms, timestamp, ts, time, or datetime")
        out["timestamp_ms"] = out["timestamp"]
    missing = (QUOTE_COLUMNS | {"timestamp_ms"}) - set(out.columns)
    if missing:
        raise ValueError("Option quote CSV is missing columns: " + ", ".join(sorted(missing)))
    return out


def normalize_option_quotes(frame: pd.DataFrame, instruments: dict[str, Any], snap: bool = False
                            ) -> tuple[list[QuoteTick], list[dict[str, Any]], list[OptionGreeks], list[dict[str, Any]]]:
    """Build ``QuoteTick`` (two-sided, on-grid) and ``OptionGreeks`` (independent of quote validity)."""
    frame = normalize_bybit_option_ticker_columns(frame)
    quotes: list[QuoteTick] = []
    quote_rows: list[dict[str, Any]] = []
    greeks: list[OptionGreeks] = []
    greeks_rows: list[dict[str, Any]] = []
    seen_quotes: set[tuple] = set()
    seen_greeks: set[tuple] = set()
    one_sided = crossed = off_grid = duplicates = rho_missing = 0

    for idx, row in enumerate(frame.to_dict("records")):
        try:
            symbol = str(row["symbol"]).strip().upper()
            iid = option_instrument_id(symbol)
            instrument = instruments[symbol]
            ts = to_ns(row["timestamp_ms"], "timestamp")

            # Greeks: kept even when the quote itself is unusable.
            if all(str(row.get(n, "")).strip() for n in GREEK_NAMES):
                delta, gamma, vega, theta = (float(text_decimal(row[n], n)) for n in GREEK_NAMES)
                rho = optional_float(row, "rho")
                if rho is None:
                    rho, rho_missing = 0.0, rho_missing + 1
                extras = tuple(optional_float(row, n) for n in
                               ("mark_iv", "bid_iv", "ask_iv", "underlying_price", "open_interest"))
                key = (symbol, ts, delta, gamma, vega, theta, rho, extras)
                if key in seen_greeks:
                    duplicates += 1
                else:
                    seen_greeks.add(key)
                    greeks.append(OptionGreeks(iid, delta, gamma, vega, theta, rho, *extras, ts, ts))
                    greeks_rows.append({
                        "instrument_id": str(iid), "symbol": symbol, "ts_event_ns": ts,
                        "delta": delta, "gamma": gamma, "vega": vega, "theta": theta, "rho": rho,
                        "mark_iv": extras[0], "bid_iv": extras[1], "ask_iv": extras[2],
                        "underlying_price": extras[3], "open_interest": extras[4],
                    })

            bid = optional_decimal(row["bid_price"], "bid_price")
            ask = optional_decimal(row["ask_price"], "ask_price")
            bid_size = optional_decimal(row["bid_size"], "bid_size")
            ask_size = optional_decimal(row["ask_size"], "ask_size")
            if None in (bid, ask, bid_size, ask_size) or min(bid, ask, bid_size, ask_size) <= 0:
                one_sided += 1
                continue
            if bid > ask:
                crossed += 1
                continue
            try:
                bid_p = conform_price(instrument, bid, f"{symbol} bid", snap)
                ask_p = conform_price(instrument, ask, f"{symbol} ask", snap)
                bid_q = conform_qty(instrument, bid_size, f"{symbol} bid size", snap)
                ask_q = conform_qty(instrument, ask_size, f"{symbol} ask size", snap)
            except OffGridError:
                if is_fallback(instrument):
                    raise  # data contradicts the fallback spec: the spec is probably wrong
                off_grid += 1
                continue
            if bid_p.as_decimal() <= 0 or bid_q.as_decimal() <= 0 or ask_q.as_decimal() <= 0:
                one_sided += 1
                continue
            key = (symbol, ts, str(bid_p), str(ask_p), str(bid_q), str(ask_q))
            if key in seen_quotes:
                duplicates += 1
                continue
            seen_quotes.add(key)
        except ValueError as exc:
            raise type(exc)(f"quote row {idx}: {exc}") from exc

        quotes.append(QuoteTick(iid, bid_p, ask_p, bid_q, ask_q, ts, ts))
        quote_rows.append({
            "instrument_id": str(iid), "symbol": symbol, "ts_event_ns": ts,
            "bid_price": str(bid_p), "bid_size": str(bid_q),
            "ask_price": str(ask_p), "ask_size": str(ask_q),
        })

    if one_sided:
        LOG.warning("Skipped %d one-sided/zero quote row(s); a backtest will keep the previous quote there", one_sided)
    if crossed:
        LOG.warning("Skipped %d crossed quote row(s)", crossed)
    if off_grid:
        LOG.warning("Skipped %d quote row(s) off the instrument tick/step grid (use --round-off-grid to snap)", off_grid)
    if duplicates:
        LOG.warning("Dropped %d duplicate quote/greeks row(s)", duplicates)
    if rho_missing:
        LOG.warning("rho was absent on %d greeks row(s) and set to 0.0 (not real data)", rho_missing)
    if not quotes and not greeks:
        raise ValueError("No valid option quote or greeks rows remain after validation")
    quotes.sort(key=lambda q: (q.ts_event, str(q.instrument_id)))
    greeks.sort(key=lambda g: (g.ts_event, str(g.instrument_id)))
    quote_rows.sort(key=lambda r: (r["ts_event_ns"], r["symbol"]))
    greeks_rows.sort(key=lambda r: (r["ts_event_ns"], r["symbol"]))
    return quotes, quote_rows, greeks, greeks_rows


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------
def write_catalog(catalog_dir: Path, instruments: list[Any], bars: list[Bar], ticks: list[TradeTick],
                  quotes: list[QuoteTick], greeks: list[OptionGreeks]) -> None:
    """Write to a temp folder, then rename. A failure never leaves a partial catalog."""
    catalog_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp = catalog_dir.parent / f".{catalog_dir.name}.tmp-{os.getpid()}"
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir()
    try:
        catalog = ParquetDataCatalog(str(tmp))
        for instrument in instruments:       # definitions first
            catalog.write_data([instrument])
        for batch in (bars, ticks, quotes, greeks):
            if batch:
                catalog.write_data(batch)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    if catalog_dir.exists():
        shutil.rmtree(catalog_dir)           # only reached when --overwrite was given
    tmp.rename(catalog_dir)


def write_csv_views(output_dir: Path, **tables: list[dict[str, Any]]) -> None:
    """Write only the normalized CSV views for the current conversion.

    Remove stale known views first so a later conversion that omits, for
    example, quote data cannot leave an old quote file that looks current.
    Raw input CSVs are never touched by this function.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    names = {"bars": "btc_underlying_bars.csv", "ticks": "btc_option_execution_ticks.csv",
             "quotes": "btc_option_quotes.csv", "greeks": "btc_option_greeks.csv"}
    for filename in names.values():
        (output_dir / filename).unlink(missing_ok=True)
    for key, rows in tables.items():
        if rows:
            pd.DataFrame(rows).to_csv(output_dir / names[key], index=False)


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Convert local Bybit BTC option CSVs into a NautilusTrader catalog and normalized CSVs")
    parser.add_argument("files", nargs="*", help="CSV paths; omit to open a file picker (needs tkinter)")
    parser.add_argument("--catalog", default="bybit_nautilus_catalog", help="Nautilus Parquet catalog folder")
    parser.add_argument("--csv-output", default="bybit_normalized", help="Folder for normalized output CSVs")
    parser.add_argument("--overwrite", action="store_true", help="Replace an existing non-empty catalog folder")
    parser.add_argument("--specs", nargs="*", default=[],
                        help="CSV(s) with symbol,tickSize,qtyStep[,launchTime,deliveryTime] for exact specs")
    parser.add_argument("--no-fallback", action="store_true",
                        help="Never use current rules for expired options; fail instead")
    parser.add_argument("--round-off-grid", action="store_true",
                        help="Snap candle/quote prices and quote sizes to the tick/step grid (trades stay strict)")
    parser.add_argument("--naive-tz", default="UTC",
                        help="Timezone for timestamp strings without an offset, e.g. Asia/Kolkata (default UTC)")
    parser.add_argument("--csv-bar-time", choices=("open", "close"), default="open",
                        help="Whether candle CSV timestamps are bar open or close time (default open)")
    parser.add_argument("--maker-fee", type=Decimal, default=None, help="Maker fee rate for instruments, e.g. 0.0002")
    parser.add_argument("--taker-fee", type=Decimal, default=None, help="Taker fee rate for instruments, e.g. 0.0002")
    return parser


def run(args: argparse.Namespace, selected: list[Path]) -> int:
    global NAIVE_TZ
    NAIVE_TZ = args.naive_tz
    pd.Timestamp("2024-01-01").tz_localize(NAIVE_TZ)  # fail fast on a bad timezone name

    catalog_dir = Path(args.catalog).resolve()
    output_dir = Path(args.csv_output).resolve()
    if catalog_dir.exists() and any(catalog_dir.iterdir()):
        if not args.overwrite:
            raise ValueError(f"Catalog folder {catalog_dir} is not empty; use --overwrite or another --catalog")
        if catalog_dir in {Path.cwd(), Path.home(), Path(catalog_dir.anchor)}:
            raise ValueError(f"Refusing to overwrite {catalog_dir}; choose a dedicated catalog folder")

    candle_path, execution_path, public_path, quote_path = identify_input_csvs(selected)

    # ---- Phase 1: read raw data (nothing is written yet) ----
    bars_df = executions_df = quotes_df = None
    if candle_path is not None:
        bars_df = read_csv(candle_path, CANDLE_COLUMNS, {"underlying_candle"})
    if execution_path is not None:
        executions_df = read_csv(execution_path, EXECUTION_COLUMNS, {"option_execution"})
        LOG.warning("Account executions are YOUR fills, not the full market tape; a backtest on them "
                    "sees a sparse, biased market. Use public trades for market-wide ticks.")
    elif public_path is not None:
        executions_df = adapt_bybit_public_trades(
            read_csv(public_path, PUBLIC_TRADE_COLUMNS))
    if quote_path is not None:
        quotes_df = normalize_bybit_option_ticker_columns(
            read_csv(quote_path, set(), {"option_quote", "option_ticker"}))

    symbols: set[str] = set()
    if executions_df is not None:
        symbols.update(executions_df["symbol"].astype(str).str.strip().str.upper())
    if quotes_df is not None:
        symbols.update(quotes_df["symbol"].astype(str).str.strip().str.upper())
    for symbol in symbols:
        option_instrument_id(symbol)  # validates BTC USDT option scope up front

    # ---- Phase 2: resolve instruments, then normalize against them ----
    session = make_session()
    archived = load_archived_option_specs(selected + [Path(p).expanduser().resolve() for p in args.specs])
    perp, option_instruments, fallback_count = resolve_instruments(
        session, sorted(symbols), archived, bars_df is not None,
        not args.no_fallback, args.maker_fee, args.taker_fee)

    bars, bar_rows = ([], [])
    ticks, tick_rows = ([], [])
    quotes, quote_rows, greeks, greeks_rows = ([], [], [], [])
    if bars_df is not None:
        bars, bar_rows = normalize_underlying_bars(bars_df, perp, args.round_off_grid, args.csv_bar_time)
    if executions_df is not None:
        ticks, tick_rows = normalize_option_executions(executions_df, option_instruments)
    if quotes_df is not None:
        quotes, quote_rows, greeks, greeks_rows = normalize_option_quotes(
            quotes_df, option_instruments, args.round_off_grid)

    # ---- Phase 3: write (temp folder + rename) ----
    instruments = ([perp] if perp is not None else []) + list(option_instruments.values())
    write_catalog(catalog_dir, instruments, bars, ticks, quotes, greeks)
    write_csv_views(output_dir, bars=bar_rows, ticks=tick_rows, quotes=quote_rows, greeks=greeks_rows)

    LOG.info("Wrote %d BTC one-minute bars", len(bars))
    LOG.info("Wrote %d BTC option TradeTick(s) across %d instruments", len(ticks),
             len({str(t.instrument_id) for t in ticks}))
    LOG.info("Wrote %d BTC option QuoteTick(s)", len(quotes))
    LOG.info("Wrote %d BTC option Greeks record(s)", len(greeks))
    LOG.info("Registered %d instrument definition(s)", len(instruments))
    if fallback_count:
        LOG.warning("%d expired option definition(s) used a tagged current-rule fallback "
                    "(see instrument.info['historicalSpecFallback']).", fallback_count)
    if not quotes:
        LOG.warning("No option quotes supplied; a realistic quote-driven option backtest is not ready.")
    LOG.warning("No fees (unless --maker-fee/--taker-fee), margin, or settlement events are modeled.")
    LOG.info("Catalog: %s", catalog_dir)
    LOG.info("Normalized CSVs: %s", output_dir)
    return 0


def main() -> int:
    args = build_parser().parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    gui_used = not args.files
    selected = select_csv_files(args.files)
    if not selected:
        LOG.info("No CSVs selected; exiting without creating a catalog")
        return 0
    try:
        return run(args, selected)
    except (ValueError, FileNotFoundError, LookupError, BybitApiError, OSError,
            requests.RequestException) as exc:
        LOG.error("%s: %s", type(exc).__name__, exc)
        if gui_used:
            show_error("Conversion failed", str(exc))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
