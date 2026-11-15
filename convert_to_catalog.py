"""Convert Bybit BTC market CSV data into a NautilusTrader Parquet catalog.

The converter supports these Nautilus market-event types:

* BTCUSDT one-minute OHLCV candles, converted into native Nautilus ``Bar`` objects.
* BTC option executions, converted into native Nautilus ``TradeTick`` objects.
* Timestamped BTC option best-bid/ask snapshots, converted into ``QuoteTick``s.
* Greeks present on those timestamped snapshots, converted into ``OptionGreeks``.

    Inputs may be separate CSV files or a tagged master CSV. In a master CSV,
    ``record_type`` routes ``underlying_candle``, ``option_execution``, and
    ``option_quote``/``option_ticker`` rows into their native data models.

Bybit public market endpoints provide instrument specifications. The script
does not use account API credentials and never submits or changes orders. It
derives Nautilus instrument IDs from the Bybit option symbols, validates the
records, writes instrument definitions first, then writes market data and
optional normalized CSV copies.

Important limitation: an expired option may no longer have an exact Bybit
instrument specification available. In that case, the converter only uses a
clearly tagged current-rule fallback when all currently listed BTC options
share the same price tick and quantity step, and all supplied fills pass those
increments. This does not reconstruct exact archived contract metadata.
"""

from __future__ import annotations

import argparse
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
import tkinter as tk
from typing import Any
from tkinter import filedialog
from tkinter import messagebox

import pandas as pd
import requests
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
UNDERLYING_ID = InstrumentId.from_str("BTCUSDT-LINEAR.BYBIT")
BAR_TYPE = BarType.from_str("BTCUSDT-LINEAR.BYBIT-1-MINUTE-LAST-EXTERNAL")
BYBIT_API = "https://api.bybit.com"


def text_decimal(value: Any, field: str) -> str:
    """Return a finite decimal value as plain text, without float conversion.

    Keeping prices and quantities as decimal strings avoids introducing binary
    floating-point rounding before Nautilus constructs ``Price`` and
    ``Quantity`` objects.
    """
    if value is None or pd.isna(value) or not str(value).strip():
        raise ValueError(f"{field} is empty")
    try:
        number = Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} is not numeric: {value!r}") from exc
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    return format(number, "f")


def option_instrument_id(symbol: str) -> InstrumentId:
    """Build Nautilus' venue-qualified ID from a Bybit BTC option symbol.

    Example: ``BTC-11NOV26-74500-C-USDT`` becomes
    ``BTC-11NOV26-74500-C-USDT-OPTION.BYBIT``. The regular expression above
    rejects symbols outside this converter's BTC/USDT option scope.
    """
    symbol = symbol.strip().upper()
    if not OPTION_RE.fullmatch(symbol):
        raise ValueError(f"not a supported BTC USDT option symbol: {symbol!r}")
    return InstrumentId.from_str(f"{symbol}-OPTION.BYBIT")


def fetch_instrument_spec(session: requests.Session, category: str, symbol: str) -> dict[str, Any]:
    """Fetch an instrument's public Bybit tick, size, and contract metadata.

    Try the detailed instruments-info endpoint first. If the contract is no
    longer listed there, try instrument-summary, which can retain historical
    option increments. Raise an error if neither endpoint returns this exact
    symbol; the caller decides whether a tagged fallback is acceptable.
    """
    errors = []
    try:
        response = session.get(
            f"{BYBIT_API}/v5/market/instruments-info",
            params={"category": category, "symbol": symbol}, timeout=20,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("retCode") == 0:
            rows = body.get("result", {}).get("list", [])
            found = next((row for row in rows if str(row.get("symbol", "")).upper() == symbol.upper()), None)
            if found:
                return found
        else:
            errors.append(str(body.get("retMsg", "instrument-info returned an error")))
    except requests.RequestException as exc:
        errors.append(str(exc))

    # instruments-info only returns online instruments. instrument-summary
    # also exposes the exact historical symbol increments where available.
    try:
        response = session.get(
            f"{BYBIT_API}/v5/market/instrument-summary",
            params={"category": category, "symbol": symbol}, timeout=20,
        )
        response.raise_for_status()
        body = response.json()
        if body.get("retCode") == 0:
            rows = body.get("result", {}).get("list", [])
            found = next((row for row in rows if str(row.get("symbol", "")).upper() == symbol.upper()), None)
            if found:
                if category == "option":
                    match = OPTION_RE.fullmatch(symbol.upper())
                    if match is None:
                        raise ValueError(f"unsupported BTC option contract symbol {symbol!r}")
                    quote = match.group("quote")
                else:
                    quote = "USDT"
                return {
                    "symbol": symbol,
                    "category": category,
                    "baseCoin": found.get("baseCoin", "BTC"),
                    "quoteCoin": quote,
                    "settleCoin": found.get("settleCoin", quote),
                    "launchTime": "0",  # This endpoint does not publish launch time.
                    "deliveryTime": found.get("deliveryTime", "0"),
                    "priceFilter": {"tickSize": found.get("tickSize")},
                    "lotSizeFilter": {"qtyStep": found.get("qtyStep")},
                    "metadataSource": "Bybit instrument-summary",
                }
        else:
            errors.append(str(body.get("retMsg", "instrument-summary returned an error")))
    except requests.RequestException as exc:
        errors.append(str(exc))

    extra = "; ".join(errors[-2:])
    raise ValueError(
        f"Bybit returned no exact instrument specification for {category}:{symbol}. "
        f"The instrument-summary fallback also had no record; archived specs are required. {extra}"
    )


def fetch_uniform_btc_option_rules(session: requests.Session) -> dict[str, Any]:
    """Return current BTC option increments only when the whole chain agrees.

    The summary endpoint is paginated, so keep following its cursor until all
    listed BTC option contracts have been checked. A single shared tick size
    and quantity step are required before these rules can be used as a
    fallback for an expired contract; mixed rules make the fallback unsafe.
    """
    cursor = ""
    rows: list[dict[str, Any]] = []
    response_time = 0
    while True:
        params = {"category": "option", "baseCoin": "BTC", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        response = session.get(f"{BYBIT_API}/v5/market/instrument-summary", params=params, timeout=20)
        response.raise_for_status()
        body = response.json()
        if body.get("retCode") != 0:
            raise ValueError(f"Bybit BTC option summary failed: {body.get('retMsg')}")
        result = body.get("result", {})
        rows.extend(result.get("list", []))
        response_time = int(body.get("time", 0))
        cursor = result.get("nextPageCursor", "")
        if not cursor:
            break
    ticks = {str(row.get("tickSize", "")) for row in rows if row.get("tickSize")}
    steps = {str(row.get("qtyStep", "")) for row in rows if row.get("qtyStep")}
    if not rows or len(ticks) != 1 or len(steps) != 1:
        raise ValueError(
            "Bybit no longer exposes expired option specs, and active BTC option "
            "contracts do not share one tickSize/qtyStep rule; archived specs are required."
        )
    return {"tickSize": ticks.pop(), "qtyStep": steps.pop(), "asOfMs": response_time, "symbolsChecked": len(rows)}


def expired_option_spec_from_common_rule(symbol: str, rules: dict[str, Any]) -> dict[str, Any]:
    """Build a marked fallback definition for an expired option.

    Bybit's option expiry is at 08:00 UTC on the date encoded in the symbol.
    Historical launch time is unavailable here, so it is set to zero and the
    ``info`` metadata explicitly records the fallback source and its limits.
    """
    match = OPTION_RE.fullmatch(symbol.upper())
    if match is None:
        raise ValueError(f"unsupported BTC option contract symbol {symbol!r}")
    expiry = datetime.strptime(match.group("expiry").upper(), "%d%b%y").replace(tzinfo=timezone.utc)
    delivery_ms = int((expiry + timedelta(hours=8)).timestamp() * 1000)
    return {
        "symbol": symbol,
        "category": "option",
        "baseCoin": match.group("base"),
        "quoteCoin": match.group("quote"),
        "settleCoin": match.group("quote"),
        "launchTime": "0",
        "deliveryTime": str(delivery_ms),
        "priceFilter": {"tickSize": rules["tickSize"]},
        "lotSizeFilter": {"qtyStep": rules["qtyStep"]},
        "metadataSource": "Bybit current uniform BTC option instrument-summary rule",
        "currentRulesAsOfMs": rules["asOfMs"],
        "currentRuleSymbolsChecked": rules["symbolsChecked"],
        "historicalLaunchTimeUnavailable": True,
        "historicalSpecFallback": True,
    }


def _required_increment(value: Any, field: str, *, quantity: bool = False):
    """Parse and validate a positive price tick or quantity step."""
    text = text_decimal(value, field)
    increment = Quantity.from_str(text) if quantity else Price.from_str(text)
    if Decimal(text) <= 0:
        raise ValueError(f"{field} must be positive")
    return increment


def build_native_instrument(category: str, symbol: str, spec: dict[str, Any]):
    """Create the native Nautilus instrument definition for one Bybit symbol.

    Linear BTCUSDT candles use ``CryptoPerpetual``. Each option execution
    symbol gets a ``CryptoOption`` with its call/put kind, strike, expiry,
    currencies, increments, and any metadata provenance in ``info``.
    """
    price_filter = spec.get("priceFilter") or {}
    lot_filter = spec.get("lotSizeFilter") or {}
    price_increment = _required_increment(price_filter.get("tickSize"), "tickSize")
    size_increment = _required_increment(lot_filter.get("qtyStep"), "qtyStep", quantity=True)
    ts_now = time.time_ns()
    raw_symbol = Symbol(symbol)
    base = Currency.from_str(str(spec.get("baseCoin", "BTC")))
    quote = Currency.from_str(str(spec.get("quoteCoin", "USDT")))
    settlement = Currency.from_str(str(spec.get("settleCoin") or spec.get("quoteCoin", "USDT")))
    min_q = Quantity.from_str(str(lot_filter["minOrderQty"])) if lot_filter.get("minOrderQty") else None
    max_value = lot_filter.get("maxOrderQty") or lot_filter.get("maxMktOrderQty")
    max_q = Quantity.from_str(str(max_value)) if max_value else None

    if category == "linear":
        return CryptoPerpetual(
            UNDERLYING_ID, raw_symbol, base, quote, settlement, False,
            price_increment.precision, size_increment.precision,
            price_increment, size_increment,
            int(spec.get("launchTime") or ts_now // 1_000_000) * 1_000_000,
            ts_now, min_quantity=min_q, max_quantity=max_q, info=spec,
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
        price_increment, size_increment, ts_now, ts_now,
        max_quantity=max_q, min_quantity=min_q, info=spec,
    )


def to_ns(value: Any, field: str) -> int:
    """Convert a Bybit/vendor timestamp to Nautilus nanoseconds.

    Accepts integer epoch timestamps in seconds, milliseconds, microseconds,
    or nanoseconds, plus ISO-like UTC timestamps. Unit detection is based on
    digit magnitude; ambiguous/invalid values fail with a useful row error.
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
                stamp = stamp.tz_localize("UTC")
            return int(stamp.tz_convert("UTC").value)
        except Exception as exc:
            raise ValueError(f"{field} is not a supported timestamp: {value!r}") from exc
    if not number.is_finite() or number <= 0 or number != number.to_integral_value():
        raise ValueError(f"{field} must be a positive integer timestamp")
    stamp = int(number)
    if stamp < 100_000_000_000:       # seconds
        return stamp * 1_000_000_000
    if stamp < 100_000_000_000_000:   # milliseconds
        return stamp * 1_000_000
    if stamp < 100_000_000_000_000_000:  # microseconds
        return stamp * 1_000
    return stamp  # nanoseconds


def normalize_bybit_option_ticker_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Map Bybit ticker fields and common vendor names to converter columns."""
    aliases = {
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
    }
    out = frame.copy()
    for source, target in aliases.items():
        if source in out.columns and target not in out.columns:
            out[target] = out[source]
    if "timestamp_ms" not in out.columns:
        if "timestamp" not in out.columns:
            raise ValueError("Option ticker data needs timestamp_ms, timestamp, ts, time, or datetime")
        out["timestamp_ms"] = out["timestamp"]
    required = {"symbol", "timestamp_ms", "bid_price", "bid_size", "ask_price", "ask_size"}
    missing = required - set(out.columns)
    if missing:
        raise ValueError("Option quote CSV is missing columns: " + ", ".join(sorted(missing)))
    return out


def _matched_precision(left: str, right: str) -> tuple[str, str]:
    """Format quote sides with equal decimal precision for Nautilus QuoteTick."""
    precision = max(
        max(0, -Decimal(left).normalize().as_tuple().exponent),
        max(0, -Decimal(right).normalize().as_tuple().exponent),
    )
    quantum = Decimal(1).scaleb(-precision)
    return (format(Decimal(left).quantize(quantum), "f"),
            format(Decimal(right).quantize(quantum), "f"))


def normalize_option_quotes(frame: pd.DataFrame) -> tuple[list[QuoteTick], list[dict[str, Any]], list[OptionGreeks], list[dict[str, Any]]]:
    """Validate option BBO snapshots and build QuoteTick/optional OptionGreeks."""
    frame = normalize_bybit_option_ticker_columns(frame)
    quotes: list[QuoteTick] = []
    quote_rows: list[dict[str, Any]] = []
    greeks: list[OptionGreeks] = []
    greeks_rows: list[dict[str, Any]] = []
    rejected_rows = 0
    for idx, row in enumerate(frame.to_dict("records")):
        symbol = str(row["symbol"]).strip().upper()
        iid = option_instrument_id(symbol)
        ts = to_ns(row["timestamp_ms"], f"option quote row {idx} timestamp")
        bid = text_decimal(row["bid_price"], "bid_price")
        ask = text_decimal(row["ask_price"], "ask_price")
        bid_size = text_decimal(row["bid_size"], "bid_size")
        ask_size = text_decimal(row["ask_size"], "ask_size")
        if min(Decimal(bid), Decimal(ask), Decimal(bid_size), Decimal(ask_size)) <= 0:
            # Illiquid options can have an empty side. Nautilus QuoteTick
            # represents a two-sided BBO, so skip these rather than inventing
            # a price for the missing side.
            rejected_rows += 1
            continue
        if Decimal(bid) > Decimal(ask):
            rejected_rows += 1
            continue
        bid, ask = _matched_precision(bid, ask)
        bid_size, ask_size = _matched_precision(bid_size, ask_size)
        quote = QuoteTick(iid, Price.from_str(bid), Price.from_str(ask),
                          Quantity.from_str(bid_size), Quantity.from_str(ask_size), ts, ts)
        quotes.append(quote)
        quote_rows.append({"instrument_id": str(iid), "symbol": symbol, "ts_event_ns": ts,
                           "bid_price": bid, "bid_size": bid_size, "ask_price": ask, "ask_size": ask_size})

        # Greeks are optional. Emit an OptionGreeks only when the complete
        # Bybit delta/gamma/vega/theta set is present on the same timestamp.
        greek_names = ("delta", "gamma", "vega", "theta")
        if all(str(row.get(name, "")).strip() for name in greek_names):
            vals = [float(text_decimal(row[name], name)) for name in greek_names]
            optional_float = lambda name: (float(text_decimal(row[name], name))
                                           if str(row.get(name, "")).strip() else None)
            greek = OptionGreeks(
                iid, *vals, 0.0,
                optional_float("mark_iv"), optional_float("bid_iv"), optional_float("ask_iv"),
                optional_float("underlying_price"), optional_float("openInterest"), ts, ts,
            )
            greeks.append(greek)
            greeks_rows.append({"instrument_id": str(iid), "symbol": symbol, "ts_event_ns": ts,
                                **{name: row[name] for name in greek_names}, "rho": "0",
                                "mark_iv": row.get("mark_iv", ""), "bid_iv": row.get("bid_iv", ""),
                                "ask_iv": row.get("ask_iv", ""),
                                "underlying_price": row.get("underlying_price", "")})
    if rejected_rows:
        LOG.warning("Skipped %d option quote row(s) with zero-size/price sides or crossed BBO", rejected_rows)
    if not quotes:
        raise ValueError("No valid two-sided option BBO rows remain after quote validation")
    quotes.sort(key=lambda item: (item.ts_event, str(item.instrument_id)))
    greeks.sort(key=lambda item: (item.ts_event, str(item.instrument_id)))
    quote_rows.sort(key=lambda item: (item["ts_event_ns"], item["symbol"]))
    greeks_rows.sort(key=lambda item: (item["ts_event_ns"], item["symbol"]))
    return quotes, quote_rows, greeks, greeks_rows


def read_csv(path: Path, required: set[str]) -> pd.DataFrame:
    """Read a CSV as strings and fail early if required columns are absent.

    Reading fields as text preserves decimal precision and avoids pandas
    interpreting empty cells as NaN before type-specific validation.
    """
    if not path.is_file():
        raise FileNotFoundError(f"required CSV not found: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name} is missing columns: {', '.join(sorted(missing))}")
    if frame.empty:
        raise ValueError(f"{path.name} has no rows")
    return frame


def select_csv_files(files: list[str]) -> list[Path]:
    """Use command-line paths, or show a file picker when no paths were given."""
    if files:
        return [Path(name).expanduser().resolve() for name in files]
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


def identify_input_csvs(paths: list[Path]) -> tuple[Path | None, Path | None, Path | None, Path | None]:
    """Identify candle, option-trade, and option-quote inputs by their columns.

    A tagged master CSV is recognized by ``record_type`` and can be selected
    by itself. Its heterogeneous rows are separated later in ``main``. For
    original files, column signatures identify the underlying candle CSV,
    account execution CSV, or Bybit public-trade CSV. Order history, position
    snapshots, and PnL tables are not treated as market trades.
    """
    underlying_columns = {"timestamp_ms", "open", "high", "low", "close", "volume_btc"}
    execution_columns = {
        "exec_id", "order_id", "timestamp_ms", "symbol", "side",
        "exec_qty_btc", "exec_price_usdt", "exec_fee_usdt", "is_maker",
    }
    bybit_trade_columns = {"symbol", "price", "size", "side", "time"}
    quote_columns = {"symbol", "bid_price", "bid_size", "ask_price", "ask_size"}
    # A tagged master CSV keeps heterogeneous source rows together. The
    # converter routes only its market-event rows to native Nautilus models.
    master_candidates = []
    for path in paths:
        columns = set(pd.read_csv(path, dtype=str, nrows=0).columns)
        if "record_type" in columns:
            kinds = set(pd.read_csv(path, dtype=str, usecols=["record_type"])["record_type"])
            if kinds & {"underlying_candle", "option_execution", "option_quote", "option_ticker"}:
                master_candidates.append((path, kinds))
    if master_candidates:
        if len(master_candidates) != 1:
            raise ValueError("Select only one tagged master market-data CSV.")
        path, kinds = master_candidates[0]
        return (
            path if "underlying_candle" in kinds else None,
            path if "option_execution" in kinds else None,
            None,
            path if kinds & {"option_quote", "option_ticker"} else None,
        )

    underlying_path = execution_path = raw_trade_path = quote_path = None
    for path in paths:
        columns = set(pd.read_csv(path, dtype=str, nrows=0).columns)
        if underlying_columns <= columns:
            underlying_path = path
        if execution_columns <= columns:
            execution_path = path
        if bybit_trade_columns <= columns:
            raw_trade_path = path
        has_quote_timestamp = bool(columns & {
            "timestamp_ms", "timestamp", "timestamp_utc", "timestamp_ns", "timestamp_us",
            "ts", "ts_ms", "ts_ns", "time", "datetime", "exchange_timestamp", "ts_event",
        })
        if has_quote_timestamp and (
            quote_columns <= columns or {"symbol", "bidPrice", "bidSize", "askPrice", "askSize"} <= columns
        ):
            quote_path = path
    trade_paths = [p for p in (execution_path, raw_trade_path) if p is not None]
    if len(trade_paths) > 1:
        raise ValueError("Select one option execution/trade CSV at a time")
    if underlying_path is not None and underlying_path in trade_paths:
        raise ValueError("Underlying candles and option trades must be separate CSV files")
    if underlying_path is None and not trade_paths and quote_path is None:
        descriptions = []
        selected_order_history = False
        for path in paths:
            columns = list(pd.read_csv(path, dtype=str, nrows=0).columns)
            selected_order_history |= {"order_id", "order_status", "qty_btc"} <= set(columns)
            descriptions.append(f"{path.name}: {', '.join(columns[:12])}")
        if selected_order_history:
            reason = (
                "account_orders_history.csv contains order lifecycle records, not market trades. "
                "Select account_executions_history.csv to convert your fills into TradeTicks, "
                "market_btc_underlying_1m.csv to convert candles into Bars, or a timestamped "
                "option quote CSV to convert BBO data into QuoteTicks."
            )
        else:
            reason = (
                "Choose account_executions_history.csv, a Bybit public-trades CSV with "
                "symbol/price/size/side/time columns, or market_btc_underlying_1m.csv."
            )
        raise ValueError(
            f"No supported market-event CSV was selected. {reason} Selected files: "
            + "; ".join(descriptions)
        )
    return underlying_path, execution_path, raw_trade_path, quote_path


def normalize_underlying_bars(frame: pd.DataFrame) -> tuple[list[Bar], list[dict[str, Any]]]:
    """Validate OHLCV rows and create native Nautilus ``Bar`` objects.

    This also prepares a human-readable normalized CSV row for each bar.
    Candle timestamps are retained as event time; ``ts_init`` is set to the
    end of the one-minute interval. Duplicate timestamps and invalid OHLCV
    relationships are rejected.
    """
    bars: list[Bar] = []
    rows: list[dict[str, Any]] = []
    seen: set[int] = set()
    for idx, row in enumerate(frame.to_dict("records")):
        ts_event = to_ns(row["timestamp_ms"], f"underlying row {idx} timestamp_ms")
        if ts_event in seen:
            raise ValueError(f"duplicate BTC underlying candle timestamp at row {idx}")
        seen.add(ts_event)
        values = {key: text_decimal(row[key], key) for key in ("open", "high", "low", "close", "volume_btc")}
        o, h, l, c, volume = (Decimal(values[key]) for key in ("open", "high", "low", "close", "volume_btc"))
        if min(o, h, l, c) <= 0 or volume < 0 or h < max(o, l, c) or l > min(o, h, c):
            raise ValueError(f"invalid OHLCV values at underlying row {idx}")
        bar = Bar(
            BAR_TYPE, Price.from_str(values["open"]), Price.from_str(values["high"]),
            Price.from_str(values["low"]), Price.from_str(values["close"]),
            Quantity.from_str(values["volume_btc"]), ts_event,
            ts_event + 60_000_000_000,
        )
        bars.append(bar)
        rows.append({
            "instrument_id": str(UNDERLYING_ID), "ts_event_ns": ts_event,
            "ts_init_ns": ts_event + 60_000_000_000, **values,
        })
    bars.sort(key=lambda item: item.ts_event)
    rows.sort(key=lambda item: item["ts_event_ns"])
    return bars, rows


def parse_bool(value: Any, field: str) -> bool:
    """Parse common CSV boolean spellings without treating unknown text as false."""
    value = str(value).strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    raise ValueError(f"{field} is not a boolean: {value!r}")


def normalize_option_executions(frame: pd.DataFrame) -> tuple[list[TradeTick], list[dict[str, Any]]]:
    """Validate Bybit execution rows and construct native option ``TradeTick``s.

    Each execution must have a unique ID, supported BTC option symbol, valid
    timestamp, positive price/size, and BUY or SELL side. Bybit's ``is_maker``
    flag plus the account order side determines the aggressor side recorded in
    Nautilus. Fees and additional CSV fields remain available in the
    normalized inspection CSV, even though ``TradeTick`` itself has no fee
    field.
    """
    records: list[tuple[int, TradeTick, dict[str, Any]]] = []
    rows: list[dict[str, Any]] = []
    seen_exec_ids: set[str] = set()
    for idx, row in enumerate(frame.to_dict("records")):
        symbol = str(row["symbol"]).strip().upper()
        iid = option_instrument_id(symbol)
        exec_id = str(row["exec_id"]).strip()
        if not exec_id or exec_id in seen_exec_ids:
            raise ValueError(f"missing or duplicate execution ID at row {idx}")
        seen_exec_ids.add(exec_id)
        ts = to_ns(row["timestamp_ms"], f"execution row {idx} timestamp_ms")
        price = text_decimal(row["exec_price_usdt"], "execution price")
        size = text_decimal(row["exec_qty_btc"], "execution quantity")
        if Decimal(price) <= 0 or Decimal(size) <= 0:
            raise ValueError(f"execution row {idx} price and quantity must be positive")
        side = str(row["side"]).strip().upper()
        maker = parse_bool(row["is_maker"], "is_maker")
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported order side at execution row {idx}: {side!r}")
        # A maker's fill was aggressed by the opposite side; a taker's fill
        # was aggressed by the account's own order side.
        if maker:
            aggressor = AggressorSide.SELLER if side == "BUY" else AggressorSide.BUYER
        else:
            aggressor = AggressorSide.BUYER if side == "BUY" else AggressorSide.SELLER
        tick = TradeTick(iid, Price.from_str(price), Quantity.from_str(size),
                         aggressor, TradeId(exec_id[:36]), ts, ts)
        rows.append({
            "instrument_id": str(iid), "ts_event_ns": ts, "exec_id": exec_id,
            "order_id": row["order_id"], "side": side, "is_maker": maker,
            "aggressor_side": str(aggressor), "price_usdt": price,
            "size_btc": size, "fee_usdt": text_decimal(row["exec_fee_usdt"], "execution fee"),
            "mark_price_usdt": row.get("mark_price", ""),
            "mark_iv": row.get("mark_iv", ""),
        })
        records.append((ts, tick, rows[-1]))
    records.sort(key=lambda item: (item[0], str(item[1].trade_id)))
    return [item[1] for item in records], [item[2] for item in records]


def adapt_bybit_public_trades(frame: pd.DataFrame) -> pd.DataFrame:
    """Map Bybit V5 public trade columns to the internal execution schema.

    Public trades do not include an account execution fee or maker flag, so
    the adapter supplies a zero fee and marks the published taker side as the
    aggressor. The shared execution normalizer then handles these rows like
    account fills.
    """
    rows = []
    for idx, row in enumerate(frame.to_dict("records")):
        symbol = str(row["symbol"]).strip().upper()
        option_instrument_id(symbol)  # Keep this converter scoped to BTC options.
        side = str(row["side"]).strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unsupported Bybit trade side at row {idx}: {side!r}")
        ts_ms = str(row["time"]).strip()
        exec_id = str(row.get("execId") or row.get("exec_id") or f"{ts_ms}-{idx}").strip()
        rows.append({
            "exec_id": exec_id,
            "order_id": "",
            "timestamp_ms": ts_ms,
            "symbol": symbol,
            "side": side,
            "exec_qty_btc": text_decimal(row["size"], "size"),
            "exec_price_usdt": text_decimal(row["price"], "price"),
            "exec_fee_usdt": "0",
            "is_maker": "false",  # Bybit public trade side is the taker/aggressor side.
            "mark_price": row.get("mP", ""),
            "trade_iv": row.get("iv", ""),
            "mark_iv": row.get("mIv", ""),
        })
    return pd.DataFrame(rows)


def validate_option_fill_increments(frame: pd.DataFrame, symbol: str, spec: dict[str, Any]) -> None:
    """Ensure fills align to the option's tick size and quantity step."""
    tick = Decimal(text_decimal((spec.get("priceFilter") or {}).get("tickSize"), "tickSize"))
    step = Decimal(text_decimal((spec.get("lotSizeFilter") or {}).get("qtyStep"), "qtyStep"))
    subset = frame[frame["symbol"].str.strip().str.upper() == symbol.upper()]
    for row_idx, row in subset.iterrows():
        price = Decimal(text_decimal(row["exec_price_usdt"], "exec_price_usdt"))
        size = Decimal(text_decimal(row["exec_qty_btc"], "exec_qty_btc"))
        if price % tick != 0 or size % step != 0:
            raise ValueError(
                f"{symbol}: execution row {row_idx} conflicts with fetched tickSize={tick} "
                f"or qtyStep={step}; refusing to write a mismatched instrument definition"
            )


def load_archived_option_specs(paths: list[Path]) -> dict[str, dict[str, Any]]:
    """Load exact option increments from any selected CSV that provides them.

    Historical metadata is optional. A row is accepted only when it contains
    both a symbol and explicit price/quantity increments, so greeks/quote
    snapshots without instrument rules are not mistaken for definitions.
    """
    specs: dict[str, dict[str, Any]] = {}
    tick_names = ("tickSize", "tick_size", "ticksize", "price_tick_size")
    step_names = ("qtyStep", "qty_step", "qtystep", "quantity_step", "size_increment")
    for path in paths:
        try:
            columns = set(pd.read_csv(path, dtype=str, nrows=0).columns)
            if "symbol" not in columns:
                continue
            tick_col = next((name for name in tick_names if name in columns), None)
            step_col = next((name for name in step_names if name in columns), None)
            if tick_col is None or step_col is None:
                continue
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
            for row in frame.to_dict("records"):
                symbol = str(row.get("symbol", "")).strip().upper()
                if not symbol or not OPTION_RE.fullmatch(symbol):
                    continue
                tick = str(row.get(tick_col, "")).strip()
                step = str(row.get(step_col, "")).strip()
                if not tick or not step:
                    continue
                match = OPTION_RE.fullmatch(symbol)
                expiry = datetime.strptime(match.group("expiry").upper(), "%d%b%y").replace(tzinfo=timezone.utc)
                delivery_ms = int((expiry + timedelta(hours=8)).timestamp() * 1000)
                specs[symbol] = {
                    "symbol": symbol, "category": "option", "baseCoin": "BTC",
                    "quoteCoin": match.group("quote"), "settleCoin": match.group("quote"),
                    "launchTime": row.get("launchTime") or row.get("launch_time_ms") or "0",
                    "deliveryTime": row.get("deliveryTime") or row.get("delivery_time_ms") or str(delivery_ms),
                    "priceFilter": {"tickSize": tick}, "lotSizeFilter": {"qtyStep": step},
                    "metadataSource": f"archived CSV: {path.name}",
                }
        except (OSError, pd.errors.ParserError, UnicodeDecodeError):
            # Non-market CSVs and unrelated metadata should not block the run.
            continue
    return specs


def main() -> int:
    """Run the full CSV → native objects → Parquet catalog pipeline.

    High-level order:
      1. Select files and identify supported event data.
      2. Read and normalize candles and executions (filtering tagged master
         rows by ``record_type`` when appropriate).
      3. Fetch/validate instrument definitions before writing output.
      4. Write normalized CSVs and native Nautilus objects to the catalog.

    The output directories are created only after CSV parsing and instrument
    metadata checks succeed, which avoids leaving a seemingly complete but
    partially populated catalog when validation fails.
    """
    parser = argparse.ArgumentParser(
        description="Convert local Bybit BTC option CSVs into a NautilusTrader catalog and normalized CSVs",
    )
    parser.add_argument("files", nargs="*", help="CSV paths; omit to open a multi-select file picker")
    parser.add_argument("--catalog", default="bybit_btc_options_catalog", help="Nautilus Parquet catalog folder")
    parser.add_argument("--csv-output", default="bybit_normalized", help="Folder for normalized output CSVs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    selected = select_csv_files(args.files)
    if not selected:
        LOG.info("No CSVs selected; exiting without creating a catalog")
        return 0
    try:
        underlying_path, execution_path, raw_trade_path, quote_path = identify_input_csvs(selected)
    except ValueError as exc:
        LOG.error("%s", exc)
        dialog = tk.Tk()
        dialog.withdraw()
        try:
            messagebox.showerror("Select a market-data CSV", str(exc), parent=dialog)
        finally:
            dialog.destroy()
        return 2
    output_dir = Path(args.csv_output).resolve()
    catalog_dir = Path(args.catalog).resolve()
    bars: list[Bar] = []
    bar_rows: list[dict[str, Any]] = []
    option_ticks: list[TradeTick] = []
    tick_rows: list[dict[str, Any]] = []
    option_quotes: list[QuoteTick] = []
    quote_rows: list[dict[str, Any]] = []
    option_greeks: list[OptionGreeks] = []
    greeks_rows: list[dict[str, Any]] = []
    executions_df: pd.DataFrame | None = None
    quotes_df: pd.DataFrame | None = None
    if underlying_path is not None:
        bars_df = read_csv(underlying_path, {
            "timestamp_ms", "open", "high", "low", "close", "volume_btc",
        })
        if "record_type" in bars_df:
            # A master CSV has one shared column set. Keep only candle rows so
            # execution/order fields cannot be mistaken for bar data.
            bars_df = bars_df[bars_df["record_type"] == "underlying_candle"].reset_index(drop=True)
            if bars_df.empty:
                raise ValueError("Master CSV contains no underlying_candle rows")
        bars, bar_rows = normalize_underlying_bars(bars_df)
    if execution_path is not None:
        executions_df = read_csv(execution_path, {
            "exec_id", "order_id", "timestamp_ms", "symbol", "side",
            "exec_qty_btc", "exec_price_usdt", "exec_fee_usdt", "is_maker",
        })
        if "record_type" in executions_df:
            # Route only actual executions to TradeTick conversion. The master
            # may also contain candle or account-lifecycle rows.
            executions_df = executions_df[executions_df["record_type"] == "option_execution"].reset_index(drop=True)
            if executions_df.empty:
                raise ValueError("Master CSV contains no option_execution rows")
        option_ticks, tick_rows = normalize_option_executions(executions_df)
    elif raw_trade_path is not None:
        raw_trades = read_csv(raw_trade_path, {"symbol", "price", "size", "side", "time"})
        executions_df = adapt_bybit_public_trades(raw_trades)
        option_ticks, tick_rows = normalize_option_executions(executions_df)
    if quote_path is not None:
        # A tagged master keeps quote rows in the shared wide table; ordinary
        # quote CSVs may use either the normalized names or Bybit WS names.
        quotes_df = pd.read_csv(quote_path, dtype=str, keep_default_na=False)
        if "record_type" in quotes_df.columns:
            quotes_df = quotes_df[quotes_df["record_type"].isin({"option_quote", "option_ticker"})].reset_index(drop=True)
            if quotes_df.empty:
                raise ValueError("Master CSV contains no option_quote/option_ticker rows")
        option_quotes, quote_rows, option_greeks, greeks_rows = normalize_option_quotes(quotes_df)

    # Resolve all instrument definitions before writing anything, so a failed
    # lookup or increment check cannot leave a partial catalog that looks
    # complete. Only public Bybit market endpoints are used here.
    session = requests.Session()
    session.headers.update({"User-Agent": "bybit-nautilus-csv-converter/1.0"})
    instruments = []
    archived_option_specs = load_archived_option_specs(selected)
    if bars:
        linear_spec = fetch_instrument_spec(session, "linear", "BTCUSDT")
        instruments.append(build_native_instrument("linear", "BTCUSDT", linear_spec))
    option_symbols_set: set[str] = set()
    if executions_df is not None:
        option_symbols_set.update(executions_df["symbol"].str.strip().str.upper())
    if quotes_df is not None:
        option_symbols_set.update(quotes_df["symbol"].astype(str).str.strip().str.upper())
    option_symbols = sorted(option_symbols_set)
    fallback_rules = None
    fallback_count = 0
    for index, symbol in enumerate(option_symbols, start=1):
        if symbol in archived_option_specs:
            option_spec = archived_option_specs[symbol]
            LOG.info("Using archived option instrument increments for %s", symbol)
        else:
            LOG.info("Fetching Bybit option spec %d/%d: %s", index, len(option_symbols), symbol)
            try:
                option_spec = fetch_instrument_spec(session, "option", symbol)
            except ValueError:
                if fallback_rules is None:
                    fallback_rules = fetch_uniform_btc_option_rules(session)
                option_spec = expired_option_spec_from_common_rule(symbol, fallback_rules)
                fallback_count += 1
                LOG.warning(
                    "%s: exact archived metadata is unavailable; using the uniform current BTC option "
                    "tickSize=%s, qtyStep=%s after validating this contract's fills. Historical launch time is unknown.",
                    symbol, fallback_rules["tickSize"], fallback_rules["qtyStep"],
                )
        if executions_df is not None:
            validate_option_fill_increments(executions_df, symbol, option_spec)
        instruments.append(build_native_instrument("option", symbol, option_spec))
        time.sleep(0.05)

    output_dir.mkdir(parents=True, exist_ok=True)
    # These CSVs are convenience views for inspection; the Nautilus catalog
    # below remains the primary typed Parquet output.
    if bar_rows:
        pd.DataFrame(bar_rows).to_csv(output_dir / "btc_underlying_bars.csv", index=False)
    if tick_rows:
        pd.DataFrame(tick_rows).to_csv(output_dir / "btc_option_execution_ticks.csv", index=False)
    if quote_rows:
        pd.DataFrame(quote_rows).to_csv(output_dir / "btc_option_quotes.csv", index=False)
    if greeks_rows:
        pd.DataFrame(greeks_rows).to_csv(output_dir / "btc_option_greeks.csv", index=False)

    catalog_dir.mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(str(catalog_dir))
    # Write definitions before events. Nautilus uses the definitions to
    # interpret each instrument ID and its price/size increments.
    for instrument in instruments:
        catalog.write_data([instrument])
    if bars:
        catalog.write_data(bars)
    if option_ticks:
        catalog.write_data(option_ticks)
    if option_quotes:
        catalog.write_data(option_quotes)
    if option_greeks:
        catalog.write_data(option_greeks)

    LOG.info("Wrote %d BTC one-minute bars", len(bars))
    LOG.info("Wrote %d BTC option execution TradeTick(s) across %d inferred instrument IDs",
             len(option_ticks), len({str(tick.instrument_id) for tick in option_ticks}))
    LOG.info("Wrote %d BTC option QuoteTick(s)", len(option_quotes))
    LOG.info("Wrote %d timestamped BTC option Greeks record(s)", len(option_greeks))
    LOG.info("Registered %d native instrument definition(s)", len(instruments))
    if fallback_count:
        LOG.warning("%d expired option definition(s) used a clearly tagged current-rule fallback.", fallback_count)
    LOG.info("Catalog: %s", catalog_dir)
    LOG.info("Normalized CSVs: %s", output_dir)
    if not option_quotes:
        LOG.warning("No timestamped option quotes were supplied; a realistic quote-driven option backtest is not ready.")
    LOG.warning("Private orders and closed PnL stay in their source CSVs; they are not market-data records.")
    LOG.info("Instrument IDs and native definitions were resolved automatically from Bybit public metadata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
