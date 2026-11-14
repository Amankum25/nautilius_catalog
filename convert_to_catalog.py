"""Convert the supplied Bybit BTC option CSVs to NautilusTrader Parquet.

This script converts local CSVs, fetches read-only public Bybit instrument
specifications, and writes native NautilusTrader Parquet data. Instrument IDs
are derived from the contract symbols in the CSVs. It never places orders.
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
from nautilus_trader.model.data import Bar, BarType, TradeTick
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
    """Derive the Bybit option ID from its exchange contract symbol."""
    symbol = symbol.strip().upper()
    if not OPTION_RE.fullmatch(symbol):
        raise ValueError(f"not a supported BTC USDT option symbol: {symbol!r}")
    return InstrumentId.from_str(f"{symbol}-OPTION.BYBIT")


def fetch_instrument_spec(session: requests.Session, category: str, symbol: str) -> dict[str, Any]:
    """Fetch exact public Bybit filters and contract terms for an instrument."""
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
    """Get the live BTC option increments; use only if all listed symbols agree."""
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
    """Build a clearly tagged fallback for expired contracts absent from Bybit APIs."""
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
    text = text_decimal(value, field)
    increment = Quantity.from_str(text) if quantity else Price.from_str(text)
    if Decimal(text) <= 0:
        raise ValueError(f"{field} must be positive")
    return increment


def build_native_instrument(category: str, symbol: str, spec: dict[str, Any]):
    """Build a Nautilus native instrument using exact Bybit metadata."""
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
    try:
        stamp_ms = int(Decimal(str(value).strip()))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} is not a millisecond timestamp: {value!r}") from exc
    if stamp_ms <= 0:
        raise ValueError(f"{field} must be a positive timestamp")
    return stamp_ms * 1_000_000


def read_csv(path: Path, required: set[str]) -> pd.DataFrame:
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
    if files:
        return [Path(name).expanduser().resolve() for name in files]
    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    try:
        selected = filedialog.askopenfilenames(
            title="Select a Bybit BTC candle or option execution CSV",
            initialdir=str(Path.cwd()),
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
    finally:
        root.destroy()
    return [Path(name).resolve() for name in selected]


def identify_input_csvs(paths: list[Path]) -> tuple[Path | None, Path | None, Path | None]:
    underlying_columns = {"timestamp_ms", "open", "high", "low", "close", "volume_btc"}
    execution_columns = {
        "exec_id", "order_id", "timestamp_ms", "symbol", "side",
        "exec_qty_btc", "exec_price_usdt", "exec_fee_usdt", "is_maker",
    }
    bybit_trade_columns = {"symbol", "price", "size", "side", "time"}
    # A tagged master CSV keeps heterogeneous source rows together. The
    # converter routes only its market-event rows to native Nautilus models.
    master_candidates = []
    for path in paths:
        columns = set(pd.read_csv(path, dtype=str, nrows=0).columns)
        if "record_type" in columns:
            kinds = set(pd.read_csv(path, dtype=str, usecols=["record_type"])["record_type"])
            if kinds & {"underlying_candle", "option_execution"}:
                master_candidates.append((path, kinds))
    if master_candidates:
        if len(paths) != 1 or len(master_candidates) != 1:
            raise ValueError("Select the master CSV by itself, or select the original CSVs; do not mix both.")
        path, kinds = master_candidates[0]
        return (
            path if "underlying_candle" in kinds else None,
            path if "option_execution" in kinds else None,
            None,
        )

    underlying_path = execution_path = raw_trade_path = None
    for path in paths:
        columns = set(pd.read_csv(path, dtype=str, nrows=0).columns)
        if underlying_columns <= columns:
            underlying_path = path
        if execution_columns <= columns:
            execution_path = path
        if bybit_trade_columns <= columns:
            raw_trade_path = path
    trade_paths = [p for p in (execution_path, raw_trade_path) if p is not None]
    if len(trade_paths) > 1:
        raise ValueError("Select one option execution/trade CSV at a time")
    if underlying_path is not None and underlying_path in trade_paths:
        raise ValueError("Underlying candles and option trades must be separate CSV files")
    if underlying_path is None and not trade_paths:
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
                "or market_btc_underlying_1m.csv to convert candles into Bars."
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
    return underlying_path, execution_path, raw_trade_path


def normalize_underlying_bars(frame: pd.DataFrame) -> tuple[list[Bar], list[dict[str, Any]]]:
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
    value = str(value).strip().lower()
    if value in {"true", "1", "yes"}:
        return True
    if value in {"false", "0", "no"}:
        return False
    raise ValueError(f"{field} is not a boolean: {value!r}")


def normalize_option_executions(frame: pd.DataFrame) -> tuple[list[TradeTick], list[dict[str, Any]]]:
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
    """Map Bybit V5 public trade rows to the converter's execution schema."""
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


def main() -> int:
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
        underlying_path, execution_path, raw_trade_path = identify_input_csvs(selected)
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
    executions_df: pd.DataFrame | None = None
    if underlying_path is not None:
        bars_df = read_csv(underlying_path, {
            "timestamp_ms", "open", "high", "low", "close", "volume_btc",
        })
        if "record_type" in bars_df:
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
            executions_df = executions_df[executions_df["record_type"] == "option_execution"].reset_index(drop=True)
            if executions_df.empty:
                raise ValueError("Master CSV contains no option_execution rows")
        option_ticks, tick_rows = normalize_option_executions(executions_df)
    elif raw_trade_path is not None:
        raw_trades = read_csv(raw_trade_path, {"symbol", "price", "size", "side", "time"})
        executions_df = adapt_bybit_public_trades(raw_trades)
        option_ticks, tick_rows = normalize_option_executions(executions_df)

    # Get exact venue metadata before writing anything, so a failed lookup
    # cannot leave a partial catalog that looks complete.
    session = requests.Session()
    session.headers.update({"User-Agent": "bybit-nautilus-csv-converter/1.0"})
    instruments = []
    if bars:
        linear_spec = fetch_instrument_spec(session, "linear", "BTCUSDT")
        instruments.append(build_native_instrument("linear", "BTCUSDT", linear_spec))
    option_symbols = sorted(set(executions_df["symbol"].str.strip().str.upper())) if executions_df is not None else []
    fallback_rules = None
    fallback_count = 0
    for index, symbol in enumerate(option_symbols, start=1):
        LOG.info("Fetching Bybit option spec %d/%d: %s", index, len(option_symbols), symbol)
        try:
            option_spec = fetch_instrument_spec(session, "option", symbol)
        except ValueError as exact_error:
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
    if bar_rows:
        pd.DataFrame(bar_rows).to_csv(output_dir / "btc_underlying_bars.csv", index=False)
    if tick_rows:
        pd.DataFrame(tick_rows).to_csv(output_dir / "btc_option_execution_ticks.csv", index=False)

    catalog_dir.mkdir(parents=True, exist_ok=True)
    catalog = ParquetDataCatalog(str(catalog_dir))
    for instrument in instruments:
        catalog.write_data([instrument])
    if bars:
        catalog.write_data(bars)
    if option_ticks:
        catalog.write_data(option_ticks)

    LOG.info("Wrote %d BTC one-minute bars", len(bars))
    LOG.info("Wrote %d BTC option execution TradeTick(s) across %d inferred instrument IDs",
             len(option_ticks), len({str(tick.instrument_id) for tick in option_ticks}))
    LOG.info("Registered %d native instrument definition(s)", len(instruments))
    if fallback_count:
        LOG.warning("%d expired option definition(s) used a clearly tagged current-rule fallback.", fallback_count)
    LOG.info("Catalog: %s", catalog_dir)
    LOG.info("Normalized CSVs: %s", output_dir)
    LOG.warning("The option metadata CSV has no event timestamp, so its quotes/Greeks are not written as historical ticks.")
    LOG.warning("Private orders and closed PnL stay in their source CSVs; they are not market-data records.")
    LOG.info("Instrument IDs and native definitions were resolved automatically from Bybit public metadata.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
