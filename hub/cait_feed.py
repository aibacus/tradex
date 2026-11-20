"""CAIT feed — candles from a CAIT TimescaleDB instead of a public exchange API.

CAIT (the private trading stack this fork serves) already stores everything a chart
needs. Reading it directly beats re-fetching from an exchange for two reasons:

  * depth of history. Kraken's public OHLC endpoint returns "up to 720 of the most
    recent entries (older data cannot be retrieved, regardless of the value of
    `since`)". The database has years.
  * it is the same data the strategies traded on. A chart drawn from a different
    source than the backtest is a chart that cannot settle an argument.

Two kinds of ticker, because they are two genuinely different measurements:

  CAIT_<SYMBOL>            traded candles from `crypto_ohlcvt` (1-minute base).
                           Real volume. This is what filled.
  CAITB_<VENUE>_<BASE>     mid-price candles from `orderbook_features`, i.e.
                           (best_bid + best_ask) / 2 sampled every ~5s per venue.
                           NO VOLUME — these are quotes, not trades. Useful for
                           spread and for comparing venues; misleading if read as
                           traded price.

Everything is READ-ONLY. The connection should be a role with SELECT and nothing
else (CAIT ships `tradex_ro`); this module never writes, and the chart must never
become a second author of trading state.

Configuration (all env, no defaults that reach a live host):

  CAIT_DB_HOST      required to enable the feed at all
  CAIT_DB_PORT      default 5432
  CAIT_DB_NAME      default crypto_ai_prod
  CAIT_DB_USER      default tradex_ro
  CAIT_DB_PASSWORD  or CAIT_DB_PASSWORD_FILE
  CAIT_DB_SSLMODE   default prefer
  CAIT_BOOK_VENUES  default kraken,binance,coinbase,okx
"""
from __future__ import annotations

import os
import threading
import time

try:
    import psycopg2
    from psycopg2 import pool as _pgpool
except Exception:  # the feed simply stays off when the driver is absent
    psycopg2 = None
    _pgpool = None

# Minutes per TradeX timeframe token. Mirrors server.TF_SEC, kept local so this
# module can be imported and tested without importing the server.
TF_MIN = {"M1": 1, "M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240, "D1": 1440}

HOST = os.getenv("CAIT_DB_HOST", "").strip()
PORT = int(os.getenv("CAIT_DB_PORT", "5432"))
DBNAME = os.getenv("CAIT_DB_NAME", "crypto_ai_prod").strip()
USER = os.getenv("CAIT_DB_USER", "tradex_ro").strip()
SSLMODE = os.getenv("CAIT_DB_SSLMODE", "prefer").strip()
BOOK_VENUES = [v.strip() for v in os.getenv("CAIT_BOOK_VENUES", "kraken,binance,coinbase,okx").split(",") if v.strip()]

# How long a symbol list is reused before another query. The set changes rarely.
SYMBOLS_TTL_S = 3600
# Live poll cadence. crypto_ohlcvt lands 1-minute bars; orderbook_features ~5s.
POLL_S = float(os.getenv("CAIT_POLL_SEC", "5"))


def _password():
    p = os.getenv("CAIT_DB_PASSWORD")
    if p:
        return p
    f = os.getenv("CAIT_DB_PASSWORD_FILE")
    if f:
        try:
            with open(f, encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            return None
    return None


def enabled():
    """The feed is off unless a host is configured and the driver is importable."""
    return bool(HOST) and psycopg2 is not None


_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = _pgpool.ThreadedConnectionPool(
                    1, int(os.getenv("CAIT_DB_POOL_MAX", "6")),
                    host=HOST, port=PORT, dbname=DBNAME, user=USER,
                    password=_password(), sslmode=SSLMODE,
                    connect_timeout=10,
                    # A chart must never hold a lock or a long scan against the
                    # trading database. orderbook_features is ~165M rows: an
                    # unbounded scan there exceeds any sane timeout, so cap it
                    # here rather than trusting every query to be well-formed.
                    options="-c statement_timeout=30000 -c default_transaction_read_only=on",
                )
    return _pool


def _query(sql, args):
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, args)
            rows = cur.fetchall()
        conn.rollback()  # read-only; end the txn so the connection is reusable
        return rows
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        pool.putconn(conn)


# --------------------------------------------------------------------------
# Ticker parsing
# --------------------------------------------------------------------------
def parse(ticker):
    """'CAIT_BTCUSD' -> ('ohlcv', 'BTCUSD', None); 'CAITB_kraken_BTC' -> ('book', 'BTC', 'kraken')."""
    t = (ticker or "").strip()
    if t.startswith("CAITB_"):
        rest = t[len("CAITB_"):]
        venue, _, base = rest.partition("_")
        if not base:
            raise ValueError(f"bad CAIT book ticker {ticker!r}: expected CAITB_<venue>_<base>")
        return "book", base, venue.lower()
    if t.startswith("CAIT_"):
        return "ohlcv", t[len("CAIT_"):], None
    raise ValueError(f"not a CAIT ticker: {ticker!r}")


def _symbol_forms(sym):
    """Both slash spellings, so the (symbol, time) index is still usable.

    CAIT stores the no-slash form, but both 'XBTUSD' and 'BTCUSD' spellings occur
    in practice. `symbol = ANY(...)` keeps the index; REPLACE(symbol,'/','') does
    not — that pattern was measured at 109 ms average over 3.3M calls.
    """
    s = sym.strip()
    out = {s, s.replace("/", "")}
    if "/" not in s and len(s) > 3:
        out.add(s[:-3] + "/" + s[-3:])
    return sorted(out)


# --------------------------------------------------------------------------
# History
# --------------------------------------------------------------------------
# Traded candles. time_bucket() on the 1-minute base covers every timeframe; the
# daily rollup is not used because it is bucket-CLOSE labelled while the chart
# wants bucket-open, and mixing the two would shift a day's candle by a day.
_SQL_OHLCV = """
SELECT EXTRACT(EPOCH FROM b)::bigint AS t,
       (array_agg(open  ORDER BY "time"))[1]::double precision,
       MAX(high)::double precision,
       MIN(low)::double precision,
       (array_agg(close ORDER BY "time" DESC))[1]::double precision,
       SUM(volume)::double precision
FROM (
  SELECT time_bucket(%(iv)s::interval, "time") AS b, "time", open, high, low, close, volume
  FROM crypto_ohlcvt
  WHERE symbol = ANY(%(syms)s) AND "time" < %(to_ts)s
) s
GROUP BY b ORDER BY b DESC LIMIT %(lim)s
"""

# Book mid candles. No volume column on purpose: these are quotes.
#
# Two sources, chosen by timeframe, because orderbook_features is ~165M rows and
# a single symbol/venue is still ~2.2M of them per 4 months. Scanning that raw for
# an H1 chart measured 20.5s — past any reasonable timeout and well past what a
# chart may cost the trading database.
#
#   M1    raw orderbook_features. ~12 samples/minute, so the candle's high/low is
#         a real intra-minute range.
#   >=M5  the orderbook_features_1m continuous aggregate, which already holds one
#         best_bid_last/best_ask_last per minute. 12x fewer rows to read.
#         CAVEAT: high/low are then the extremes of the per-minute CLOSING mids,
#         not of every tick inside the bar. Slightly narrower than the truth. For
#         a quote series being read for level and spread that is the right trade;
#         do not use these candles to measure intrabar range.
_SQL_BOOK_RAW = """
SELECT EXTRACT(EPOCH FROM b)::bigint AS t,
       (array_agg(mid ORDER BY ts))[1]::double precision,
       MAX(mid)::double precision,
       MIN(mid)::double precision,
       (array_agg(mid ORDER BY ts DESC))[1]::double precision
FROM (
  SELECT time_bucket(%(iv)s::interval, ts) AS b, ts,
         (best_bid + best_ask) / 2.0 AS mid
  FROM orderbook_features
  WHERE symbol = %(sym)s AND venue = %(venue)s
    AND ts < %(to_ts)s AND ts >= %(from_ts)s
    AND best_bid IS NOT NULL AND best_ask IS NOT NULL
) s
GROUP BY b ORDER BY b DESC LIMIT %(lim)s
"""

_SQL_BOOK_1M = """
SELECT EXTRACT(EPOCH FROM b)::bigint AS t,
       (array_agg(mid ORDER BY bucket))[1]::double precision,
       MAX(mid)::double precision,
       MIN(mid)::double precision,
       (array_agg(mid ORDER BY bucket DESC))[1]::double precision
FROM (
  SELECT time_bucket(%(iv)s::interval, bucket) AS b, bucket,
         (best_bid_last + best_ask_last) / 2.0 AS mid
  FROM orderbook_features_1m
  WHERE symbol = %(sym)s AND venue = %(venue)s
    AND bucket < %(to_ts)s AND bucket >= %(from_ts)s
    AND best_bid_last IS NOT NULL AND best_ask_last IS NOT NULL
) s
GROUP BY b ORDER BY b DESC LIMIT %(lim)s
"""


def history(ticker, tf, limit=1000, to_ms=None):
    """Bars oldest-first: [{time (epoch seconds), open, high, low, close, volume}]."""
    kind, sym, venue = parse(ticker)
    tf = tf if tf in TF_MIN else "M1"
    minutes = TF_MIN[tf]
    limit = max(1, min(int(limit or 1000), 5000))
    to_ts = (int(to_ms) / 1000.0) if to_ms else time.time()
    iv = f"{minutes} minutes"

    if kind == "ohlcv":
        rows = _query(_SQL_OHLCV, {
            "iv": iv, "syms": _symbol_forms(sym),
            "to_ts": _ts(to_ts), "lim": limit,
        })
        return [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
                 "low": float(r[3]), "close": float(r[4]),
                 "volume": float(r[5] or 0.0)} for r in reversed(rows)]

    # Book: bound the scan by time as well as LIMIT. orderbook_features is far too
    # large to let a LIMIT alone decide how much of it gets read.
    span_s = minutes * 60 * limit
    rows = _query(_SQL_BOOK_RAW if minutes == 1 else _SQL_BOOK_1M, {
        "iv": iv, "sym": sym, "venue": venue,
        "to_ts": _ts(to_ts), "from_ts": _ts(to_ts - span_s * 1.5), "lim": limit,
    })
    return [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4]), "volume": 0.0}
            for r in reversed(rows)]


def _ts(epoch_seconds):
    from datetime import datetime, timezone
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc)


# --------------------------------------------------------------------------
# Symbol list
# --------------------------------------------------------------------------
_symbols_cache = {"at": 0.0, "rows": []}


def symbols():
    """Chart rows for every CAIT instrument, cached for SYMBOLS_TTL_S."""
    if not enabled():
        return []
    now = time.time()
    if _symbols_cache["rows"] and now - _symbols_cache["at"] < SYMBOLS_TTL_S:
        return _symbols_cache["rows"]

    rows = []
    try:
        # Only symbols with recent data — a dead symbol in the picker is noise.
        for (sym,) in _query(
            "SELECT DISTINCT symbol FROM crypto_ohlcvt "
            "WHERE \"time\" > now() - interval '7 days' ORDER BY 1", {}):
            rows.append({"ticker": f"CAIT_{sym}", "name": f"{sym} (CAIT)",
                         "source": "cait", "group": "crypto", "digits": 2})
    except Exception as e:
        print("CAIT: crypto_ohlcvt symbols failed:", e, flush=True)

    try:
        for (sym, venue) in _query(
            "SELECT DISTINCT symbol, venue FROM orderbook_features "
            "WHERE ts > now() - interval '2 hours' ORDER BY 1, 2", {}):
            if venue and venue.lower() in BOOK_VENUES:
                rows.append({"ticker": f"CAITB_{venue.lower()}_{sym}",
                             "name": f"{sym} mid @ {venue} (CAIT book)",
                             "source": "cait", "group": "crypto", "digits": 2})
    except Exception as e:
        print("CAIT: orderbook_features symbols failed:", e, flush=True)

    if rows:
        _symbols_cache.update(at=now, rows=rows)
        print(f"CAIT: {len(rows)} symbols loaded", flush=True)
    return rows or _symbols_cache["rows"]


def symbols_loop():
    """Background refresh, mirroring the other feeds' loaders."""
    while True:
        try:
            symbols()
        except Exception as e:
            print("CAIT symbols loop:", e, flush=True)
        time.sleep(SYMBOLS_TTL_S if _symbols_cache["rows"] else 60)


def instrument_base(ticker):
    """The cross-broker key, so the broker switch can land on the same instrument."""
    try:
        kind, sym, venue = parse(ticker)
    except ValueError:
        return None, None
    if kind == "ohlcv":
        s = sym.replace("/", "")
        # 'XBT' is Kraken's spelling of BTC; the switch keys on BTCUSD.
        if s.startswith("XBT"):
            s = "BTC" + s[3:]
        return s, "CAIT"
    base = sym.replace("/", "")
    if base.startswith("XBT"):
        base = "BTC" + base[3:]
    if not base.endswith("USD"):
        base += "USD"
    return base, f"book mid @ {venue}"
