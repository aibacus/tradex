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

import json
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


# --------------------------------------------------------------------------
# Overlays: what actually traded, and what a backtest said would have
# --------------------------------------------------------------------------
# Two different kinds of claim, deliberately never merged into one list:
#
#   orders    rows from bb8_orders. What a venue did (or refused to do).
#   backtest  rows from a *_backtest_trades table. What a model says would have
#             happened. These are not trades and must never be shown as if they were.
#
# BB8 (the Go executor) is the sole author of execution state. This module only
# ever SELECTs; a chart that wrote here would be a second author, which is the
# failure this codebase already shipped once.

# Whitelist, because the table name is interpolated into SQL. A caller cannot
# reach an arbitrary table through this parameter.
BACKTEST_TABLES = {
    "donchian": "donchian_backtest_trades",
    "session_momentum": "session_momentum_backtest_trades",
    "mr_refined": "mr_refined_backtest_trades",
    "wr2": "wr2_backtest_trades",
}

# A busy symbol can have tens of thousands of orders inside one chart window.
# `ORDER BY created_at LIMIT n` would then return only the OLDEST n, so the chart
# would show markers for the first few days of the window and nothing after —
# silently, and looking exactly like "there were no orders later". Instead, count
# first and take an even stride through the window, so the whole range is
# represented and the caller is told the series was thinned.
_SQL_ORDERS = """
WITH w AS (
  SELECT created_at, side, status,
         COALESCE(filled_price, price)::double precision       AS px,
         COALESCE(filled_quantity, quantity)::double precision AS qty,
         COALESCE(bb8_strategy, '')                            AS strategy,
         COALESCE(exchange, '')                                AS venue,
         row_number() OVER (ORDER BY created_at) AS rn,
         count(*)     OVER ()                    AS total
  FROM bb8_orders
  WHERE symbol = ANY(%(syms)s)
    AND created_at >= %(from_ts)s AND created_at < %(to_ts)s
    AND status = ANY(%(statuses)s)
)
SELECT EXTRACT(EPOCH FROM created_at)::bigint, side, status, px, qty, strategy, venue, total
FROM w
WHERE total <= %(lim)s OR (rn - 1) %% GREATEST(1, (total / %(lim)s)::bigint) = 0
ORDER BY created_at LIMIT %(lim)s
"""


def order_markers(ticker, from_s, to_s, statuses=("filled",), limit=2000):
    """(markers, total_in_window). Side is reported as-is; entry/exit is not inferred.

    When total > limit the markers are an even sample across the window, not the
    first `limit` of them — see the query. Callers should surface the thinning.
    """
    kind, sym, _venue = parse(ticker)
    if kind != "ohlcv":
        return [], 0  # a book-mid series has no orders of its own
    rows = _query(_SQL_ORDERS, {
        "syms": _symbol_forms(sym), "from_ts": _ts(from_s), "to_ts": _ts(to_s),
        "statuses": list(statuses), "lim": int(limit),
    })
    total = int(rows[0][7]) if rows else 0
    out = []
    for t, side, status, px, qty, strategy, venue, _total in rows:
        if px is None:
            continue
        buy = (side or "").upper().startswith("B")
        # 'pending' means queued, not executed. Say so rather than drawing it as a fill.
        note = "" if status == "filled" else f" [{status}]"
        out.append({
            "kind": "order", "time": int(t), "side": "BUY" if buy else "SELL",
            "price": float(px), "qty": float(qty or 0.0), "status": status,
            "strategy": strategy, "venue": venue,
            "text": f"{'BUY' if buy else 'SELL'} {float(qty or 0):g} @ {float(px):g}"
                    + (f" · {strategy}" if strategy else "") + note,
        })
    return out, total


def backtest_markers(ticker, from_s, to_s, table_key, strategy=None, limit=2000):
    """Entry and exit points from a *_backtest_trades table. NOT trades that happened."""
    kind, sym, _venue = parse(ticker)
    if kind != "ohlcv":
        return []
    table = BACKTEST_TABLES.get(table_key)
    if not table:
        raise ValueError(f"unknown backtest table {table_key!r}; known: {', '.join(sorted(BACKTEST_TABLES))}")
    sql = f"""
        SELECT strategy, side,
               EXTRACT(EPOCH FROM entry_time)::bigint, entry_price::double precision,
               EXTRACT(EPOCH FROM exit_time)::bigint,  exit_price::double precision,
               pnl_pct::double precision
        FROM {table}
        WHERE symbol = ANY(%(syms)s)
          AND entry_time < %(to_ts)s AND exit_time >= %(from_ts)s
          {"AND strategy = %(strategy)s" if strategy else ""}
        ORDER BY entry_time LIMIT %(lim)s
    """
    args = {"syms": _symbol_forms(sym), "from_ts": _ts(from_s), "to_ts": _ts(to_s), "lim": int(limit)}
    if strategy:
        args["strategy"] = strategy
    out = []
    for strat, side, et, ep, xt, xp, pnl in _query(sql, args):
        long = not (side or "").upper().startswith("S")
        pnl = float(pnl or 0.0)
        out.append({"kind": "backtest_entry", "time": int(et), "side": "BUY" if long else "SELL",
                    "price": float(ep), "strategy": strat, "pnl_pct": pnl,
                    "text": f"{strat} {'long' if long else 'short'} entry @ {float(ep):g}"})
        if xt is not None and xp is not None:
            out.append({"kind": "backtest_exit", "time": int(xt), "side": "SELL" if long else "BUY",
                        "price": float(xp), "strategy": strat, "pnl_pct": pnl,
                        "text": f"{strat} exit @ {float(xp):g} · {pnl:+.2f}%"})
    return out


def backtest_strategies(table_key):
    """Distinct strategy labels in a backtest table, for a picker."""
    table = BACKTEST_TABLES.get(table_key)
    if not table:
        return []
    return [r[0] for r in _query(f"SELECT DISTINCT strategy FROM {table} ORDER BY 1", {}) if r[0]]


# --------------------------------------------------------------------------
# Live tail: the forming bar from Kafka, not from polling Postgres
# --------------------------------------------------------------------------
# The collector publishes each 1-minute bar to `crypto-ohlcvt` as it forms. That
# is the same bar the DB writer later lands, so polling Postgres for it adds the
# writer's ~3s plus up to a full poll interval for nothing. This tails the topic
# instead and the chart updates within a second, which is what every other TradeX
# feed already does (REST history + a live socket).
#
# Deliberate choices:
#   * assign(), never subscribe(). A chart wants "what is happening now" and has
#     no business owning a committed offset. With assign() librdkafka does not
#     join a consumer group, so this can never show up in consumer-group
#     monitoring, never accrues lag, and cannot be the thing that looks healthy
#     while consuming nothing (the 2026-09-22 failure mode).
#   * Start at the END of each partition. History comes from Postgres; replaying
#     the topic from the beginning would be both slow and wrong.
#   * Postgres stays authoritative. The DB merges several collectors while this
#     topic carries only the Kraken rapid collector (measured: 18 of 21 symbols,
#     every message `rapid_kraken_websocket`), so a periodic reconcile re-reads
#     the bar from the database and overwrites whatever the tail built.
KAFKA_BOOTSTRAP = os.getenv("CAIT_KAFKA_BOOTSTRAP", "").strip()
KAFKA_TOPIC = os.getenv("CAIT_KAFKA_TOPIC", "crypto-ohlcvt").strip()
# How often the database corrects the live tail. Also the poll cadence when the
# tail is unavailable or does not carry this symbol.
RECONCILE_S = float(os.getenv("CAIT_RECONCILE_SEC", "15"))


def kafka_enabled():
    return bool(KAFKA_BOOTSTRAP)


class _Tail:
    """One consumer thread for the process; fans 1-minute bars out to streams."""

    def __init__(self):
        self._subs = {}            # SYMBOL -> set of callables
        self._lock = threading.Lock()
        self._thread = None
        self.last_error = None
        self.messages = 0
        # raw counts every message on the topic, messages only those a chart asked
        # for. The pair distinguishes "topic is quiet" from "nobody wants these".
        self.raw = 0

    def subscribe(self, symbol, fn):
        key = symbol.replace("/", "").upper()
        with self._lock:
            self._subs.setdefault(key, set()).add(fn)
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, daemon=True)
                self._thread.start()

    def unsubscribe(self, symbol, fn):
        key = symbol.replace("/", "").upper()
        with self._lock:
            s = self._subs.get(key)
            if s:
                s.discard(fn)
                if not s:
                    self._subs.pop(key, None)

    def _dispatch(self, symbol, bar):
        with self._lock:
            fns = list(self._subs.get(symbol, ()))
        for fn in fns:
            try:
                fn(bar)
            except Exception:
                pass

    def _run(self):
        try:
            from confluent_kafka import Consumer, TopicPartition, OFFSET_END
        except Exception as e:
            self.last_error = f"confluent-kafka not installed: {e}"
            print("CAIT tail:", self.last_error, flush=True)
            return
        while True:
            try:
                c = Consumer({
                    "bootstrap.servers": KAFKA_BOOTSTRAP,
                    # librdkafka requires a group.id even for assign(). With
                    # assign() no group is joined, so nothing is registered
                    # broker-side and nothing is ever committed.
                    "group.id": "tradex-chart-tail-no-group",
                    "enable.auto.commit": False,
                    "socket.keepalive.enable": True,
                })
                meta = c.list_topics(KAFKA_TOPIC, timeout=10)
                parts = list(meta.topics[KAFKA_TOPIC].partitions)
                c.assign([TopicPartition(KAFKA_TOPIC, p, OFFSET_END) for p in parts])
                print(f"CAIT tail: {KAFKA_TOPIC} {len(parts)} partition(s) from end", flush=True)
                self.last_error = None
                while True:
                    msg = c.poll(1.0)
                    if msg is None or msg.error():
                        continue
                    try:
                        d = json.loads(msg.value())
                    except Exception as e:
                        # Record it: a bare `continue` here hid a missing import
                        # behind what looked like an idle topic.
                        self.last_error = f"decode: {e.__class__.__name__}: {e}"
                        continue
                    self.raw += 1
                    sym = str(d.get("symbol") or "").replace("/", "").upper()
                    if not sym:
                        continue
                    with self._lock:
                        wanted = sym in self._subs
                    if not wanted:
                        continue
                    bar = _bar_from_message(d)
                    if bar:
                        self.messages += 1
                        self._dispatch(sym, bar)
            except Exception as e:
                self.last_error = f"{e.__class__.__name__}: {e}"
                print("CAIT tail reconnecting:", self.last_error, flush=True)
                time.sleep(5)


def _bar_from_message(d):
    """One 1-minute bar from a `crypto-ohlcvt` payload, or None if unusable."""
    from datetime import datetime
    t = d.get("time") or d.get("timestamp")
    if not t:
        return None
    try:
        ts = int(datetime.fromisoformat(str(t).replace("Z", "+00:00")).timestamp())
    except Exception:
        return None
    try:
        close = float(d.get("close", d.get("price")))
        return {
            "time": ts - ts % 60,
            "open": float(d.get("open", close)),
            "high": float(d.get("high", close)),
            "low": float(d.get("low", close)),
            "close": close,
            "volume": float(d.get("volume") or 0.0),
        }
    except (TypeError, ValueError):
        return None


TAIL = _Tail()


# --------------------------------------------------------------------------
# Live book: the ladder from Redis, not a database copy of it
# --------------------------------------------------------------------------
# The multi-venue collector writes `{venue}:orderbook:{BASE}` with a ~100ms
# coalescing flush and a 60s TTL. `orderbook_features` is a 5-second sample of
# that same book landed into Postgres, so a CAITB_* chart reading the database
# was looking at a stale copy of something sitting in memory.
#
# Redis has no push for these keys (the collector SETEXes them), so this polls —
# but a Redis GET is sub-millisecond, which is a different thing entirely from
# polling a 165M-row hypertable.
#
# A missing key means the collector is not running or the TTL lapsed. It is NOT
# "spread zero" or "mid unchanged": the caller must treat None as no book, which
# is the same rule the CAIT codebase applies to its own LOB reads.
REDIS_URL_HOST = os.getenv("CAIT_REDIS_HOST", "").strip()
REDIS_PORT = int(os.getenv("CAIT_REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("CAIT_REDIS_DB", "0"))
# How often the ladder is re-read. The collector flushes about every 100ms;
# faster than that buys nothing but load.
BOOK_POLL_S = float(os.getenv("CAIT_BOOK_POLL_SEC", "0.5"))

_redis_client = None
_redis_lock = threading.Lock()


def _redis_password():
    p = os.getenv("CAIT_REDIS_PASSWORD")
    if p:
        return p
    f = os.getenv("CAIT_REDIS_PASSWORD_FILE")
    if f:
        try:
            with open(f, encoding="utf-8") as fh:
                return fh.read().strip()
        except OSError:
            return None
    return None


def redis_enabled():
    return bool(REDIS_URL_HOST)


def _redis():
    global _redis_client
    if _redis_client is None:
        with _redis_lock:
            if _redis_client is None:
                import redis as _r
                _redis_client = _r.Redis(
                    host=REDIS_URL_HOST, port=REDIS_PORT, db=REDIS_DB,
                    password=_redis_password(), socket_timeout=2,
                    socket_connect_timeout=3, decode_responses=True,
                )
    return _redis_client


def book_mid(venue, base):
    """(mid, ts_epoch) from the live ladder, or (None, None) when there is no book.

    None means no book — never zero, never a stale carry-forward.
    """
    try:
        raw = _redis().get(f"{venue}:orderbook:{base.upper()}")
    except Exception:
        return None, None
    if not raw:
        return None, None
    try:
        d = json.loads(raw)
        bid = float(d["bids"][0]["price"])
        ask = float(d["asks"][0]["price"])
    except (ValueError, KeyError, IndexError, TypeError):
        return None, None
    if not (bid > 0 and ask > 0):
        return None, None
    ts = None
    t = d.get("timestamp")
    if t:
        try:
            from datetime import datetime
            ts = datetime.fromisoformat(str(t).replace("Z", "+00:00")).timestamp()
        except Exception:
            ts = None
    return (bid + ask) / 2.0, (ts or time.time())
