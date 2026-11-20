# TradeX (The Open Source TradingView)

TradeX is a free, self-hosted, TradingView-style charting app that runs on your own computer.
It puts one interface over several free market-data feeds and three chart engines. It runs Pine Script
indicators and can also show candles from your own MetaTrader 5 terminal, on this PC or on a VPS.

> Not affiliated with, endorsed by, or connected to TradingView, Inc. "TradingView" is their trademark.
> This project is an independent open-source tool. It is not financial advice.

![TradeX: gold on the free Deriv feed, Vela engine](docs/screenshots/01_hub_vela_gold.png)

| Four synced charts + favourite drawing tools | Indicators dialog |
|---|---|
| ![4 chart layout](docs/screenshots/02_four_chart_layout.png) | ![Indicators](docs/screenshots/03_indicators_dialog.png) |
| **KLineChart engine** | **Themes, fonts and candle colours** |
| ![KLineChart](docs/screenshots/04_klinechart_engine.png) | ![Settings](docs/screenshots/06_settings_themes.png) |
| **Order flow: session volume profile (real Binance trades)** | **Floating favourite-tools bar** |
| ![Order flow](docs/screenshots/07_order_flow_profile.png) | ![Favourites bar](docs/screenshots/08_favourites_bar.png) |

## What it does

- **Free feeds, no account needed:** Deriv (forex, gold, indices, crypto), Bybit (about 3,300 crypto
  and metal markets) and Binance (crypto). They work out of the box.
- **Your own MetaTrader 5 (optional):**
  - **Local MT5:** a terminal on this Windows PC (any broker).
  - **Remote MT5:** a terminal on a VPS or cloud server, reached through an SSH tunnel and a small bridge
    (`bridge/mt5_bridge.py`). The bridge only reads candles. It never places orders.
- **The broker switch keeps the instrument.** Gold stays gold when you move from Deriv to Bybit to MT5.
  Only exact matches are joined, so gold tokens such as PAXG are kept separate from gold spot.
- **Three chart engines, one switch:** Vela (main engine, with Pine Script, 79 native indicators, 79 drawing
  tools and layouts of up to 8 synced charts), Lightweight Charts with 86 drawing tools, and KLineChart Pro.
- **Deep history:** Vela loads 10,000 candles per chart (more in chart settings > Advanced), and fills any gap
  left by sleep or a reconnect before the next live candle.
- **Favourite drawing tools:** star a tool in the drawing toolbar and it appears on a small floating bar over the
  chart, like TradingView. Drag the bar by its grip, double-click the grip to turn it vertical. Stars and position
  are remembered.
- **Order flow (Vela chart types):** volume footprint, session volume profile and TPO, plus a delta indicator.
  Crypto and gold use real Binance futures trades with the buyer/seller side; other symbols get a labelled
  estimate from the chart's own candles. Deriv candles have no volume, so use Bybit, Binance or MT5 for order flow.
- **Pine Script:** write or import your own scripts in the Pine Editor. Scripts written for v1 to v4 are
  converted to v5 automatically and the original is kept. Converted values are not verified
  against any reference yet, so check them before you rely on them.
- **Backend log panel:** one live, redacted view of every feed: reconnects, lag, silent streams.
- **Safe by default:** binds to `127.0.0.1` only and serves only a whitelist of static files. With nothing
  configured, it never opens an SSH link.

## Quick start

Requirements: Python 3 (tested on 3.14). Node.js is not needed to run it; the built bundle is included.

```bash
git clone https://github.com/saifullahshafin/open-source-tradingview.git
cd open-source-tradingview
python -m venv .venv
# Windows: .venv\Scripts\activate      macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env        # optional: only needed for MT5
python hub/server.py
```

Then open <http://localhost:8770/hub.html>. On Windows you can double-click `start.bat` instead.

## Connect MetaTrader 5 (optional)

**MT5 on this Windows PC:** install MetaTrader 5, log in to your account, then run `pip install MetaTrader5`
and set `LOCAL_MT5=1` in `.env`. A "Local MT5" feed appears.

**MT5 on a VPS or server:** step-by-step instructions are in [AGENTS.md](AGENTS.md#remote-mt5-on-a-vps). In short:
1. On the server: run MT5, enable its MCP server (Tools > Options), then start
   `MT5_MCP_TOKEN=... python3 bridge/mt5_bridge.py`. It listens on `127.0.0.1:8797` only.
2. On your PC: add the server to `~/.ssh/config` (key login) and set `REMOTE_MT5_SSH_HOST=<alias>` in `.env`.
3. Restart the hub. It opens the SSH tunnel itself and reconnects if the link drops. A "Remote MT5" feed appears.

## CAIT feed (this fork)

This fork adds a fifth feed, `cait`: candles read straight from a
[CAIT](https://github.com/aibacus) TimescaleDB rather than a public exchange API. It is **off
unless `CAIT_DB_HOST` is set**, so the upstream behaviour is unchanged by default.

Two ticker shapes, because they are two different measurements:

| Ticker | Source | Volume |
|---|---|---|
| `CAIT_<SYMBOL>` | `crypto_ohlcvt` traded candles | real |
| `CAITB_<venue>_<BASE>` | `(best_bid+best_ask)/2` from `orderbook_features` | none — these are quotes |

Why read a database instead of the exchange: Kraken's public OHLC endpoint returns "up to 720
of the most recent entries (older data cannot be retrieved)", while the database holds years,
and it is the same data the strategies were tested against.

```bash
export CAIT_DB_HOST=<tailscale-ip>      # required; the feed stays off without it
export CAIT_DB_USER=tradex_ro           # a SELECT-only role
export CAIT_DB_PASSWORD_FILE=/run/secrets/tradex_ro_password
python hub/server.py
```

### Overlays

On a `CAIT_*` ticker, **Vela** and **Lightweight Charts** can both draw two overlays,
kept visually distinct because they are different claims:

| Overlay | Source | Drawn as |
|---|---|---|
| `orders` | `bb8_orders` — what a venue actually did | arrows; unfilled statuses faint |
| `backtest` | a `*_backtest_trades` table — what a model says *would* have happened | circles (entry) / squares (exit), coloured by P&L |

Pick them from the toolbar: an **overlay dropdown** appears whenever the chart is
on a `CAIT_*` ticker (and disappears otherwise, since
it has nothing to draw). Choosing *Backtest trades* reveals a table picker and a
strategy picker, both populated from the database.

On Vela the markers are real `arrowmarkup` / `arrowmarkdown` drawings placed just
off each bar, capped at 300 objects; on Lightweight they are series markers. Note
that Vela's bar times are epoch **milliseconds** while the markers API speaks
seconds — the adapter converts, and getting that wrong sends the server a
timestamp in the year 57927.

A note beside the dropdown says how many markers are drawn, and turns amber when
the set was sampled — e.g. `2000 of 12173 sampled, grouped to 38`, so a thinned
view never passes for the whole one.

The same thing is scriptable:

```js
labChart.setOverlay("orders");
labChart.setOverlay("backtest", { table: "donchian", strategy: "Donchian_LongOnly" });
labChart.setOverlay("off");
```

Markers landing on the same bar and side are collapsed into one, labelled with the
count and average price — a day with 400 fills would otherwise draw 400 overlapping
arrows. A dense window is **evenly sampled** across its whole range rather than
truncated to its oldest rows, and the response says so (`sampled`, `total`) so the
chart never implies it is showing everything.

`GET /api/cait/markers?symbol=&from=&to=&kind=orders|backtest[&table=&strategy=&status=]`
and `GET /api/cait/backtests?table=` back these.

### Live updates

With `CAIT_KAFKA_BOOTSTRAP` set, the forming bar comes from the `crypto-ohlcvt`
Kafka topic instead of being polled out of Postgres — the same shape as every
other TradeX feed (history over REST, the live edge over a socket).

```bash
export CAIT_KAFKA_BOOTSTRAP=<host>:29092   # optional; without it the feed polls
```

Postgres stays authoritative. Every `CAIT_RECONCILE_SEC` (default 15) the bar is
re-read from the database and replaces whatever the tail built, because the
database merges several collectors while the topic carries one. If the tail is
off, or does not carry a symbol, that reconcile is the update path and behaviour
degrades to the old poll.

The consumer uses `assign()`, never `subscribe()`: it starts at the end of each
partition and joins no consumer group, so a chart can never accrue lag, appear in
consumer-group monitoring, or become the thing that looks healthy while consuming
nothing.

`GET /api/cait/tail` answers "is the live feed actually live?" — messages seen,
messages matched, subscribed symbols, last error.

All access is read-only: the pool sets `default_transaction_read_only=on` and a 30 s
`statement_timeout`, and the feed never writes. Point it at a role with `SELECT` and nothing
else. Other settings: `CAIT_DB_PORT` (5432), `CAIT_DB_NAME` (crypto_ai_prod), `CAIT_DB_SSLMODE`,
`CAIT_BOOK_VENUES`, `CAIT_POLL_SEC`.

## Let your AI agent set it up

[AGENTS.md](AGENTS.md) is written for coding agents (Claude Code, Codex, Cursor and similar). It gives the exact
install, run and check steps for every setup (free feeds, local MT5, VPS MT5), the expected outputs, and the
security rules. Point your agent at it: *"Read AGENTS.md and set this up for me."*

## Project layout

```
hub/        the app: server.py (data + web server), hub.html (the shell), engine pages, Pine tools
bridge/     mt5_bridge.py: read-only candle bridge for a remote MT5 terminal
build/      source and build script for hub/vela.bundle.js and hub/vendor (npm)
docs/       screenshots
licenses/   third-party licence and notice files
```

## Roadmap

- Price and Pine alerts (built privately, not yet ported to run without a server)
- Accuracy tests: candles against the broker, Pine values against reference values
- Pine Script on the Lightweight and KLineChart engines (today an indicator stays on and draws when you switch
  back to Vela; adding one while on another engine switches to Vela)
- Drawings that carry across engines

## Licence

**AGPL-3.0-only** (see [LICENSE](LICENSE)), because the bundled Pine Script engine
([PineTS](https://github.com/LuxAlgo/PineTS) and [Vela-pinets](https://github.com/LuxAlgo/Vela-pinets)) is AGPL-3.0. If you run a
modified version for other people over a network, you must offer them its source.
Third-party parts keep their own licences. See [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
Market data belongs to its providers. Check each feed's terms before any public or commercial use.
