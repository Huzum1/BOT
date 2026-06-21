import json
import threading
import time
from datetime import datetime
from collections import deque

import streamlit as st
import websocket
from urllib.request import urlopen, Request

# ─── CONFIG ──────────────────────────────────────────────────────
SYMBOL_BINANCE   = "btcusdc"
MIN_TRADE_QTY    = 0.0
WHALE_QTY        = 0.5
BIG_QTY          = 0.1
BOOK_RANGE_USD   = 50.0
ABSORPTION_RATIO = 3.0
ROUND_SECONDS    = 300
TRADE_LOG_SIZE   = 14

# ─── Shared state (un singur set de WS pe proces) ────────────────────────
lock = threading.Lock()


def _fresh_state():
    return {
        "next_boundary": None,
        "round_start_price": None,
        "round_number": 0,
        "poly_price": None,
        "round_start_time": None,
        "poly_price_history": deque(maxlen=120),
        "total_buy": 0.0, "total_sell": 0.0,
        "buy_count": 0, "sell_count": 0,
        "whale_buy": 0.0, "whale_sell": 0.0,
        "big_buy": 0.0, "big_sell": 0.0,
        "cvd": 0.0,
        "cvd_history": deque(maxlen=120),
        "trade_times": deque(maxlen=300),
        "book_bid_wall": 0.0, "book_ask_wall": 0.0, "book_mid": None,
        "last_minute_buy": 0.0, "last_minute_sell": 0.0,
        "minute_snapshots": [], "last_minute_mark": 0,
        "trade_log": deque(maxlen=TRADE_LOG_SIZE),
        "running": True,
        # diagnostic conexiuni
        "status": {"poly": "...", "trade": "...", "depth": "...", "source": "-"},
    }


# Domenii Binance de incercat in ordine (unele blocate pe anumite IP-uri/cloud)
WS_HOSTS = [
    "stream.binance.com:9443",
    "data-stream.binance.vision",
]
REST_HOSTS = [
    "https://api.binance.com",
    "https://data-api.binance.vision",
    "https://api.binance.us",
]


@st.cache_resource
def get_state():
    """State partajat o singura data pe proces; porneste si WS-urile."""
    s = _fresh_state()
    _start_workers(s)
    return s


# ─── Helpers de calcul (identice cu v2) ─────────────────────────────
def get_boundary(ts_ms):
    ts = ts_ms // 1000
    return ((ts // ROUND_SECONDS) + 1) * ROUND_SECONDS


def get_price(p):
    if "full_accuracy_value" in p and p["full_accuracy_value"]:
        try:
            return float(p["full_accuracy_value"]) / 1e18
        except Exception:
            pass
    return float(p["value"])


def trade_tier(qty):
    if qty >= WHALE_QTY:
        return "WHALE"
    if qty >= BIG_QTY:
        return "BIG"
    return "RETAIL"


def minute_signal(buy_pct):
    if buy_pct >= 70:
        return "AGRESIV UP"
    if buy_pct >= 60:
        return "MODERAT UP"
    if buy_pct <= 30:
        return "AGRESIV DOWN"
    if buy_pct <= 40:
        return "MODERAT DOWN"
    return "NEUTRU"


def compute_aggression(trade_times, total_vol, total_count, whale_vol):
    now = time.time()
    recent = [t for t in trade_times if now - t <= 10.0]
    tps = len(recent) / 10.0
    avg_size = (total_vol / total_count) if total_count > 0 else 0.0
    whale_share = (whale_vol / total_vol * 100) if total_vol > 0 else 0.0
    speed_score = min(tps / 5.0, 1.0) * 50
    size_score = min(avg_size / 0.2, 1.0) * 25
    whale_score = min(whale_share / 40.0, 1.0) * 25
    score = speed_score + size_score + whale_score
    if score >= 70:
        label = "🔥 FOARTE AGRESIVA"
    elif score >= 45:
        label = "⚡ AGRESIVA"
    elif score >= 20:
        label = "〰 NORMALA"
    else:
        label = "😴 LINISTITA"
    return label, score, tps


def compute_absorption(book_bid_wall, book_ask_wall, minute_buy, minute_sell):
    msgs = []
    if minute_buy > 0 and book_ask_wall >= minute_buy * ABSORPTION_RATIO:
        msgs.append(f"ASK wall absoarbe BUY ({book_ask_wall:.2f} vs {minute_buy:.2f} BTC)")
    if minute_sell > 0 and book_bid_wall >= minute_sell * ABSORPTION_RATIO:
        msgs.append(f"BID wall absoarbe SELL ({book_bid_wall:.2f} vs {minute_sell:.2f} BTC)")
    if not msgs:
        if book_bid_wall + book_ask_wall > 0:
            return ("FARA REZISTENTA", "Order book subtire — volumul agresiv misca usor pretul")
        return ("NECUNOSCUT", "Astept date order book...")
    return ("ABSORBTIE", "  ;  ".join(msgs))


def compute_divergence(cvd_history, price_history):
    now = time.time()
    cvd_old = next((c for (t, c) in cvd_history if now - t <= 60), None)
    px_old = next((p for (t, p) in price_history if now - t <= 60), None)
    cvd_now = cvd_history[-1][1] if cvd_history else None
    px_now = price_history[-1][1] if price_history else None
    if None in (cvd_old, px_old, cvd_now, px_now):
        return (False, "Strang date pentru divergenta (~60s)...")
    d_cvd = cvd_now - cvd_old
    d_px = px_now - px_old
    if d_cvd > 0.3 and d_px < -5:
        return (True, f"⚠ DIVERGENTA BULLISH: cumparare neta (+{d_cvd:.2f} BTC) dar pret in scadere ({d_px:+.1f}) — posibil REVERSAL UP")
    if d_cvd < -0.3 and d_px > 5:
        return (True, f"⚠ DIVERGENTA BEARISH: vanzare neta ({d_cvd:.2f} BTC) dar pret in urcare ({d_px:+.1f}) — posibil REVERSAL DOWN")
    return (False, f"Fara divergenta (CVD {d_cvd:+.2f} BTC / pret {d_px:+.1f} pe 60s)")


def composite_signal(buy_pct, absorption_label, has_divergence, aggression_score):
    direction = "UP" if buy_pct >= 50 else "DOWN"
    strength = abs(buy_pct - 50) * 2
    confidence = strength
    notes = []
    if absorption_label == "ABSORBTIE":
        confidence *= 0.5
        notes.append("absorbtie in order book")
    elif absorption_label == "FARA REZISTENTA":
        confidence = min(confidence * 1.2, 100)
        notes.append("order book subtire")
    if has_divergence:
        confidence *= 0.6
        notes.append("divergenta CVD/pret")
    if aggression_score >= 45:
        confidence = min(confidence * 1.15, 100)
        notes.append("flux agresiv")
    confidence = max(0, min(100, confidence))
    if confidence >= 65:
        verdict = f"PARIAZA {direction}"
        level = "strong"
    elif confidence >= 40:
        verdict = f"INCLINAT {direction} (slab)"
        level = "weak"
    else:
        verdict = "NU PARIA — semnal neclar"
        level = "neutral"
        direction = "NEUTRU"
    return verdict, confidence, notes, direction, level


# ─── PREDICTIE: semnale care anticipeaza miscarea ─────────────────────────
def cvd_momentum(cvd_history, window=10):
    """Acceleratia fluxului net pe ultimele `window` secunde.
    CVD care urca tot mai repede = presiune de cumparare care se intareste,
    de obicei VIZIBIL inainte ca pretul/oracle-ul sa reactioneze.
    Returneaza (slope BTC/s, eticheta)."""
    now = time.time()
    pts = [(t, c) for (t, c) in cvd_history if now - t <= window]
    if len(pts) < 2:
        return 0.0, "strang date..."
    slope = (pts[-1][1] - pts[0][1]) / max(pts[-1][0] - pts[0][0], 1e-6)
    if slope > 0.05:
        lbl = "↗ accelereaza CUMPARAREA"
    elif slope < -0.05:
        lbl = "↘ accelereaza VANZAREA"
    else:
        lbl = "→ flux stabil"
    return slope, lbl


def book_imbalance(bid_wall, ask_wall):
    """Dezechilibru order book: -100 (presiune SELL) .. +100 (presiune BUY).
    Peretii pasivi arata unde e lichiditatea; dezechilibrul indica
    directia in care pretul cedeaza mai usor URMATOR."""
    tot = bid_wall + ask_wall
    if tot <= 0:
        return 0.0, "fara date book"
    imb = (bid_wall - ask_wall) / tot * 100
    if imb > 20:
        lbl = "BID domina — suport, inclinat UP"
    elif imb < -20:
        lbl = "ASK domina — rezistenta, inclinat DOWN"
    else:
        lbl = "echilibrat"
    return imb, lbl


def lead_signal(price_history, window=8):
    """Lead Binance/oracle: panta pretului oracle pe ultimele secunde.
    Oracle-ul Polymarket actualizeaza cu mica intarziere; o panta clara
    aici prevede unde se aseaza urmatorul tick."""
    now = time.time()
    pts = [(t, p) for (t, p) in price_history if now - t <= window]
    if len(pts) < 2:
        return 0.0, "strang date..."
    slope = (pts[-1][1] - pts[0][1]) / max(pts[-1][0] - pts[0][0], 1e-6)
    if slope > 0.5:
        lbl = "↗ oracle urca"
    elif slope < -0.5:
        lbl = "↘ oracle scade"
    else:
        lbl = "→ plat"
    return slope, lbl


def project_round_end(price, start_p, elapsed, lead_slope):
    """Proiectie naiva a pretului la finalul rundei, la ritmul curent.
    Combina deriva recenta (lead_slope) cu timpul ramas."""
    remaining = max(ROUND_SECONDS - elapsed, 0)
    projected = price + lead_slope * remaining
    proj_delta = projected - (start_p or price)
    side = "UP" if proj_delta >= 0 else "DOWN"
    return projected, proj_delta, side


def predictive_verdict(cvd_slope, imb, lead_slope):
    """Scor predictiv -100..+100 din momentum CVD + imbalance book + lead oracle.
    Pozitiv = se construieste UP inainte sa fie evident; negativ = DOWN."""
    score = 0.0
    score += max(-1, min(1, cvd_slope / 0.2)) * 45      # flux net (cel mai timpuriu)
    score += max(-1, min(1, imb / 50.0)) * 30           # lichiditate pasiva
    score += max(-1, min(1, lead_slope / 2.0)) * 25     # deriva oracle
    score = max(-100, min(100, score))
    if score >= 35:
        return score, "UP", "🔮 SE CONSTRUIESTE UP", "strong"
    if score <= -35:
        return score, "DOWN", "🔮 SE CONSTRUIESTE DOWN", "strong"
    if score >= 15:
        return score, "UP", "usor inclinat UP", "weak"
    if score <= -15:
        return score, "DOWN", "usor inclinat DOWN", "weak"
    return score, "NEUTRU", "fara avans clar", "neutral"


def reset_round(s, start_price, round_num):
    s["round_start_price"] = start_price
    s["round_number"] = round_num
    s["round_start_time"] = time.time()
    s["total_buy"] = s["total_sell"] = 0.0
    s["buy_count"] = s["sell_count"] = 0
    s["whale_buy"] = s["whale_sell"] = 0.0
    s["big_buy"] = s["big_sell"] = 0.0
    s["cvd"] = 0.0
    s["cvd_history"].clear()
    s["trade_times"].clear()
    s["minute_snapshots"] = []
    s["last_minute_buy"] = s["last_minute_sell"] = 0.0
    s["last_minute_mark"] = 0
    s["trade_log"].clear()


# ─── WebSocket workers ──────────────────────────────────────────
def _poly_on_message(s, ws, msg):
    s["status"]["poly"] = "✓ live"
    data = json.loads(msg)
    if data.get("topic") != "crypto_prices_chainlink":
        return
    p = data["payload"]
    chain_ts = p["timestamp"]
    price = get_price(p)
    with lock:
        s["poly_price"] = price
        s["poly_price_history"].append((time.time(), price))
        if s["next_boundary"] is None:
            s["next_boundary"] = get_boundary(chain_ts)
        if chain_ts >= s["next_boundary"] * 1000:
            reset_round(s, price, s["round_number"] + 1)
            s["next_boundary"] += ROUND_SECONDS


def _start_poly_ws(s):
    while s["running"]:
        try:
            ws = websocket.WebSocketApp(
                "wss://ws-live-data.polymarket.com",
                on_open=lambda ws: ws.send(json.dumps({
                    "action": "subscribe",
                    "subscriptions": [{
                        "topic": "crypto_prices_chainlink",
                        "type": "*",
                        "filters": "{\"symbol\":\"btc/usd\"}",
                    }],
                })),
                on_message=lambda ws, m: _poly_on_message(s, ws, m),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception:
            pass
        time.sleep(2)


def _binance_trade_on_message(s, ws, msg):
    data = json.loads(msg)
    qty = float(data["q"])
    is_buy = data["m"] is False
    if qty < MIN_TRADE_QTY:
        return
    tier = trade_tier(qty)
    with lock:
        if s["round_start_time"] is None:
            return
        poly_price_now = s["poly_price"] or 0.0
        s["trade_times"].append(time.time())
        if is_buy:
            s["total_buy"] += qty
            s["buy_count"] += 1
            s["last_minute_buy"] += qty
            s["cvd"] += qty
            if tier == "WHALE":
                s["whale_buy"] += qty
            elif tier == "BIG":
                s["big_buy"] += qty
        else:
            s["total_sell"] += qty
            s["sell_count"] += 1
            s["last_minute_sell"] += qty
            s["cvd"] -= qty
            if tier == "WHALE":
                s["whale_sell"] += qty
            elif tier == "BIG":
                s["big_sell"] += qty
        s["cvd_history"].append((time.time(), s["cvd"]))
        s["trade_log"].appendleft({
            "type": "BUY" if is_buy else "SELL",
            "price": poly_price_now,
            "qty": qty,
            "tier": tier,
            "time": datetime.now().strftime("%H:%M:%S"),
        })


def _start_binance_trade_ws(s):
    i = 0
    while s["running"]:
        host = WS_HOSTS[i % len(WS_HOSTS)]
        i += 1
        try:
            s["status"]["trade"] = f"conectare {host}..."
            ws = websocket.WebSocketApp(
                f"wss://{host}/ws/{SYMBOL_BINANCE}@aggTrade",
                on_open=lambda ws: s["status"].update(trade=f"✓ live ({host})"),
                on_message=lambda ws, m: _binance_trade_on_message(s, ws, m),
                on_error=lambda ws, e: s["status"].update(trade=f"✗ {e}"),
                on_close=lambda ws, *a: s["status"].update(trade="inchis, reconectez..."),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            s["status"]["trade"] = f"✗ {e}"
        time.sleep(2)


def _binance_depth_on_message(s, ws, msg):
    data = json.loads(msg)
    bids = data.get("bids") or data.get("b") or []
    asks = data.get("asks") or data.get("a") or []
    if not bids or not asks:
        return
    bids = [[float(p), float(q)] for p, q in bids]
    asks = [[float(p), float(q)] for p, q in asks]
    mid = (bids[0][0] + asks[0][0]) / 2.0
    bid_wall = sum(q for (p, q) in bids if mid - p <= BOOK_RANGE_USD)
    ask_wall = sum(q for (p, q) in asks if p - mid <= BOOK_RANGE_USD)
    with lock:
        s["book_mid"] = mid
        s["book_bid_wall"] = bid_wall
        s["book_ask_wall"] = ask_wall


def _start_binance_depth_ws(s):
    i = 0
    while s["running"]:
        host = WS_HOSTS[i % len(WS_HOSTS)]
        i += 1
        try:
            s["status"]["depth"] = f"conectare {host}..."
            ws = websocket.WebSocketApp(
                f"wss://{host}/ws/{SYMBOL_BINANCE}@depth20@100ms",
                on_open=lambda ws: s["status"].update(depth=f"✓ live ({host})"),
                on_message=lambda ws, m: _binance_depth_on_message(s, ws, m),
                on_error=lambda ws, e: s["status"].update(depth=f"✗ {e}"),
                on_close=lambda ws, *a: s["status"].update(depth="inchis, reconectez..."),
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            s["status"]["depth"] = f"✗ {e}"
        time.sleep(2)


def _http_get(path):
    """Incearca fiecare host REST pana raspunde unul. Returneaza JSON sau None."""
    for base in REST_HOSTS:
        try:
            req = Request(base + path, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=8) as r:
                return json.loads(r.read().decode()), base
        except Exception:
            continue
    return None, None


def _binance_rest_fallback(s):
    """
    Daca WebSocket-ul Binance e blocat (frecvent pe cloud / eroare 451),
    populam order book si volumul prin REST polling, ca sa nu ramana totul pe 0.
    """
    sym = SYMBOL_BINANCE.upper()
    last_trade_id = None
    while s["running"]:
        time.sleep(2)
        # Activam fallback doar daca WS-ul de trade nu e live
        if str(s["status"].get("trade", "")).startswith("✓"):
            continue

        # 1) Order book (depth) prin REST
        depth, base = _http_get(f"/api/v3/depth?symbol={sym}&limit=20")
        if depth:
            try:
                _binance_depth_on_message(s, None, json.dumps(depth))
                s["status"]["depth"] = f"✓ REST ({base})"
                s["status"]["source"] = "REST fallback"
            except Exception:
                pass

        # 2) Trade-uri recente prin REST
        trades, base = _http_get(f"/api/v3/trades?symbol={sym}&limit=200")
        if trades:
            s["status"]["trade"] = f"✓ REST ({base})"
            for t in trades:
                tid = t.get("id")
                if last_trade_id is not None and tid is not None and tid <= last_trade_id:
                    continue
                # m=isBuyerMaker; aggressor=buy cand isBuyerMaker e False
                fake = json.dumps({"q": t["qty"], "m": t["isBuyerMaker"]})
                _binance_trade_on_message(s, None, fake)
            if trades:
                last_trade_id = trades[-1].get("id", last_trade_id)


def _start_workers(s):
    threading.Thread(target=_start_poly_ws, args=(s,), daemon=True).start()
    threading.Thread(target=_start_binance_trade_ws, args=(s,), daemon=True).start()
    threading.Thread(target=_start_binance_depth_ws, args=(s,), daemon=True).start()
    threading.Thread(target=_binance_rest_fallback, args=(s,), daemon=True).start()


# ─── UI ────────────────────────────────────────────────────
st.set_page_config(page_title="Am cea mai faina iubita", page_icon="📊", layout="wide")

CSS = """
<style>
#MainMenu, footer, header {visibility: hidden;}
.block-container {padding-top: 1.2rem; padding-bottom: 1rem; max-width: 1200px;}
body {background: #0b0e14;}
.stApp {background: radial-gradient(1200px 600px at 20% -10%, #131a2b 0%, #0b0e14 55%);}
.card {
  background: rgba(255,255,255,0.03);
  border: 1px solid rgba(255,255,255,0.08);
  border-radius: 16px;
  padding: 16px 18px;
  box-shadow: 0 8px 30px rgba(0,0,0,0.35);
  backdrop-filter: blur(6px);
  height: 100%;
}
.card h4 {margin: 0 0 10px 0; font-size: 12px; letter-spacing: .12em;
  text-transform: uppercase; color: #8b95a7;}
.big {font-size: 30px; font-weight: 800; line-height: 1.1;}
.sub {color: #8b95a7; font-size: 12px;}
.green {color: #2fd47a;} .red {color: #ff5d6c;} .amber {color: #ffc857;}
.verdict {border-radius: 18px; padding: 18px 22px; text-align: center;
  font-size: 26px; font-weight: 800; letter-spacing: .02em;}
.v-strong-up {background: linear-gradient(90deg,#0f3,#0a8); color:#031; }
.v-strong-down {background: linear-gradient(90deg,#f36,#a02); color:#fff; }
.v-weak {background: rgba(255,255,255,0.06); color:#e7ecf5; border:1px solid rgba(255,255,255,.12);}
.v-neutral {background: rgba(255,200,87,.12); color:#ffc857; border:1px solid rgba(255,200,87,.3);}
.bar {height: 12px; border-radius: 8px; overflow: hidden; display:flex; background:#1a2030;}
.pill {display:inline-block; padding:3px 10px; border-radius:999px; font-size:11px;
  background: rgba(255,255,255,.07); color:#cfd6e4; margin-right:6px;}
.trade {font-family: ui-monospace, monospace; font-size: 13px; padding: 3px 0;
  border-bottom: 1px solid rgba(255,255,255,.05);}
.timer {height: 8px; border-radius: 6px; background:#1a2030; overflow:hidden;}
.tip {border-bottom: 1px dotted #6b7689; cursor: help; position: relative;}
.tip .tt {visibility:hidden; opacity:0; transition:.15s; position:absolute; z-index:50;
  bottom:130%; left:0; width:240px; background:#0f1422; color:#cfd6e4;
  border:1px solid rgba(255,255,255,.12); border-radius:10px; padding:8px 10px;
  font-size:11px; line-height:1.4; box-shadow:0 10px 30px rgba(0,0,0,.5);}
.tip:hover .tt {visibility:visible; opacity:1;}
.badge {display:inline-block;padding:2px 9px;border-radius:999px;font-size:11px;font-weight:700;}
.b-up {background:rgba(47,212,122,.16);color:#2fd47a;}
.b-down {background:rgba(255,93,108,.16);color:#ff5d6c;}
.b-neu {background:rgba(255,200,87,.16);color:#ffc857;}
.gauge {height:10px;border-radius:8px;background:linear-gradient(90deg,#ff5d6c,#1a2030,#2fd47a);position:relative;}
.gauge .needle {position:absolute;top:-3px;width:3px;height:16px;background:#fff;border-radius:2px;box-shadow:0 0 6px #fff;}
.predict {border-radius:16px;padding:14px 18px;border:1px solid rgba(255,255,255,.1);
  background:linear-gradient(135deg,rgba(124,92,255,.12),rgba(47,212,122,.05));}
.kbig {font-size:22px;font-weight:800;}
</style>
"""
st.markdown(CSS, unsafe_allow_html=True)


def tip(text, hint):
    return f"<span class='tip'>{text}<span class='tt'>{hint}</span></span>"


def card(title, inner, hint=None):
    head = title if not hint else f"{tip(title, hint)}"
    st.markdown(f'<div class="card"><h4>{head}</h4>{inner}</div>', unsafe_allow_html=True)


def render(s):
    with lock:
        snap = {
            "tb": s["total_buy"], "ts": s["total_sell"],
            "bc": s["buy_count"], "sc": s["sell_count"],
            "price": s["poly_price"], "start_p": s["round_start_price"],
            "start_t": s["round_start_time"], "rn": s["round_number"],
            "trades": list(s["trade_log"]),
            "mb": s["last_minute_buy"], "ms": s["last_minute_sell"],
            "cvd": s["cvd"], "wb": s["whale_buy"], "ws": s["whale_sell"],
            "gb": s["big_buy"], "gs": s["big_sell"],
            "bid_wall": s["book_bid_wall"], "ask_wall": s["book_ask_wall"],
            "book_mid": s["book_mid"],
            "trade_times": list(s["trade_times"]),
            "cvd_history": list(s["cvd_history"]),
            "price_history": list(s["poly_price_history"]),
        }

    st.markdown(
        "<div style='display:flex;justify-content:space-between;align-items:center;'>"
        "<div style='font-size:22px;font-weight:800;'>📊 Am cea mai faina iubita "
        "<span class='sub'>Binance volum + order book · Polymarket oracle</span></div>"
        f"<div class='pill'>Runda #{snap['rn']}</div></div>",
        unsafe_allow_html=True,
    )

    if snap["start_t"] is None or snap["price"] is None:
        st.info("⏳ Se conecteaza la Binance + Polymarket... reincarca in cateva secunde.")
        return

    elapsed = int(time.time() - snap["start_t"])
    current_minute = elapsed // 60 + 1
    total = snap["tb"] + snap["ts"]
    buy_pct = (snap["tb"] / total * 100) if total > 0 else 50.0
    net_btc = snap["tb"] - snap["ts"]
    price_delta = (snap["price"] - snap["start_p"]) if snap["start_p"] else 0.0
    efficiency = (total / abs(price_delta)) if abs(price_delta) > 0.01 else None

    aggr_label, aggr_score, tps = compute_aggression(
        snap["trade_times"], total, snap["bc"] + snap["sc"], snap["wb"] + snap["ws"])
    absorption_label, absorption_msg = compute_absorption(
        snap["bid_wall"], snap["ask_wall"], snap["mb"], snap["ms"])
    has_div, div_msg = compute_divergence(snap["cvd_history"], snap["price_history"])
    verdict, confidence, notes, direction, level = composite_signal(
        buy_pct, absorption_label, has_div, aggr_score)

    # Semnale PREDICTIVE (cu un pas inainte)
    cvd_slope, cvd_lbl = cvd_momentum(snap["cvd_history"])
    imb, imb_lbl = book_imbalance(snap["bid_wall"], snap["ask_wall"])
    lead_slope, lead_lbl = lead_signal(snap["price_history"])
    proj_px, proj_delta, proj_side = project_round_end(
        snap["price"], snap["start_p"], elapsed, lead_slope)
    pscore, pdir, plabel, plevel = predictive_verdict(cvd_slope, imb, lead_slope)

    # ─── Verdict (hero) ───
    vclass = {
        "strong": "v-strong-up" if direction == "UP" else "v-strong-down",
        "weak": "v-weak", "neutral": "v-neutral",
    }[level]
    note_html = "".join(f"<span class='pill'>{n}</span>" for n in notes)
    st.markdown(
        f"<div class='verdict {vclass}'>{verdict}"
        f"<div style='font-size:14px;font-weight:600;margin-top:6px;'>incredere {confidence:.0f}%</div></div>"
        f"<div style='margin-top:8px;'>{note_html}</div>",
        unsafe_allow_html=True,
    )

    timer_pct = min(elapsed / ROUND_SECONDS, 1.0) * 100
    tcol = "#2fd47a" if timer_pct < 50 else ("#ffc857" if timer_pct < 80 else "#ff5d6c")
    st.markdown(
        f"<div style='margin:12px 0 4px;'><span class='sub'>min {current_minute}/5 · {elapsed}s/{ROUND_SECONDS}s</span>"
        f"<div class='timer'><div style='width:{timer_pct}%;height:100%;background:{tcol};'></div></div></div>",
        unsafe_allow_html=True,
    )

    st.write("")
    # ─── PANOU PREDICTIE (cu un pas inainte) ───
    needle = (pscore + 100) / 2  # 0..100
    pbadge = {"UP": "b-up", "DOWN": "b-down", "NEUTRU": "b-neu"}[pdir]
    pj = "green" if proj_delta >= 0 else "red"
    st.markdown(
        "<div class='predict'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;'>"
        f"<div class='kbig'>🔮 Predictie: <span class='badge {pbadge}'>{plabel}</span></div>"
        f"<div class='sub'>scor anticipare {pscore:+.0f} / 100</div></div>"
        f"<div class='gauge' style='margin:10px 0;'><div class='needle' style='left:calc({needle}% - 1px);'></div></div>"
        "<div style='display:flex;gap:18px;flex-wrap:wrap;font-size:13px;'>"
        f"<span>{tip('Flux net (10s)', 'Momentum CVD: cat de repede se acumuleaza cumparare/vanzare neta. Se vede INAINTE ca pretul sa reactioneze.')}: "
        f"<b>{cvd_slope:+.3f} BTC/s</b> · {cvd_lbl}</span>"
        f"<span>{tip('Imbalance book', 'Dezechilibrul peretilor pasivi bid vs ask. Arata in ce directie cedeaza pretul mai usor urmator.')}: "
        f"<b>{imb:+.0f}</b> · {imb_lbl}</span>"
        f"<span>{tip('Lead oracle', 'Panta recenta a oracle-ului. Polymarket actualizeaza cu mica intarziere, deci panta prevede urmatorul tick.')}: "
        f"<b>{lead_slope:+.2f} USD/s</b> · {lead_lbl}</span>"
        f"<span>{tip('Proiectie final runda', 'Extrapolare la ritmul curent: unde s-ar inchide runda fata de pretul de start.')}: "
        f"<b class='{pj}'>{proj_px:,.0f} ({proj_delta:+.0f}, {proj_side})</b></span>"
        "</div></div>",
        unsafe_allow_html=True,
    )

    st.write("")
    # ─── Rand 1: pret / net / agresivitate ───
    c1, c2, c3 = st.columns(3)
    with c1:
        pc = "green" if price_delta >= 0 else "red"
        card("Oracle pret",
             f"<div class='big'>{snap['price']:,.2f} <span class='sub'>USD</span></div>"
             f"<div class='sub'>start {snap['start_p']:,.2f} · "
             f"<span class='{pc}'>Δ {price_delta:+.2f}</span></div>",
             "Pretul de referinta (oracle Chainlink via Polymarket) si cat s-a miscat fata de startul rundei. Δ verde = peste start (UP), rosu = sub start (DOWN).")
    with c2:
        nc = "green" if net_btc >= 0 else "red"
        nd = "UP" if net_btc >= 0 else "DOWN"
        card("Net flow",
             f"<div class='big {nc}'>{net_btc:+.3f} <span class='sub'>BTC</span></div>"
             f"<div class='sub'>a ramas net pe <span class='{nc}'>{nd}</span></div>",
             "Volum BUY agresiv minus SELL agresiv pe runda. Pozitiv = cumparatorii domina (presiune UP); negativ = vanzatorii domina (presiune DOWN).")
    with c3:
        card("Agresivitate",
             f"<div class='big'>{aggr_label}</div>"
             f"<div class='sub'>{tps:.1f} trade/sec · scor {aggr_score:.0f}/100</div>",
             "Cat de agresiv e fluxul: viteza trade-urilor, marimea medie si prezenta whale-urilor. Agresivitate mare intareste semnalul din directia dominanta.")

    st.write("")
    # ─── Rand 2: presiune buy/sell + categorii ───
    c4, c5 = st.columns([2, 1])
    with c4:
        bar = (f"<div class='bar'><div style='width:{buy_pct}%;background:#2fd47a;'></div>"
               f"<div style='width:{100-buy_pct}%;background:#ff5d6c;'></div></div>")
        card("Presiune BUY / SELL",
             f"<div style='display:flex;justify-content:space-between;'>"
             f"<span class='green'><b>BUY {buy_pct:.1f}%</b> · {snap['tb']:.3f} BTC ({snap['bc']})</span>"
             f"<span class='red'><b>{100-buy_pct:.1f}% SELL</b> · {snap['ts']:.3f} BTC ({snap['sc']})</span>"
             f"</div><div style='margin-top:10px;'>{bar}</div>",
             "Procentul e calculat pe VOLUM (BTC), nu pe numarul de trade-uri. Cifra din paranteza = cate trade-uri. Multe trade-uri mici vs putine mari iti spune cine e mai 'greu'.")
    with c5:
        wnet = snap["wb"] - snap["ws"]
        wc = "green" if wnet >= 0 else "red"
        card("Whales / Big",
             f"<div>🐋 Whale (≥{WHALE_QTY}): "
             f"<span class='green'>{snap['wb']:.3f}</span> / <span class='red'>{snap['ws']:.3f}</span> "
             f"<span class='{wc}'>net {wnet:+.3f}</span></div>"
             f"<div style='margin-top:6px;'>🐟 Big ({BIG_QTY}-{WHALE_QTY}): "
             f"<span class='green'>{snap['gb']:.3f}</span> / <span class='red'>{snap['gs']:.3f}</span></div>",
             "Volumul jucatorilor mari, care de obicei muta piata. Whale net pozitiv = banii grei cumpara (semnal UP timpuriu); negativ = vand.")

    st.write("")
    # ─── Rand 3: order book + CVD/divergenta ───
    c6, c7 = st.columns(2)
    with c6:
        mid_txt = f"{snap['book_mid']:,.2f}" if snap["book_mid"] else "-"
        eff_txt = (f"{efficiency:.3f} BTC / 1 USD "
                   f"({'absorbtie mare' if efficiency and efficiency > 0.5 else 'misca usor'})"
                   if efficiency is not None else "-")
        acolor = {"ABSORBTIE": "amber", "FARA REZISTENTA": "green"}.get(absorption_label, "sub")
        card(f"Order book (±{BOOK_RANGE_USD:.0f} USD)",
             f"<div>BID wall <span class='green'>{snap['bid_wall']:.3f}</span> · "
             f"ASK wall <span class='red'>{snap['ask_wall']:.3f}</span> · mid {mid_txt}</div>"
             f"<div style='margin-top:6px;' class='{acolor}'>{absorption_msg}</div>"
             f"<div class='sub' style='margin-top:6px;'>Eficienta: {eff_txt}</div>",
             "Lichiditatea pasiva langa pret. Un perete mare ASK absoarbe cumpararea (pret blocat = reversal posibil). Eficienta mare = volum mult fara miscare = absorbtie.")
    with c7:
        cc = "green" if snap["cvd"] >= 0 else "red"
        dcolor = "amber" if has_div else "sub"
        card("CVD / Divergenta",
             f"<div class='big {cc}'>{snap['cvd']:+.3f} <span class='sub'>BTC</span></div>"
             f"<div class='{dcolor}' style='margin-top:6px;'>{div_msg}</div>",
             "CVD = volum cumulativ net. Divergenta = CVD urca dar pretul scade (sau invers) = reversal probabil. Exact tiparul: oracle down, cumparare masiva, apoi up.")

    st.write("")
    # ─── Rand 4: tranzactii recente ───
    rows = ""
    for t in snap["trades"][:12]:
        c = "green" if t["type"] == "BUY" else "red"
        tag = {"WHALE": "🐋", "BIG": "🐟", "RETAIL": "  "}.get(t.get("tier"), "")
        rows += (f"<div class='trade'>{tag} <span class='{c}'>[{t['type']:>4}]</span> "
                 f"{t['time']} · {t['price']:,.2f} USD · "
                 f"<span class='{c}'>{t['qty']:.4f} BTC</span></div>")
    card("Ultimele tranzactii (pret = oracle Polymarket)", rows or "<div class='sub'>...</div>")


def main():
    s = get_state()
    with st.sidebar:
        st.markdown("### ⚙️ Setari")
        st.caption("Pereche: " + SYMBOL_BINANCE.upper())
        refresh = st.slider("Refresh (sec)", 1, 10, 2)
        st.caption("Datele vin live prin WebSocket (cu fallback REST).")
        st.markdown("#### 📡 Conexiuni")
        st.write(f"Polymarket: {s['status']['poly']}")
        st.write(f"Binance trade: {s['status']['trade']}")
        st.write(f"Binance depth: {s['status']['depth']}")
        st.write(f"Sursa: {s['status']['source']}")
    render(s)
    time.sleep(refresh)
    st.rerun()


if __name__ == "__main__":
    main()
