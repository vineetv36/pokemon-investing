"""FastAPI dashboard for PSA 10 Pokemon Card tracking."""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import date, timedelta

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from db import get_connection, init_db
from analysis.ratio_calculator import calculate_momentum_score

app = FastAPI(title="PSA 10 Pokemon Card Dashboard")
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "templates")
)


@app.on_event("startup")
def startup():
    init_db()


# --- API Endpoints ---


@app.get("/api/cards")
def api_cards():
    """Get all active cards with their latest metrics."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cards WHERE is_active = 1 ORDER BY name")
    cards = [dict(row) for row in cursor.fetchall()]

    for card in cards:
        card_id = card["id"]
        # Latest ratio
        cursor.execute(
            "SELECT * FROM price_ratios WHERE card_id = ? ORDER BY recorded_date DESC LIMIT 1",
            (card_id,),
        )
        ratio_row = cursor.fetchone()
        card["ratio"] = dict(ratio_row) if ratio_row else None

        # Latest sentiment
        cursor.execute(
            "SELECT * FROM daily_sentiment WHERE card_id = ? ORDER BY recorded_date DESC LIMIT 1",
            (card_id,),
        )
        sent_row = cursor.fetchone()
        card["sentiment"] = dict(sent_row) if sent_row else None

    conn.close()
    return {"cards": cards}


@app.get("/api/cards/{card_id}")
def api_card_detail(card_id: int):
    """Get detailed data for a single card."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM cards WHERE id = ?", (card_id,))
    card = cursor.fetchone()
    if not card:
        conn.close()
        return {"error": "Card not found"}
    card = dict(card)

    # Price ratio history (last 90 days)
    cursor.execute(
        """SELECT * FROM price_ratios WHERE card_id = ?
           ORDER BY recorded_date DESC LIMIT 90""",
        (card_id,),
    )
    card["ratio_history"] = [dict(r) for r in cursor.fetchall()]

    # PSA 10 price history
    cursor.execute(
        """SELECT * FROM psa10_prices WHERE card_id = ?
           ORDER BY recorded_date DESC LIMIT 90""",
        (card_id,),
    )
    card["psa10_history"] = [dict(r) for r in cursor.fetchall()]

    # Raw price history
    cursor.execute(
        """SELECT * FROM raw_prices WHERE card_id = ?
           ORDER BY recorded_date DESC LIMIT 90""",
        (card_id,),
    )
    card["raw_history"] = [dict(r) for r in cursor.fetchall()]

    # Sentiment history
    cursor.execute(
        """SELECT * FROM daily_sentiment WHERE card_id = ?
           ORDER BY recorded_date DESC LIMIT 30""",
        (card_id,),
    )
    card["sentiment_history"] = [dict(r) for r in cursor.fetchall()]

    # Recent PSA 10 sales
    cursor.execute(
        """SELECT * FROM psa10_sales WHERE card_id = ?
           ORDER BY sale_date DESC LIMIT 20""",
        (card_id,),
    )
    card["recent_sales"] = [dict(r) for r in cursor.fetchall()]

    # Reddit mentions
    cursor.execute(
        """SELECT * FROM reddit_mentions WHERE card_id = ?
           ORDER BY created_utc DESC LIMIT 20""",
        (card_id,),
    )
    card["reddit_mentions"] = [dict(r) for r in cursor.fetchall()]

    conn.close()
    return card


@app.get("/api/leaderboard")
def api_leaderboard():
    """Get all cards sorted by momentum score."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, set_name, card_number FROM cards WHERE is_active = 1")
    cards = [dict(row) for row in cursor.fetchall()]
    conn.close()

    results = []
    for card in cards:
        try:
            momentum = calculate_momentum_score(card["id"])
            results.append({**card, **momentum})
        except Exception:
            results.append({**card, "momentum_score": 0, "badge": "No data"})

    results.sort(key=lambda x: x["momentum_score"], reverse=True)
    return {"leaderboard": results}


@app.get("/api/movers")
def api_movers():
    """Get biggest price movers — raw, PSA 10, and ratio changes."""
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT c.id, c.name, c.set_name, c.card_number,
            pr.psa10_price, pr.raw_price, pr.ratio,
            pr.ratio_7d_change, pr.ratio_30d_change,

            rp_now.price as raw_now,
            rp_7d.price as raw_7d_ago,
            rp_30d.price as raw_30d_ago,

            p10_now.avg_price as psa10_now,
            p10_7d.avg_price as psa10_7d_ago,
            p10_30d.avg_price as psa10_30d_ago
        FROM cards c
        LEFT JOIN price_ratios pr ON c.id = pr.card_id
            AND pr.recorded_date = (SELECT MAX(recorded_date) FROM price_ratios)
        LEFT JOIN raw_prices rp_now ON c.id = rp_now.card_id
            AND rp_now.recorded_date = (SELECT MAX(recorded_date) FROM raw_prices)
        LEFT JOIN raw_prices rp_7d ON c.id = rp_7d.card_id
            AND rp_7d.recorded_date = date((SELECT MAX(recorded_date) FROM raw_prices), '-7 days')
        LEFT JOIN raw_prices rp_30d ON c.id = rp_30d.card_id
            AND rp_30d.recorded_date = date((SELECT MAX(recorded_date) FROM raw_prices), '-30 days')
        LEFT JOIN psa10_prices p10_now ON c.id = p10_now.card_id
            AND p10_now.recorded_date = (SELECT MAX(recorded_date) FROM psa10_prices)
        LEFT JOIN psa10_prices p10_7d ON c.id = p10_7d.card_id
            AND p10_7d.recorded_date = date((SELECT MAX(recorded_date) FROM psa10_prices), '-7 days')
        LEFT JOIN psa10_prices p10_30d ON c.id = p10_30d.card_id
            AND p10_30d.recorded_date = date((SELECT MAX(recorded_date) FROM psa10_prices), '-30 days')
        WHERE c.is_active = 1
    """)

    movers = []
    for row in cursor.fetchall():
        r = dict(row)
        # Calculate percentage changes
        r["raw_7d_pct"] = round((r["raw_now"] - r["raw_7d_ago"]) / r["raw_7d_ago"] * 100, 1) if r["raw_7d_ago"] else None
        r["raw_30d_pct"] = round((r["raw_now"] - r["raw_30d_ago"]) / r["raw_30d_ago"] * 100, 1) if r["raw_30d_ago"] else None
        r["psa10_7d_pct"] = round((r["psa10_now"] - r["psa10_7d_ago"]) / r["psa10_7d_ago"] * 100, 1) if r["psa10_7d_ago"] else None
        r["psa10_30d_pct"] = round((r["psa10_now"] - r["psa10_30d_ago"]) / r["psa10_30d_ago"] * 100, 1) if r["psa10_30d_ago"] else None
        movers.append(r)

    conn.close()
    return {"movers": movers}


# --- HTML Pages ---


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    """Main dashboard page."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, set_name, card_number FROM cards WHERE is_active = 1 ORDER BY name")
    cards = [dict(row) for row in cursor.fetchall()]

    for card in cards:
        cursor.execute(
            "SELECT ratio, ratio_7d_change, ratio_30d_change, psa10_price, raw_price FROM price_ratios WHERE card_id = ? ORDER BY recorded_date DESC LIMIT 1",
            (card["id"],),
        )
        ratio = cursor.fetchone()
        card["ratio"] = dict(ratio) if ratio else {}

        cursor.execute(
            "SELECT weighted_sentiment, mention_count FROM daily_sentiment WHERE card_id = ? ORDER BY recorded_date DESC LIMIT 1",
            (card["id"],),
        )
        sent = cursor.fetchone()
        card["sentiment"] = dict(sent) if sent else {}

    conn.close()
    return templates.TemplateResponse(request, "index.html", {"cards": cards})


@app.get("/movers", response_class=HTMLResponse)
def movers_page(request: Request):
    """Momentum movers page — biggest price changes."""
    data = api_movers()
    leaderboard = api_leaderboard()
    return templates.TemplateResponse(request, "movers.html", {
        "movers": data["movers"],
        "leaderboard": leaderboard["leaderboard"],
    })


@app.get("/card/{card_id}", response_class=HTMLResponse)
def card_detail_page(request: Request, card_id: int):
    """Card detail page."""
    data = api_card_detail(card_id)
    return templates.TemplateResponse(request, "card_detail.html", {"card": data})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=8000, reload=True)
