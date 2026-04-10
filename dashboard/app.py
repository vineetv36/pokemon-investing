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
def api_movers(sort: str = "raw_30d", limit: int = 50):
    """Get biggest price movers — raw, PSA 10, and ratio changes.

    Uses each card's own latest date (not a global max), and finds
    the closest available date within a window for 7d/30d comparisons.
    """
    conn = get_connection()
    cursor = conn.cursor()

    # Get each card with its latest raw price, plus the earliest price
    # within ~5-9 days ago and ~25-35 days ago for flexible date matching
    cursor.execute("""
        SELECT c.id, c.name, c.set_name, c.card_number,
            latest.price as raw_now,
            latest.recorded_date as latest_date,
            (SELECT rp2.price FROM raw_prices rp2
             WHERE rp2.card_id = c.id
               AND rp2.recorded_date BETWEEN date(latest.recorded_date, '-9 days') AND date(latest.recorded_date, '-5 days')
             ORDER BY rp2.recorded_date DESC LIMIT 1) as raw_7d_ago,
            (SELECT rp3.price FROM raw_prices rp3
             WHERE rp3.card_id = c.id
               AND rp3.recorded_date BETWEEN date(latest.recorded_date, '-35 days') AND date(latest.recorded_date, '-25 days')
             ORDER BY rp3.recorded_date DESC LIMIT 1) as raw_30d_ago,
            (SELECT p1.avg_price FROM psa10_prices p1
             WHERE p1.card_id = c.id
             ORDER BY p1.recorded_date DESC LIMIT 1) as psa10_now,
            (SELECT p2.avg_price FROM psa10_prices p2
             WHERE p2.card_id = c.id
               AND p2.recorded_date BETWEEN date(latest.recorded_date, '-9 days') AND date(latest.recorded_date, '-5 days')
             ORDER BY p2.recorded_date DESC LIMIT 1) as psa10_7d_ago,
            (SELECT p3.avg_price FROM psa10_prices p3
             WHERE p3.card_id = c.id
               AND p3.recorded_date BETWEEN date(latest.recorded_date, '-35 days') AND date(latest.recorded_date, '-25 days')
             ORDER BY p3.recorded_date DESC LIMIT 1) as psa10_30d_ago,
            (SELECT pr.ratio FROM price_ratios pr
             WHERE pr.card_id = c.id
             ORDER BY pr.recorded_date DESC LIMIT 1) as ratio,
            (SELECT pr.ratio_7d_change FROM price_ratios pr
             WHERE pr.card_id = c.id AND pr.ratio_7d_change IS NOT NULL
             ORDER BY pr.recorded_date DESC LIMIT 1) as ratio_7d_change,
            (SELECT pr.ratio_30d_change FROM price_ratios pr
             WHERE pr.card_id = c.id AND pr.ratio_30d_change IS NOT NULL
             ORDER BY pr.recorded_date DESC LIMIT 1) as ratio_30d_change
        FROM cards c
        JOIN (
            SELECT card_id, price, recorded_date,
                   ROW_NUMBER() OVER (PARTITION BY card_id ORDER BY recorded_date DESC) as rn
            FROM raw_prices
        ) latest ON c.id = latest.card_id AND latest.rn = 1
        WHERE c.is_active = 1 AND latest.price > 0
    """)

    movers = []
    for row in cursor.fetchall():
        r = dict(row)
        r["raw_7d_pct"] = round((r["raw_now"] - r["raw_7d_ago"]) / r["raw_7d_ago"] * 100, 1) if r["raw_7d_ago"] else None
        r["raw_30d_pct"] = round((r["raw_now"] - r["raw_30d_ago"]) / r["raw_30d_ago"] * 100, 1) if r["raw_30d_ago"] else None
        r["psa10_7d_pct"] = round((r["psa10_now"] - r["psa10_7d_ago"]) / r["psa10_7d_ago"] * 100, 1) if r["psa10_7d_ago"] else None
        r["psa10_30d_pct"] = round((r["psa10_now"] - r["psa10_30d_ago"]) / r["psa10_30d_ago"] * 100, 1) if r["psa10_30d_ago"] else None
        movers.append(r)

    conn.close()

    sort_key = {
        "raw_7d": "raw_7d_pct", "raw_30d": "raw_30d_pct",
        "psa10_7d": "psa10_7d_pct", "psa10_30d": "psa10_30d_pct",
        "ratio_30d": "ratio_30d_change",
    }.get(sort, "raw_30d_pct")
    movers.sort(key=lambda x: x.get(sort_key) or -9999, reverse=True)

    return {
        "movers": movers[:limit],
        "total_cards": len(movers),
    }


# --- HTML Pages ---


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    """Main dashboard page — redirects to movers since that's the primary view."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM cards WHERE is_active = 1")
    total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT card_id) FROM raw_prices")
    with_prices = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(DISTINCT card_id) FROM daily_sentiment")
    with_sentiment = cursor.fetchone()[0]

    # Only load top 50 cards for the overview table
    cursor.execute("""
        SELECT c.id, c.name, c.set_name, c.card_number
        FROM cards c
        JOIN raw_prices rp ON c.id = rp.card_id
            AND rp.recorded_date = (SELECT MAX(recorded_date) FROM raw_prices WHERE card_id = c.id)
        WHERE c.is_active = 1
        ORDER BY rp.price DESC
        LIMIT 50
    """)
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
    return templates.TemplateResponse(request, "index.html", {
        "cards": cards, "total": total, "with_prices": with_prices, "with_sentiment": with_sentiment,
    })


@app.get("/movers", response_class=HTMLResponse)
def movers_page(request: Request, sort: str = "raw_30d", limit: int = 50):
    """Momentum movers page — biggest price changes."""
    data = api_movers(sort=sort, limit=limit)
    return templates.TemplateResponse(request, "movers.html", {
        "movers": data["movers"],
        "total_cards": data["total_cards"],
        "sort": sort,
        "limit": limit,
    })


@app.get("/card/{card_id}", response_class=HTMLResponse)
def card_detail_page(request: Request, card_id: int):
    """Card detail page."""
    data = api_card_detail(card_id)
    return templates.TemplateResponse(request, "card_detail.html", {"card": data})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dashboard.app:app", host="0.0.0.0", port=8000, reload=True)
