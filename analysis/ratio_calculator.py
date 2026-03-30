"""PSA 10 / Raw price ratio calculations and momentum scoring."""

import logging
from datetime import date, timedelta

from db import get_connection
from analysis.sentiment import get_sentiment_momentum

logger = logging.getLogger(__name__)


def calculate_psa10_rolling_avg(card_id: int, target_date: date | None = None,
                                 window_days: int = 30) -> dict | None:
    """Calculate rolling average PSA 10 price from recent sales."""
    if target_date is None:
        target_date = date.today()

    conn = get_connection()
    cursor = conn.cursor()
    start_date = target_date - timedelta(days=window_days)

    cursor.execute(
        """SELECT AVG(sale_price) as avg_price, MIN(sale_price) as min_price,
                  MAX(sale_price) as max_price, COUNT(*) as sale_count
           FROM psa10_sales
           WHERE card_id = ? AND sale_date >= ? AND sale_date <= ?""",
        (card_id, start_date.isoformat(), target_date.isoformat()),
    )
    row = cursor.fetchone()

    if not row or row["sale_count"] == 0:
        conn.close()
        return None

    result = {
        "avg_price": round(row["avg_price"], 2),
        "min_price": round(row["min_price"], 2),
        "max_price": round(row["max_price"], 2),
        "sale_count": row["sale_count"],
        "low_confidence": row["sale_count"] < 3,
    }

    # Store in psa10_prices table
    cursor.execute(
        """INSERT OR REPLACE INTO psa10_prices
           (card_id, avg_price, min_price, max_price, sale_count, recorded_date)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (card_id, result["avg_price"], result["min_price"],
         result["max_price"], result["sale_count"], target_date.isoformat()),
    )
    conn.commit()
    conn.close()

    return result


def get_latest_raw_price(card_id: int) -> float | None:
    """Get the most recent raw NM price for a card."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT price FROM raw_prices WHERE card_id = ? ORDER BY recorded_date DESC LIMIT 1",
        (card_id,),
    )
    row = cursor.fetchone()
    conn.close()
    return row["price"] if row else None


def calculate_ratio(card_id: int, target_date: date | None = None) -> dict | None:
    """Calculate PSA 10 / raw price ratio and changes over time."""
    if target_date is None:
        target_date = date.today()

    psa10 = calculate_psa10_rolling_avg(card_id, target_date)
    raw_price = get_latest_raw_price(card_id)

    if not psa10 or not raw_price or raw_price == 0:
        return None

    ratio = round(psa10["avg_price"] / raw_price, 2)

    conn = get_connection()
    cursor = conn.cursor()

    # Get ratio from 7 days ago
    cursor.execute(
        "SELECT ratio FROM price_ratios WHERE card_id = ? AND recorded_date = ?",
        (card_id, (target_date - timedelta(days=7)).isoformat()),
    )
    row_7d = cursor.fetchone()
    ratio_7d_change = round(ratio - row_7d["ratio"], 2) if row_7d else None

    # Get ratio from 30 days ago
    cursor.execute(
        "SELECT ratio FROM price_ratios WHERE card_id = ? AND recorded_date = ?",
        (card_id, (target_date - timedelta(days=30)).isoformat()),
    )
    row_30d = cursor.fetchone()
    ratio_30d_change = round(ratio - row_30d["ratio"], 2) if row_30d else None

    # Store ratio
    cursor.execute(
        """INSERT OR REPLACE INTO price_ratios
           (card_id, psa10_price, raw_price, ratio, ratio_7d_change, ratio_30d_change, recorded_date)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (card_id, psa10["avg_price"], raw_price, ratio,
         ratio_7d_change, ratio_30d_change, target_date.isoformat()),
    )
    conn.commit()
    conn.close()

    return {
        "psa10_price": psa10["avg_price"],
        "raw_price": raw_price,
        "ratio": ratio,
        "ratio_7d_change": ratio_7d_change,
        "ratio_30d_change": ratio_30d_change,
        "low_confidence": psa10["low_confidence"],
    }


def calculate_sales_velocity(card_id: int) -> float:
    """Compare 30-day PSA 10 sale count vs prior 30 days."""
    conn = get_connection()
    cursor = conn.cursor()
    today = date.today()

    cursor.execute(
        "SELECT COUNT(*) as cnt FROM psa10_sales WHERE card_id = ? AND sale_date >= ?",
        (card_id, (today - timedelta(days=30)).isoformat()),
    )
    recent = cursor.fetchone()["cnt"]

    cursor.execute(
        """SELECT COUNT(*) as cnt FROM psa10_sales
           WHERE card_id = ? AND sale_date >= ? AND sale_date < ?""",
        (card_id, (today - timedelta(days=60)).isoformat(),
         (today - timedelta(days=30)).isoformat()),
    )
    prior = cursor.fetchone()["cnt"]
    conn.close()

    if prior == 0:
        return 1.0 if recent > 0 else 0.0
    return (recent - prior) / prior


def _normalize_score(value: float, min_val: float, max_val: float) -> float:
    """Normalize a value to 0-100 range."""
    if max_val == min_val:
        return 50.0
    return max(0, min(100, (value - min_val) / (max_val - min_val) * 100))


def calculate_momentum_score(card_id: int) -> dict:
    """
    Calculate composite momentum score (0-100) for a card.

    Weights:
    - Ratio momentum: 40%
    - Sales velocity: 35%
    - Sentiment: 25%
    """
    ratio_data = calculate_ratio(card_id)
    velocity = calculate_sales_velocity(card_id)
    sentiment = get_sentiment_momentum(card_id)

    # Ratio momentum score (0-100)
    if ratio_data and ratio_data["ratio_7d_change"] is not None:
        ratio_score = _normalize_score(ratio_data["ratio_7d_change"], -1.0, 1.0)
    else:
        ratio_score = 50.0  # neutral if no data

    # Sales velocity score (0-100)
    velocity_score = _normalize_score(velocity, -0.5, 1.5)

    # Sentiment score (0-100)
    sentiment_score = _normalize_score(sentiment["sentiment_7d_avg"], -1.0, 1.0)

    # Composite
    momentum = (
        ratio_score * 0.40
        + velocity_score * 0.35
        + sentiment_score * 0.25
    )

    # Classification
    if momentum >= 70:
        classification = "strong_momentum"
        badge = "Strong momentum"
    elif momentum >= 50:
        classification = "moderate_momentum"
        badge = "Moderate momentum"
    elif momentum >= 30:
        classification = "watch"
        badge = "Watch list"
    else:
        classification = "neutral"
        badge = "Neutral / cooling"

    return {
        "momentum_score": round(momentum, 1),
        "ratio_score": round(ratio_score, 1),
        "velocity_score": round(velocity_score, 1),
        "sentiment_score": round(sentiment_score, 1),
        "classification": classification,
        "badge": badge,
        "ratio_data": ratio_data,
        "sentiment_data": sentiment,
        "low_confidence": (ratio_data or {}).get("low_confidence", True),
    }
