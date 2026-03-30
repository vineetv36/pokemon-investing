"""Sentiment analysis for Reddit posts using Hugging Face transformers."""

import logging
import re
from datetime import date, timedelta

from db import get_connection

logger = logging.getLogger(__name__)

# Lazy-load the pipeline to avoid slow startup
_pipeline = None


def _get_pipeline():
    """Load the sentiment analysis pipeline (cached after first call)."""
    global _pipeline
    if _pipeline is None:
        from transformers import pipeline as hf_pipeline

        logger.info("Loading sentiment model (first time may download ~500MB)...")
        _pipeline = hf_pipeline(
            "sentiment-analysis",
            model="cardiffnlp/twitter-roberta-base-sentiment-latest",
            return_all_scores=True,
        )
        logger.info("Sentiment model loaded.")
    return _pipeline


def preprocess_text(text: str) -> str:
    """Clean text for sentiment analysis."""
    # Strip URLs
    text = re.sub(r"https?://\S+", "", text)
    # Strip Reddit markdown
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)  # bold
    text = re.sub(r"__(.+?)__", r"\1", text)  # underline
    text = re.sub(r"~~(.+?)~~", r"\1", text)  # strikethrough
    text = re.sub(r"^>.*$", "", text, flags=re.MULTILINE)  # quotes
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", text)  # links
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    # Truncate to ~512 tokens (rough estimate: 4 chars per token)
    return text[:2048]


def analyze_sentiment(text: str) -> dict:
    """
    Analyze sentiment of a text string.

    Returns:
        {"label": "positive"|"neutral"|"negative", "score": 0.0-1.0}
    """
    pipe = _get_pipeline()
    cleaned = preprocess_text(text)
    if not cleaned:
        return {"label": "neutral", "score": 0.0}

    results = pipe(cleaned)[0]
    # Model returns LABEL_0=negative, LABEL_1=neutral, LABEL_2=positive
    label_map = {
        "LABEL_0": "negative",
        "LABEL_1": "neutral",
        "LABEL_2": "positive",
        "negative": "negative",
        "neutral": "neutral",
        "positive": "positive",
    }

    best = max(results, key=lambda x: x["score"])
    label = label_map.get(best["label"], best["label"])

    # Convert to -1.0 to 1.0 scale
    score_map = {"negative": -1.0, "neutral": 0.0, "positive": 1.0}
    weighted_score = score_map.get(label, 0.0) * best["score"]

    return {"label": label, "score": weighted_score}


def analyze_posts(posts: list[dict]) -> list[dict]:
    """Analyze sentiment for a batch of Reddit posts."""
    for post in posts:
        text = f"{post['post_title']} {post.get('post_body', '')[:200]}"
        result = analyze_sentiment(text)
        post["sentiment_label"] = result["label"]
        post["sentiment_score"] = result["score"]
    return posts


def compute_daily_sentiment(card_id: int, target_date: date | None = None):
    """Compute weighted daily sentiment for a card and store it."""
    if target_date is None:
        target_date = date.today()

    conn = get_connection()
    cursor = conn.cursor()

    # Get all mentions for this card on this date
    cursor.execute(
        """SELECT sentiment_score, score as upvotes FROM reddit_mentions
           WHERE card_id = ? AND DATE(created_utc) = ?
           AND sentiment_score IS NOT NULL""",
        (card_id, target_date.isoformat()),
    )
    rows = cursor.fetchall()

    if not rows:
        conn.close()
        return None

    # Weighted sentiment: weight by upvotes (min 1 to avoid zero weights)
    total_weight = sum(max(row["upvotes"], 1) for row in rows)
    weighted_sentiment = sum(
        row["sentiment_score"] * max(row["upvotes"], 1) for row in rows
    ) / total_weight

    mention_count = len(rows)

    cursor.execute(
        """INSERT OR REPLACE INTO daily_sentiment (card_id, weighted_sentiment, mention_count, recorded_date)
           VALUES (?, ?, ?, ?)""",
        (card_id, weighted_sentiment, mention_count, target_date.isoformat()),
    )
    conn.commit()
    conn.close()

    logger.info(
        "Card %d: daily sentiment = %.3f (%d mentions) on %s",
        card_id, weighted_sentiment, mention_count, target_date,
    )
    return weighted_sentiment


def get_sentiment_momentum(card_id: int) -> dict:
    """Calculate 7-day sentiment average and momentum."""
    conn = get_connection()
    cursor = conn.cursor()
    today = date.today()
    week_ago = today - timedelta(days=7)
    two_weeks_ago = today - timedelta(days=14)

    # Current 7-day average
    cursor.execute(
        """SELECT AVG(weighted_sentiment) as avg_sent, SUM(mention_count) as total_mentions
           FROM daily_sentiment
           WHERE card_id = ? AND recorded_date >= ?""",
        (card_id, week_ago.isoformat()),
    )
    current = cursor.fetchone()

    # Previous 7-day average
    cursor.execute(
        """SELECT AVG(weighted_sentiment) as avg_sent
           FROM daily_sentiment
           WHERE card_id = ? AND recorded_date >= ? AND recorded_date < ?""",
        (card_id, two_weeks_ago.isoformat(), week_ago.isoformat()),
    )
    previous = cursor.fetchone()

    conn.close()

    current_avg = current["avg_sent"] or 0.0
    previous_avg = previous["avg_sent"] or 0.0
    change = current_avg - previous_avg

    return {
        "sentiment_7d_avg": current_avg,
        "sentiment_change": change,
        "mention_count_7d": current["total_mentions"] or 0,
        "rising": change > 0.15,
    }
