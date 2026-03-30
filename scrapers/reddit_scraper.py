"""Reddit scraper using PRAW to capture community sentiment for Pokemon cards."""

import logging
import os
import re
import time
from datetime import datetime, timezone

import praw
from dotenv import load_dotenv
from rapidfuzz import fuzz

from db import get_connection

load_dotenv()
logger = logging.getLogger(__name__)

TARGET_SUBREDDITS = [
    "PokemonTCG",
    "pkmntcgdeals",
    "PokemonCardValue",
    "pokemoncardcollectors",
]

# Common card name aliases for fuzzy matching
CARD_ALIASES = {
    "zard": "Charizard",
    "char": "Charizard",
    "base zard": "Charizard Base Set",
    "shadowless zard": "Charizard Base Set",
    "blastoise": "Blastoise",
    "venusaur": "Venusaur",
    "umbreon": "Umbreon VMAX",
    "ray": "Rayquaza VMAX",
    "lugia": "Lugia",
    "ho-oh": "Ho-Oh",
    "pika vmax": "Pikachu VMAX",
}


def get_reddit_client() -> praw.Reddit:
    """Create and return a PRAW Reddit client."""
    return praw.Reddit(
        client_id=os.getenv("REDDIT_CLIENT_ID"),
        client_secret=os.getenv("REDDIT_CLIENT_SECRET"),
        user_agent=os.getenv("REDDIT_USER_AGENT", "psa10-dashboard/1.0"),
    )


def get_watchlist_cards() -> list[dict]:
    """Fetch all active cards from the watchlist."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, set_name, card_number FROM cards WHERE is_active = 1")
    cards = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return cards


def match_card_in_text(text: str, watchlist: list[dict]) -> list[dict]:
    """Match card names in text against the watchlist using fuzzy matching."""
    text_lower = text.lower()
    matched = []

    # Check aliases first
    for alias, card_name in CARD_ALIASES.items():
        if alias in text_lower:
            for card in watchlist:
                if card["name"].lower() in card_name.lower():
                    if card not in matched:
                        matched.append(card)

    # Direct name matching with fuzzy threshold
    for card in watchlist:
        card_name_lower = card["name"].lower()
        # Exact substring match
        if card_name_lower in text_lower:
            if card not in matched:
                matched.append(card)
            continue
        # Fuzzy match on card name + set
        full_name = f"{card['name']} {card['set_name']}".lower()
        # Check each word-window of the text
        words = text_lower.split()
        for i in range(len(words)):
            window = " ".join(words[i : i + 4])
            if fuzz.partial_ratio(card_name_lower, window) > 85:
                if card not in matched:
                    matched.append(card)
                break

    return matched


def scrape_subreddit(reddit: praw.Reddit, subreddit_name: str,
                     watchlist: list[dict], limit: int = 50) -> list[dict]:
    """Scrape a subreddit for posts mentioning watched cards."""
    posts = []
    try:
        subreddit = reddit.subreddit(subreddit_name)
        for submission in subreddit.new(limit=limit):
            text = f"{submission.title} {submission.selftext or ''}"
            matched_cards = match_card_in_text(text, watchlist)

            if not matched_cards:
                continue

            for card in matched_cards:
                posts.append({
                    "card_id": card["id"],
                    "card_name": card["name"],
                    "subreddit": subreddit_name,
                    "post_id": submission.id,
                    "post_title": submission.title,
                    "post_body": (submission.selftext or "")[:500],
                    "score": submission.score,
                    "upvote_ratio": submission.upvote_ratio,
                    "num_comments": submission.num_comments,
                    "created_utc": datetime.fromtimestamp(
                        submission.created_utc, tz=timezone.utc
                    ),
                })
    except Exception as e:
        logger.error("Error scraping r/%s: %s", subreddit_name, e)

    return posts


def scrape_all_subreddits(limit_per_sub: int = 50) -> list[dict]:
    """Scrape all target subreddits for card mentions."""
    reddit = get_reddit_client()
    watchlist = get_watchlist_cards()

    if not watchlist:
        logger.warning("No active cards in watchlist. Skipping Reddit scrape.")
        return []

    all_posts = []
    for sub in TARGET_SUBREDDITS:
        logger.info("Scraping r/%s ...", sub)
        posts = scrape_subreddit(reddit, sub, watchlist, limit=limit_per_sub)
        all_posts.extend(posts)
        time.sleep(1)  # Be polite between subreddits

    logger.info("Collected %d total Reddit mentions.", len(all_posts))
    return all_posts


def store_reddit_mentions(posts: list[dict]):
    """Store Reddit mentions in the database (deduplicating by post_id)."""
    conn = get_connection()
    cursor = conn.cursor()
    inserted = 0
    for post in posts:
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO reddit_mentions
                   (card_id, post_id, subreddit, post_title, score, upvote_ratio,
                    num_comments, sentiment_label, sentiment_score, created_utc)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    post["card_id"],
                    post["post_id"],
                    post["subreddit"],
                    post["post_title"],
                    post["score"],
                    post["upvote_ratio"],
                    post["num_comments"],
                    post.get("sentiment_label"),
                    post.get("sentiment_score"),
                    post["created_utc"].isoformat(),
                ),
            )
            inserted += cursor.rowcount
        except Exception as e:
            logger.error("Error storing Reddit mention %s: %s", post["post_id"], e)
    conn.commit()
    conn.close()
    logger.info("Stored %d new Reddit mentions.", inserted)
    return inserted
