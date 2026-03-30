"""Tests for scrapers, API clients, and analysis modules."""

import os
import sys
import unittest
from datetime import date, datetime, timezone
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scrapers.point130_scraper import _should_skip_listing, _is_psa10_listing, _is_raw_listing, _filter_outliers_iqr
from scrapers.reddit_scraper import match_card_in_text, CARD_ALIASES
from analysis.sentiment import preprocess_text


class TestPoint130Scraper(unittest.TestCase):

    def test_skip_lot_sales(self):
        self.assertTrue(_should_skip_listing("Pokemon Card Lot 50 cards"))
        self.assertTrue(_should_skip_listing("Base Set Bundle with Charizard"))

    def test_skip_non_psa_graded(self):
        self.assertTrue(_should_skip_listing("Charizard BGS 10 Pristine"))
        self.assertTrue(_should_skip_listing("Charizard CGC 10 Perfect"))

    def test_dont_skip_normal_listings(self):
        self.assertFalse(_should_skip_listing("Charizard Base Set PSA 10"))
        self.assertFalse(_should_skip_listing("Charizard Base Set NM Raw"))

    def test_is_psa10_listing(self):
        self.assertTrue(_is_psa10_listing("Charizard Base Set PSA 10"))
        self.assertTrue(_is_psa10_listing("Charizard PSA10 Gem Mint"))
        self.assertFalse(_is_psa10_listing("Charizard PSA 9 Mint"))
        self.assertFalse(_is_psa10_listing("Charizard Raw NM"))

    def test_is_raw_listing(self):
        self.assertTrue(_is_raw_listing("Charizard Base Set NM"))
        self.assertTrue(_is_raw_listing("Charizard Near Mint Raw"))
        self.assertFalse(_is_raw_listing("Charizard PSA 10"))

    def test_filter_outliers_iqr(self):
        prices = [10, 12, 11, 13, 12, 100, 11, 12]  # 100 is outlier
        filtered = _filter_outliers_iqr(prices)
        self.assertNotIn(100, filtered)
        self.assertIn(12, filtered)

    def test_filter_outliers_small_list(self):
        prices = [10, 100, 12]
        # With < 4 items, returns as-is
        self.assertEqual(_filter_outliers_iqr(prices), prices)


class TestRedditScraper(unittest.TestCase):

    def setUp(self):
        self.watchlist = [
            {"id": 1, "name": "Charizard", "set_name": "Base Set", "card_number": "4/102"},
            {"id": 2, "name": "Blastoise", "set_name": "Base Set", "card_number": "2/102"},
            {"id": 3, "name": "Umbreon VMAX", "set_name": "Evolving Skies", "card_number": "215/203"},
        ]

    def test_exact_match(self):
        matches = match_card_in_text("Just pulled a Charizard from a pack!", self.watchlist)
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["name"], "Charizard")

    def test_alias_match(self):
        matches = match_card_in_text("Got a base zard for cheap", self.watchlist)
        self.assertTrue(any(m["name"] == "Charizard" for m in matches))

    def test_no_match(self):
        matches = match_card_in_text("Just bought some card sleeves", self.watchlist)
        self.assertEqual(len(matches), 0)

    def test_multiple_matches(self):
        matches = match_card_in_text("Charizard vs Blastoise, which is better?", self.watchlist)
        names = [m["name"] for m in matches]
        self.assertIn("Charizard", names)
        self.assertIn("Blastoise", names)


class TestSentiment(unittest.TestCase):

    def test_preprocess_strips_urls(self):
        text = "Check this out https://example.com/card amazing card"
        result = preprocess_text(text)
        self.assertNotIn("https://", result)
        self.assertIn("amazing card", result)

    def test_preprocess_strips_markdown(self):
        text = "**Bold** text and ~~strikethrough~~ and > quote"
        result = preprocess_text(text)
        self.assertIn("Bold", result)
        self.assertNotIn("**", result)
        self.assertNotIn("~~", result)

    def test_preprocess_truncates(self):
        text = "a" * 5000
        result = preprocess_text(text)
        self.assertLessEqual(len(result), 2048)

    def test_preprocess_strips_links(self):
        text = "Check [this link](https://example.com) out"
        result = preprocess_text(text)
        self.assertIn("this link", result)
        self.assertNotIn("https://", result)


class TestDatabase(unittest.TestCase):

    def test_init_db(self):
        """Test that database initialization works."""
        from db import init_db, get_connection, get_db_path
        # Use a temp database
        os.environ["DATABASE_URL"] = "sqlite:///./test_dashboard.db"
        try:
            # Reimport to pick up new env
            import importlib
            import db
            importlib.reload(db)
            db.init_db()
            conn = db.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row["name"] for row in cursor.fetchall()]
            conn.close()
            self.assertIn("cards", tables)
            self.assertIn("psa10_sales", tables)
            self.assertIn("price_ratios", tables)
            self.assertIn("reddit_mentions", tables)
        finally:
            if os.path.exists("test_dashboard.db"):
                os.remove("test_dashboard.db")
            os.environ["DATABASE_URL"] = "sqlite:///./dashboard.db"


if __name__ == "__main__":
    unittest.main()
