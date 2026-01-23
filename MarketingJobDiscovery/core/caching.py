"""Caching utilities for job discovery."""

import sqlite3
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any


class ATSDetectionCache:
    """
    Cache ATS detection results in the database.

    TTL: 7 days (companies rarely change their ATS)
    """

    def __init__(self, db_connection: sqlite3.Connection, ttl_days: int = 7):
        self.conn = db_connection
        self.ttl_days = ttl_days

    def get(self, domain: str) -> Optional[Dict]:
        """Get cached ATS detection result."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT ats_provider, board_token, expires_at
            FROM ats_cache
            WHERE domain = ?
            """,
            (domain,),
        )

        row = cursor.fetchone()
        if not row:
            return None

        provider, token, expires_at = row
        expires = datetime.fromisoformat(expires_at)

        if datetime.now() > expires:
            cursor.execute("DELETE FROM ats_cache WHERE domain = ?", (domain,))
            self.conn.commit()
            return None

        return {"provider": provider, "board_token": token}

    def set(self, domain: str, provider: str, board_token: Optional[str]) -> None:
        """Cache ATS detection result."""
        now = datetime.now()
        expires = now + timedelta(days=self.ttl_days)

        self.conn.execute(
            """
            INSERT OR REPLACE INTO ats_cache
            (domain, ats_provider, board_token, detected_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (domain, provider, board_token, now.isoformat(), expires.isoformat()),
        )
        self.conn.commit()

    def clear(self, domain: Optional[str] = None) -> None:
        """Clear cache for a domain or all domains."""
        if domain:
            self.conn.execute("DELETE FROM ats_cache WHERE domain = ?", (domain,))
        else:
            self.conn.execute("DELETE FROM ats_cache")
        self.conn.commit()

    def clear_expired(self) -> int:
        """Remove expired cache entries."""
        cursor = self.conn.cursor()
        cursor.execute(
            "DELETE FROM ats_cache WHERE expires_at < ?",
            (datetime.now().isoformat(),),
        )
        count = cursor.rowcount
        self.conn.commit()
        return count


class SimpleHTTPCache:
    """
    Simple in-memory HTTP response cache.

    Used for caching API responses during a single run.
    """

    def __init__(self, default_ttl: int = 3600):
        """
        Initialize cache.

        Args:
            default_ttl: Default TTL in seconds (1 hour)
        """
        self.default_ttl = default_ttl
        self._cache: Dict[str, Dict[str, Any]] = {}

    def _make_key(self, method: str, url: str, params: Optional[Dict] = None) -> str:
        """Generate cache key from request."""
        key = f"{method}:{url}"
        if params:
            key += f":{json.dumps(params, sort_keys=True)}"
        return key

    def get(
        self, method: str, url: str, params: Optional[Dict] = None
    ) -> Optional[Any]:
        """Get cached response if valid."""
        key = self._make_key(method, url, params)
        entry = self._cache.get(key)

        if not entry:
            return None

        if datetime.now() > entry["expires_at"]:
            del self._cache[key]
            return None

        return entry["value"]

    def set(
        self,
        method: str,
        url: str,
        value: Any,
        params: Optional[Dict] = None,
        ttl: Optional[int] = None,
    ) -> None:
        """Cache a response."""
        key = self._make_key(method, url, params)
        ttl = ttl or self.default_ttl

        self._cache[key] = {
            "value": value,
            "expires_at": datetime.now() + timedelta(seconds=ttl),
        }

    def clear(self) -> None:
        """Clear all cache entries."""
        self._cache.clear()

    def clear_expired(self) -> int:
        """Remove expired entries."""
        now = datetime.now()
        expired_keys = [
            key for key, entry in self._cache.items() if now > entry["expires_at"]
        ]
        for key in expired_keys:
            del self._cache[key]
        return len(expired_keys)
