"""SQLite database operations for IT MSP lead discovery."""

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any


class Database:
    """SQLite database for IT MSP discovery data."""

    SCHEMA = """
    -- Companies found from SerpAPI job listings
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        domain TEXT,
        website TEXT,
        industry TEXT,
        employee_count INTEGER,
        employee_count_verified INTEGER DEFAULT 0,
        first_seen_date TEXT,
        last_seen_date TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    -- Job postings found via SerpAPI
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        job_url TEXT,
        location TEXT,
        posting_date TEXT,
        posted_at_raw TEXT,
        source TEXT,
        description_snippet TEXT,
        search_metro TEXT,
        discovered_at TEXT DEFAULT (datetime('now')),
        is_active INTEGER DEFAULT 1,
        FOREIGN KEY (company_id) REFERENCES companies(id)
    );

    -- Decision maker contacts found via Gemini lookup
    CREATE TABLE IF NOT EXISTS decision_makers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL UNIQUE,
        person_name TEXT NOT NULL,
        title TEXT,
        source_url TEXT,
        confidence TEXT,
        looked_up_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (company_id) REFERENCES companies(id)
    );

    -- Dedup: tracks company+title combos already stored
    CREATE TABLE IF NOT EXISTS seen_listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dedup_key TEXT UNIQUE NOT NULL,
        company_name TEXT NOT NULL,
        job_title TEXT NOT NULL,
        run_id TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );

    -- Run history
    CREATE TABLE IF NOT EXISTS run_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        run_date TEXT NOT NULL,
        searches_used INTEGER,
        raw_listings INTEGER,
        unique_listings INTEGER,
        companies_stored INTEGER,
        decision_makers_found INTEGER
    );

    -- Indexes
    CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);
    CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);
    CREATE INDEX IF NOT EXISTS idx_dm_company ON decision_makers(company_id);
    CREATE INDEX IF NOT EXISTS idx_seen_dedup ON seen_listings(dedup_key);
    CREATE INDEX IF NOT EXISTS idx_companies_name ON companies(name);
    """

    def __init__(self, db_path: Path):
        """Initialize database connection."""
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        """Create database schema."""
        self.conn.executescript(self.SCHEMA)
        self.conn.commit()

    def close(self):
        """Close database connection."""
        self.conn.close()

    # Company operations

    def upsert_company(self, company: Dict, run_id: str = None) -> int:
        """
        Insert or update a company by name. Returns company ID.

        Since SerpAPI doesn't provide domains, companies are matched by
        normalized name.
        """
        cursor = self.conn.cursor()
        today = datetime.now().date().isoformat()
        now = datetime.now().isoformat()

        # Check if exists by name (case-insensitive)
        cursor.execute(
            "SELECT id FROM companies WHERE LOWER(name) = LOWER(?)",
            (company["name"],),
        )
        row = cursor.fetchone()

        if row:
            cursor.execute(
                """
                UPDATE companies SET last_seen_date = ?, updated_at = ?
                WHERE id = ?
                """,
                (today, now, row["id"]),
            )
            self.conn.commit()
            return row["id"]
        else:
            cursor.execute(
                """
                INSERT INTO companies (name, first_seen_date, last_seen_date)
                VALUES (?, ?, ?)
                """,
                (company["name"], today, today),
            )
            self.conn.commit()
            return cursor.lastrowid

    def update_company_enrichment(
        self,
        company_id: int,
        employee_count: Optional[int] = None,
        industry: Optional[str] = None,
        domain: Optional[str] = None,
        website: Optional[str] = None,
    ):
        """Update company with enrichment data from Gemini."""
        updates = {}
        if employee_count is not None:
            updates["employee_count"] = employee_count
            updates["employee_count_verified"] = 1
        if industry:
            updates["industry"] = industry
        if domain:
            updates["domain"] = domain
        if website:
            updates["website"] = website

        if not updates:
            return

        updates["updated_at"] = datetime.now().isoformat()
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [company_id]
        self.conn.execute(
            f"UPDATE companies SET {set_clause} WHERE id = ?",
            values,
        )
        self.conn.commit()

    # Job operations

    def insert_job(self, job: Dict, company_id: int) -> int:
        """Insert a new job listing."""
        now = datetime.now().isoformat()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO jobs (
                company_id, title, job_url, location, posting_date,
                posted_at_raw, source, description_snippet, search_metro,
                discovered_at, is_active
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                company_id,
                job.get("title"),
                job.get("job_url"),
                job.get("location"),
                job.get("posting_date"),
                job.get("posted_at_raw"),
                job.get("source"),
                job.get("description_snippet"),
                job.get("search_metro"),
                now,
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    # Decision maker operations

    def upsert_decision_maker(
        self,
        company_id: int,
        person_name: str,
        title: Optional[str] = None,
        source_url: Optional[str] = None,
        confidence: Optional[str] = None,
    ) -> int:
        """Insert or update decision maker for a company."""
        now = datetime.now().isoformat()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO decision_makers (
                company_id, person_name, title, source_url, confidence,
                looked_up_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id) DO UPDATE SET
                person_name = excluded.person_name,
                title = excluded.title,
                source_url = excluded.source_url,
                confidence = excluded.confidence,
                updated_at = excluded.updated_at
            """,
            (company_id, person_name, title, source_url, confidence, now, now),
        )
        self.conn.commit()
        return cursor.lastrowid

    def get_decision_maker_for_company(self, company_id: int) -> Optional[Dict]:
        """Get decision maker info for a company."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM decision_makers WHERE company_id = ?",
            (company_id,),
        )
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_companies_needing_decision_makers(self) -> List[Dict]:
        """Get companies that don't have a decision maker entry yet."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT c.id, c.name, c.domain
            FROM companies c
            LEFT JOIN decision_makers dm ON dm.company_id = c.id
            WHERE dm.id IS NULL
            ORDER BY c.last_seen_date DESC
            """
        )
        return [dict(row) for row in cursor.fetchall()]

    # Seen listings tracking (dedup across runs)

    def is_listing_seen(self, dedup_key: str) -> bool:
        """Check if a listing has been seen in any previous run."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT 1 FROM seen_listings WHERE dedup_key = ?", (dedup_key,)
        )
        return cursor.fetchone() is not None

    def mark_listing_seen(
        self, dedup_key: str, company_name: str, job_title: str, run_id: str
    ):
        """Mark a listing as seen."""
        self.conn.execute(
            """
            INSERT OR IGNORE INTO seen_listings
            (dedup_key, company_name, job_title, run_id)
            VALUES (?, ?, ?, ?)
            """,
            (dedup_key, company_name, job_title, run_id),
        )
        self.conn.commit()

    # Run snapshots

    def record_run_snapshot(
        self,
        run_id: str,
        searches_used: int,
        raw_listings: int,
        unique_listings: int,
        companies_stored: int,
        decision_makers_found: int,
    ):
        """Store a run summary."""
        today = datetime.now().date().isoformat()
        self.conn.execute(
            """
            INSERT INTO run_snapshots (
                run_id, run_date, searches_used, raw_listings,
                unique_listings, companies_stored, decision_makers_found
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id, today, searches_used, raw_listings,
                unique_listings, companies_stored, decision_makers_found,
            ),
        )
        self.conn.commit()

    # Statistics

    def get_statistics(self) -> Dict[str, Any]:
        """Get overall statistics."""
        cursor = self.conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM companies")
        total_companies = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM jobs WHERE is_active = 1")
        active_jobs = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM decision_makers")
        total_dm = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM seen_listings")
        total_seen = cursor.fetchone()[0]

        # Companies by industry
        cursor.execute(
            """
            SELECT industry, COUNT(*)
            FROM companies
            WHERE industry IS NOT NULL AND industry != ''
            GROUP BY industry
            ORDER BY COUNT(*) DESC
            """
        )
        by_industry = {row[0]: row[1] for row in cursor.fetchall()}

        # Last run
        cursor.execute(
            """
            SELECT run_date, searches_used, unique_listings, companies_stored,
                   decision_makers_found
            FROM run_snapshots
            ORDER BY id DESC LIMIT 1
            """
        )
        last_run = cursor.fetchone()

        return {
            "total_companies": total_companies,
            "active_jobs": active_jobs,
            "total_decision_makers": total_dm,
            "total_seen_listings": total_seen,
            "by_industry": by_industry,
            "last_run": dict(last_run) if last_run else None,
        }

    # Upload query

    def get_companies_for_upload(self, max_employee_count: int = 100) -> List[Dict]:
        """
        Get companies with active jobs for upload.

        Returns companies with employee_count <= max_employee_count
        (or employee_count IS NULL), along with their jobs and decision makers.
        """
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT
                c.id,
                c.name as company_name,
                c.domain,
                c.website,
                c.employee_count,
                c.employee_count_verified,
                c.industry,
                c.first_seen_date,
                dm.person_name,
                dm.title as dm_title,
                dm.source_url,
                dm.confidence,
                (SELECT MAX(j.posting_date) FROM jobs j
                 WHERE j.company_id = c.id AND j.is_active = 1) as most_recent_posting
            FROM companies c
            LEFT JOIN decision_makers dm ON dm.company_id = c.id
            WHERE (c.employee_count IS NULL OR c.employee_count <= ?)
              AND EXISTS (
                SELECT 1 FROM jobs j
                WHERE j.company_id = c.id AND j.is_active = 1
                  AND j.posting_date >= date('now', '-7 days')
              )
            ORDER BY most_recent_posting DESC
            """,
            (max_employee_count,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_jobs_for_company(self, company_id: int) -> List[Dict]:
        """Get active jobs for a company, posted within last 7 days."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT title, job_url, location, posting_date, source
            FROM jobs
            WHERE company_id = ? AND is_active = 1
              AND posting_date >= date('now', '-7 days')
            ORDER BY posting_date DESC
            """,
            (company_id,),
        )
        return [dict(row) for row in cursor.fetchall()]
