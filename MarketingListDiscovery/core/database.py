"""SQLite database operations for list discovery."""

import sqlite3
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any, Tuple

from .models import Company, JobPosting


class Database:
    """SQLite database for list discovery data."""

    SCHEMA = """
    -- Companies discovered from GitHub listings
    CREATE TABLE IF NOT EXISTS companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        domain TEXT UNIQUE NOT NULL,
        website TEXT,
        industry TEXT,
        keywords TEXT,
        employee_count INTEGER,
        ats_provider TEXT,
        ats_board_token TEXT,
        careers_page_url TEXT,
        last_checked_at TEXT,
        urgency_score INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );

    -- Individual job postings
    CREATE TABLE IF NOT EXISTS jobs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_id INTEGER NOT NULL,
        external_id TEXT,
        title TEXT NOT NULL,
        category TEXT,
        department TEXT,
        location TEXT,
        description TEXT,
        job_url TEXT NOT NULL,
        posting_date TEXT,
        discovered_at TEXT DEFAULT (datetime('now')),
        last_seen_at TEXT DEFAULT (datetime('now')),
        is_active INTEGER DEFAULT 1,
        relevance_score REAL,
        FOREIGN KEY (company_id) REFERENCES companies(id),
        UNIQUE(company_id, external_id)
    );

    -- Tracking daily run snapshots for change detection
    CREATE TABLE IF NOT EXISTS run_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id TEXT NOT NULL,
        run_date TEXT NOT NULL,
        company_id INTEGER NOT NULL,
        jobs_found INTEGER,
        new_jobs INTEGER,
        removed_jobs INTEGER,
        status TEXT,
        error_message TEXT,
        FOREIGN KEY (company_id) REFERENCES companies(id)
    );

    -- Job state changes
    CREATE TABLE IF NOT EXISTS job_changes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_id INTEGER NOT NULL,
        run_id TEXT NOT NULL,
        change_type TEXT NOT NULL,
        changed_at TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (job_id) REFERENCES jobs(id)
    );

    -- ATS detection cache
    CREATE TABLE IF NOT EXISTS ats_cache (
        domain TEXT PRIMARY KEY,
        ats_provider TEXT,
        board_token TEXT,
        detected_at TEXT DEFAULT (datetime('now')),
        expires_at TEXT
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

    -- Tracks companies already processed so we skip them in future runs
    CREATE TABLE IF NOT EXISTS seen_companies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        domain TEXT UNIQUE NOT NULL,
        github_listing_date TEXT NOT NULL,
        run_id TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);
    CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);
    CREATE INDEX IF NOT EXISTS idx_jobs_relevance ON jobs(relevance_score);
    CREATE INDEX IF NOT EXISTS idx_changes_run ON job_changes(run_id);
    CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain);
    CREATE INDEX IF NOT EXISTS idx_dm_company ON decision_makers(company_id);
    CREATE INDEX IF NOT EXISTS idx_seen_domain ON seen_companies(domain);
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
        self._run_migrations()

    def _run_migrations(self):
        """Run schema migrations for existing databases."""
        cursor = self.conn.cursor()

        # Add email and linkedin_url columns to decision_makers if missing
        cursor.execute("PRAGMA table_info(decision_makers)")
        existing_cols = {row[1] for row in cursor.fetchall()}
        for col in [("email", "TEXT"), ("linkedin_url", "TEXT")]:
            if col[0] not in existing_cols:
                cursor.execute(
                    f"ALTER TABLE decision_makers ADD COLUMN {col[0]} {col[1]}"
                )

        # Add daily run tracking columns to companies table
        cursor.execute("PRAGMA table_info(companies)")
        company_cols = {row[1] for row in cursor.fetchall()}
        for col in [
            ("first_seen_date", "TEXT"),  # When company first appeared in any CSV
            ("last_csv_date", "TEXT"),  # Most recent CSV containing this company
            ("current_run_id", "TEXT"),  # Marks companies active in today's run
        ]:
            if col[0] not in company_cols:
                cursor.execute(
                    f"ALTER TABLE companies ADD COLUMN {col[0]} {col[1]}"
                )

        # Add verification_status column to jobs table
        cursor.execute("PRAGMA table_info(jobs)")
        job_cols = {row[1] for row in cursor.fetchall()}
        if "verification_status" not in job_cols:
            cursor.execute(
                "ALTER TABLE jobs ADD COLUMN verification_status TEXT DEFAULT 'unverified'"
            )

        self.conn.commit()

    def close(self):
        """Close database connection."""
        self.conn.close()

    # Company operations

    def upsert_company(self, company: Dict, run_id: str = None) -> Tuple[int, bool]:
        """
        Insert or update a company, return (ID, is_new_or_resurfacing).

        Args:
            company: Company data dict with name, domain, etc.
            run_id: Optional run ID to mark this company as part of today's run.

        Returns:
            Tuple of (company_id, should_process_ats) where should_process_ats
            is True if company is new or resurfacing (not seen in previous run).
        """
        cursor = self.conn.cursor()
        today = datetime.now().date().isoformat()
        now = datetime.now().isoformat()

        # Check if exists and get last_csv_date
        cursor.execute(
            "SELECT id, last_csv_date FROM companies WHERE domain = ?",
            (company["domain"],),
        )
        row = cursor.fetchone()

        if row:
            # Existing company - check if resurfacing (last seen on different day)
            last_csv_date = row["last_csv_date"]
            is_resurfacing = last_csv_date != today if last_csv_date else True

            # Update existing company with new CSV date and run_id
            cursor.execute(
                """
                UPDATE companies SET
                    name = ?, website = ?, industry = ?, keywords = ?,
                    employee_count = ?, last_csv_date = ?, current_run_id = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    company["name"],
                    company.get("website"),
                    company.get("industry"),
                    company.get("keywords"),
                    company.get("employee_count"),
                    today,
                    run_id,
                    now,
                    row["id"],
                ),
            )
            self.conn.commit()
            return row["id"], is_resurfacing
        else:
            # Insert new company with first_seen_date and last_csv_date
            cursor.execute(
                """
                INSERT INTO companies (
                    name, domain, website, industry, keywords, employee_count,
                    first_seen_date, last_csv_date, current_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company["name"],
                    company["domain"],
                    company.get("website"),
                    company.get("industry"),
                    company.get("keywords"),
                    company.get("employee_count"),
                    today,  # first_seen_date
                    today,  # last_csv_date
                    run_id,
                ),
            )
            self.conn.commit()
            return cursor.lastrowid, True  # New company = always process

    def update_company_ats(
        self, company_id: int, ats_provider: str, board_token: Optional[str]
    ):
        """Update company's ATS information."""
        now = datetime.now().isoformat()
        self.conn.execute(
            """
            UPDATE companies SET
                ats_provider = ?, ats_board_token = ?,
                last_checked_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (ats_provider, board_token, now, now, company_id),
        )
        self.conn.commit()

    def update_company_urgency(self, company_id: int, urgency_score: int):
        """Update company's urgency score (count of marketing jobs)."""
        now = datetime.now().isoformat()
        self.conn.execute(
            """
            UPDATE companies SET urgency_score = ?, updated_at = ?
            WHERE id = ?
            """,
            (urgency_score, now, company_id),
        )
        self.conn.commit()

    # Decision maker operations

    def upsert_decision_maker(
        self,
        company_id: int,
        person_name: str,
        title: Optional[str] = None,
        source_url: Optional[str] = None,
        confidence: Optional[str] = None,
        email: Optional[str] = None,
        linkedin_url: Optional[str] = None,
    ) -> int:
        """Insert or update decision maker for a company."""
        now = datetime.now().isoformat()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO decision_makers (
                company_id, person_name, title, source_url, confidence,
                email, linkedin_url, looked_up_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(company_id) DO UPDATE SET
                person_name = excluded.person_name,
                title = excluded.title,
                source_url = excluded.source_url,
                confidence = excluded.confidence,
                email = excluded.email,
                linkedin_url = excluded.linkedin_url,
                updated_at = excluded.updated_at
            """,
            (company_id, person_name, title, source_url, confidence, email, linkedin_url, now, now),
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

    def get_top_companies_by_urgency(self, limit: int = 10) -> List[Dict]:
        """Get top companies by marketing job urgency score."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT c.id, c.name, c.domain, c.urgency_score, c.ats_provider
            FROM companies c
            WHERE c.urgency_score > 0
            ORDER BY c.urgency_score DESC
            LIMIT ?
            """,
            (limit,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_companies_sorted_by_recency(self, limit: int = None) -> List[Dict]:
        """
        Get companies sorted by most recent job posting date.

        Sorting:
        1. Most recent job posting date (primary, descending)
        2. Number of active marketing roles (tiebreaker, descending)

        Returns companies with their most recent posting date and job count.
        Only includes companies with at least one active job.
        """
        cursor = self.conn.cursor()
        query = """
            SELECT c.*,
                   MAX(j.posting_date) as most_recent_posting,
                   COUNT(j.id) as active_job_count
            FROM companies c
            INNER JOIN jobs j ON j.company_id = c.id AND j.is_active = 1
            GROUP BY c.id
            ORDER BY most_recent_posting DESC, active_job_count DESC
        """
        if limit:
            query += f" LIMIT {limit}"
        cursor.execute(query)
        return [dict(row) for row in cursor.fetchall()]

    def get_jobs_for_company_by_id(self, company_id: int) -> List[Dict]:
        """Get all active jobs for a company by ID."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT title, category, relevance_score, job_url
            FROM jobs
            WHERE company_id = ? AND is_active = 1
            ORDER BY relevance_score DESC
            """,
            (company_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_company_by_domain(self, domain: str) -> Optional[Dict]:
        """Get company by domain."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM companies WHERE domain = ?", (domain,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_companies(self) -> List[Dict]:
        """Get all companies."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM companies ORDER BY name")
        return [dict(row) for row in cursor.fetchall()]

    # Job operations

    def get_active_jobs_for_company(self, company_id: int) -> List[Dict]:
        """Get all active jobs for a company."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT * FROM jobs
            WHERE company_id = ? AND is_active = 1
            ORDER BY relevance_score DESC
            """,
            (company_id,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def insert_job(self, job: Dict, company_id: int) -> int:
        """Insert a new job."""
        now = datetime.now().isoformat()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO jobs (
                company_id, external_id, title, category, department,
                location, description, job_url, posting_date,
                discovered_at, last_seen_at, is_active, relevance_score
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            (
                company_id,
                job.get("external_id"),
                job.get("title"),
                job.get("matched_category"),
                job.get("department"),
                job.get("location"),
                job.get("description"),
                job.get("job_url"),
                job.get("posting_date"),
                now,
                now,
                job.get("relevance_score"),
            ),
        )
        self.conn.commit()
        return cursor.lastrowid

    def mark_job_inactive(self, job_id: int):
        """Mark a job as inactive (removed)."""
        now = datetime.now().isoformat()
        self.conn.execute(
            "UPDATE jobs SET is_active = 0, last_seen_at = ? WHERE id = ?",
            (now, job_id),
        )
        self.conn.commit()

    def update_job_last_seen(self, job_id: int):
        """Update last_seen_at for a job."""
        now = datetime.now().isoformat()
        self.conn.execute(
            "UPDATE jobs SET last_seen_at = ? WHERE id = ?", (now, job_id)
        )
        self.conn.commit()

    # Job verification

    def update_job_verification(self, job_id: int, status: str):
        """Update job verification status ('verified', 'stale', or 'unverified')."""
        self.conn.execute(
            "UPDATE jobs SET verification_status = ? WHERE id = ?",
            (status, job_id),
        )
        self.conn.commit()

    def get_jobs_for_verification(self, company_id: int = None) -> List[Dict]:
        """
        Get jobs needing verification (status is 'unverified' and is_active=1).

        Args:
            company_id: Optional - filter to specific company. If None, get all.

        Returns:
            List of dicts with id and job_url for verification.
        """
        cursor = self.conn.cursor()
        if company_id:
            cursor.execute(
                """
                SELECT id, job_url FROM jobs
                WHERE company_id = ? AND is_active = 1 AND verification_status = 'unverified'
                """,
                (company_id,),
            )
        else:
            cursor.execute(
                """
                SELECT id, job_url FROM jobs
                WHERE is_active = 1 AND verification_status = 'unverified'
                """
            )
        return [{"id": row[0], "job_url": row[1]} for row in cursor.fetchall()]

    # Change tracking

    def record_job_change(self, job_id: int, run_id: str, change_type: str):
        """Record a job change."""
        self.conn.execute(
            "INSERT INTO job_changes (job_id, run_id, change_type) VALUES (?, ?, ?)",
            (job_id, run_id, change_type),
        )
        self.conn.commit()

    def record_run_snapshot(
        self,
        run_id: str,
        company_id: int,
        jobs_found: int,
        new_jobs: int,
        removed_jobs: int,
        status: str,
        error_message: Optional[str] = None,
    ):
        """Record a run snapshot for a company."""
        now = datetime.now().date().isoformat()
        self.conn.execute(
            """
            INSERT INTO run_snapshots (
                run_id, run_date, company_id, jobs_found,
                new_jobs, removed_jobs, status, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (run_id, now, company_id, jobs_found, new_jobs, removed_jobs, status, error_message),
        )
        self.conn.commit()

    # Statistics

    def get_statistics(self) -> Dict[str, Any]:
        """Get overall statistics."""
        cursor = self.conn.cursor()

        # Total companies
        cursor.execute("SELECT COUNT(*) FROM companies")
        total_companies = cursor.fetchone()[0]

        # Active jobs
        cursor.execute("SELECT COUNT(*) FROM jobs WHERE is_active = 1")
        active_jobs = cursor.fetchone()[0]

        # Relevant jobs (score >= 60)
        cursor.execute(
            "SELECT COUNT(*) FROM jobs WHERE is_active = 1 AND relevance_score >= 60"
        )
        relevant_jobs = cursor.fetchone()[0]

        # Last run info
        cursor.execute(
            """
            SELECT run_date, SUM(new_jobs), SUM(removed_jobs)
            FROM run_snapshots
            WHERE run_date = (SELECT MAX(run_date) FROM run_snapshots)
            GROUP BY run_date
            """
        )
        last_run = cursor.fetchone()

        # Jobs by ATS
        cursor.execute(
            """
            SELECT c.ats_provider, COUNT(j.id)
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            WHERE j.is_active = 1
            GROUP BY c.ats_provider
            """
        )
        by_ats = {row[0] or "unknown": row[1] for row in cursor.fetchall()}

        # Jobs by category
        cursor.execute(
            """
            SELECT category, COUNT(*)
            FROM jobs
            WHERE is_active = 1 AND category IS NOT NULL
            GROUP BY category
            """
        )
        by_category = {row[0]: row[1] for row in cursor.fetchall()}

        # Recent changes
        cursor.execute(
            """
            SELECT jc.change_type, j.title, c.name
            FROM job_changes jc
            JOIN jobs j ON jc.job_id = j.id
            JOIN companies c ON j.company_id = c.id
            ORDER BY jc.changed_at DESC
            LIMIT 20
            """
        )
        recent_changes = [
            {"type": row[0], "title": row[1], "company": row[2]}
            for row in cursor.fetchall()
        ]

        return {
            "total_companies": total_companies,
            "active_jobs": active_jobs,
            "relevant_jobs": relevant_jobs,
            "last_run_date": last_run[0] if last_run else None,
            "last_run_new": last_run[1] if last_run else 0,
            "last_run_removed": last_run[2] if last_run else 0,
            "by_ats": by_ats,
            "by_category": by_category,
            "recent_changes": recent_changes,
        }

    # Export

    def export_to_csv(self, output_path: str, only_relevant: bool = True):
        """Export jobs to CSV, sorted by most recent posting date."""
        cursor = self.conn.cursor()

        query = """
            SELECT
                c.name as company_name,
                c.domain as company_domain,
                c.ats_provider,
                c.first_seen_date,
                c.last_csv_date,
                j.title as job_title,
                j.category as job_category,
                j.department,
                j.location,
                j.description as job_description,
                j.job_url,
                j.posting_date,
                j.discovered_at,
                j.relevance_score,
                j.verification_status,
                dm.person_name as decision_maker_name,
                dm.title as decision_maker_title,
                dm.source_url as decision_maker_source,
                dm.confidence as decision_maker_confidence,
                dm.email as decision_maker_email,
                dm.linkedin_url as decision_maker_linkedin
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            LEFT JOIN decision_makers dm ON dm.company_id = c.id
            WHERE j.is_active = 1
            AND (j.verification_status IS NULL OR j.verification_status != 'stale')
        """

        if only_relevant:
            query += " AND j.relevance_score >= 60"

        # Sort by most recent posting date (recency-first), then by company name
        query += " ORDER BY j.posting_date DESC, c.name"

        cursor.execute(query)
        rows = cursor.fetchall()

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(rows)

    def export_to_json(self, output_path: str, only_relevant: bool = True):
        """Export jobs to JSON, sorted by most recent posting date."""
        cursor = self.conn.cursor()

        query = """
            SELECT
                c.name as company_name,
                c.domain as company_domain,
                c.ats_provider,
                c.first_seen_date,
                c.last_csv_date,
                j.title as job_title,
                j.category as job_category,
                j.department,
                j.location,
                j.description as job_description,
                j.job_url,
                j.posting_date,
                j.discovered_at,
                j.relevance_score,
                j.verification_status,
                dm.person_name as decision_maker_name,
                dm.title as decision_maker_title,
                dm.source_url as decision_maker_source,
                dm.confidence as decision_maker_confidence,
                dm.email as decision_maker_email,
                dm.linkedin_url as decision_maker_linkedin
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            LEFT JOIN decision_makers dm ON dm.company_id = c.id
            WHERE j.is_active = 1
            AND (j.verification_status IS NULL OR j.verification_status != 'stale')
        """

        if only_relevant:
            query += " AND j.relevance_score >= 60"

        # Sort by most recent posting date (recency-first), then by company name
        query += " ORDER BY j.posting_date DESC, c.name"

        cursor.execute(query)
        rows = cursor.fetchall()

        jobs = []
        for row in rows:
            job = dict(row)
            # Determine if company is new (first seen today = last csv date)
            first_seen = job.get("first_seen_date")
            last_csv = job.get("last_csv_date")
            job["is_new_company"] = first_seen == last_csv if first_seen and last_csv else False
            jobs.append(job)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"jobs": jobs, "exported_at": datetime.now().isoformat()}, f, indent=2)

    # Seen companies tracking

    def is_company_seen(self, domain: str) -> bool:
        """Check if a company has been processed in any previous run."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM seen_companies WHERE domain = ?", (domain,))
        return cursor.fetchone() is not None

    def mark_company_seen(
        self, domain: str, company_name: str, github_date: str, run_id: str
    ):
        """Mark a company as processed."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO seen_companies
            (company_name, domain, github_listing_date, run_id)
            VALUES (?, ?, ?, ?)
            """,
            (company_name, domain, github_date, run_id),
        )
        self.conn.commit()

    def get_seen_companies_count(self) -> int:
        """Get total count of seen companies."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM seen_companies")
        return cursor.fetchone()[0]

    def reset_seen_companies(self):
        """Reset the seen companies table (for re-processing)."""
        self.conn.execute("DELETE FROM seen_companies")
        self.conn.commit()
