"""SQLite database operations for job discovery."""

import sqlite3
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Any

from .models import Company, JobPosting


class Database:
    """SQLite database for job discovery data."""

    SCHEMA = """
    -- Companies from Apollo CSV
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

    -- Indexes for performance
    CREATE INDEX IF NOT EXISTS idx_jobs_company ON jobs(company_id);
    CREATE INDEX IF NOT EXISTS idx_jobs_active ON jobs(is_active);
    CREATE INDEX IF NOT EXISTS idx_jobs_relevance ON jobs(relevance_score);
    CREATE INDEX IF NOT EXISTS idx_changes_run ON job_changes(run_id);
    CREATE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain);
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

    def upsert_company(self, company: Dict) -> int:
        """Insert or update a company, return its ID."""
        cursor = self.conn.cursor()

        # Check if exists
        cursor.execute(
            "SELECT id FROM companies WHERE domain = ?", (company["domain"],)
        )
        row = cursor.fetchone()

        now = datetime.now().isoformat()

        if row:
            # Update existing
            cursor.execute(
                """
                UPDATE companies SET
                    name = ?, website = ?, industry = ?, keywords = ?,
                    employee_count = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    company["name"],
                    company.get("website"),
                    company.get("industry"),
                    company.get("keywords"),
                    company.get("employee_count"),
                    now,
                    row["id"],
                ),
            )
            self.conn.commit()
            return row["id"]
        else:
            # Insert new
            cursor.execute(
                """
                INSERT INTO companies (name, domain, website, industry, keywords, employee_count)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    company["name"],
                    company["domain"],
                    company.get("website"),
                    company.get("industry"),
                    company.get("keywords"),
                    company.get("employee_count"),
                ),
            )
            self.conn.commit()
            return cursor.lastrowid

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
        """Export jobs to CSV."""
        cursor = self.conn.cursor()

        query = """
            SELECT
                c.name as company_name,
                c.domain as company_domain,
                c.ats_provider,
                j.title as job_title,
                j.category as job_category,
                j.department,
                j.location,
                j.description as job_description,
                j.job_url,
                j.posting_date,
                j.discovered_at,
                j.relevance_score,
                j.is_active
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            WHERE j.is_active = 1
        """

        if only_relevant:
            query += " AND j.relevance_score >= 60"

        query += " ORDER BY j.relevance_score DESC, c.name"

        cursor.execute(query)
        rows = cursor.fetchall()

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([desc[0] for desc in cursor.description])
            writer.writerows(rows)

    def export_to_json(self, output_path: str, only_relevant: bool = True):
        """Export jobs to JSON."""
        cursor = self.conn.cursor()

        query = """
            SELECT
                c.name as company_name,
                c.domain as company_domain,
                c.ats_provider,
                j.title as job_title,
                j.category as job_category,
                j.department,
                j.location,
                j.description as job_description,
                j.job_url,
                j.posting_date,
                j.discovered_at,
                j.relevance_score,
                j.is_active
            FROM jobs j
            JOIN companies c ON j.company_id = c.id
            WHERE j.is_active = 1
        """

        if only_relevant:
            query += " AND j.relevance_score >= 60"

        query += " ORDER BY j.relevance_score DESC, c.name"

        cursor.execute(query)
        rows = cursor.fetchall()

        jobs = []
        for row in rows:
            job = dict(row)
            job["is_new"] = False  # Would need change tracking
            job["is_removed"] = False
            jobs.append(job)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump({"jobs": jobs, "exported_at": datetime.now().isoformat()}, f, indent=2)
