"""Change detection for job postings between runs."""

import uuid
from datetime import datetime
from typing import List, Dict, Set
import sqlite3

from .models import JobChange, ChangeReport


class ChangeDetector:
    """
    Detect new and removed jobs between runs.

    Algorithm:
    1. Load previously active jobs for company from database
    2. Compare with newly fetched jobs
    3. Mark new jobs (in fetch but not in DB)
    4. Mark removed jobs (in DB but not in fetch)
    5. Record changes in job_changes table
    """

    def __init__(self, db_connection: sqlite3.Connection):
        self.conn = db_connection
        self.run_id = str(uuid.uuid4())[:8]

    def detect_changes(
        self,
        company_id: int,
        company_name: str,
        fetched_jobs: List[Dict],
    ) -> ChangeReport:
        """
        Compare fetched jobs against database state.

        Args:
            company_id: Database ID of the company
            company_name: Company name for reporting
            fetched_jobs: List of jobs from ATS, each with 'external_id' key

        Returns:
            ChangeReport with new and removed jobs
        """
        # Get current active jobs from database
        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT id, external_id, title, job_url
            FROM jobs
            WHERE company_id = ? AND is_active = 1
            """,
            (company_id,),
        )

        db_jobs = {
            row[1]: {"id": row[0], "external_id": row[1], "title": row[2], "job_url": row[3]}
            for row in cursor.fetchall()
        }

        # Get external IDs from fetched jobs
        fetched_ids = {job["external_id"] for job in fetched_jobs}
        db_ids = set(db_jobs.keys())

        # Find new jobs (in fetch, not in DB)
        new_ids = fetched_ids - db_ids
        new_jobs = []
        for job in fetched_jobs:
            if job["external_id"] in new_ids:
                new_jobs.append(
                    JobChange(
                        job_id=0,  # Will be assigned on insert
                        external_id=job["external_id"],
                        title=job.get("title", ""),
                        company_name=company_name,
                        change_type="new",
                        job_url=job.get("job_url", ""),
                    )
                )

        # Find removed jobs (in DB, not in fetch)
        removed_ids = db_ids - fetched_ids
        removed_jobs = []
        for ext_id in removed_ids:
            db_job = db_jobs[ext_id]
            removed_jobs.append(
                JobChange(
                    job_id=db_job["id"],
                    external_id=ext_id,
                    title=db_job["title"],
                    company_name=company_name,
                    change_type="removed",
                    job_url=db_job["job_url"],
                )
            )

        return ChangeReport(
            run_id=self.run_id,
            run_date=datetime.now(),
            company_id=company_id,
            company_name=company_name,
            new_jobs=new_jobs,
            removed_jobs=removed_jobs,
            total_active=len(fetched_ids),
        )

    def apply_changes(self, report: ChangeReport, fetched_jobs: List[Dict]) -> None:
        """
        Apply detected changes to the database.

        1. Insert new jobs
        2. Mark removed jobs as inactive
        3. Update last_seen_at for active jobs
        4. Record changes in job_changes table
        """
        cursor = self.conn.cursor()
        now = datetime.now().isoformat()

        # Create a map of external_id -> job for quick lookup
        jobs_by_ext_id = {job["external_id"]: job for job in fetched_jobs}

        # Insert new jobs
        for change in report.new_jobs:
            job = jobs_by_ext_id.get(change.external_id)
            if not job:
                continue

            cursor.execute(
                """
                INSERT INTO jobs (
                    company_id, external_id, title, category, department,
                    location, description, job_url, posting_date,
                    discovered_at, last_seen_at, is_active, relevance_score
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
                """,
                (
                    report.company_id,
                    job.get("external_id"),
                    job.get("title", ""),
                    job.get("matched_category"),
                    job.get("department"),
                    job.get("location"),
                    job.get("description"),
                    job.get("job_url", ""),
                    job.get("posting_date"),
                    now,
                    now,
                    job.get("relevance_score", 0),
                ),
            )

            job_id = cursor.lastrowid

            # Record change
            cursor.execute(
                """
                INSERT INTO job_changes (job_id, run_id, change_type)
                VALUES (?, ?, 'new')
                """,
                (job_id, report.run_id),
            )

        # Mark removed jobs as inactive
        for change in report.removed_jobs:
            cursor.execute(
                """
                UPDATE jobs SET is_active = 0, last_seen_at = ?
                WHERE id = ?
                """,
                (now, change.job_id),
            )

            cursor.execute(
                """
                INSERT INTO job_changes (job_id, run_id, change_type)
                VALUES (?, ?, 'removed')
                """,
                (change.job_id, report.run_id),
            )

        # Update last_seen_at for still-active jobs (not new)
        new_ext_ids = {change.external_id for change in report.new_jobs}
        active_external_ids = [
            job["external_id"]
            for job in fetched_jobs
            if job["external_id"] not in new_ext_ids
        ]

        if active_external_ids:
            placeholders = ",".join("?" * len(active_external_ids))
            cursor.execute(
                f"""
                UPDATE jobs SET last_seen_at = ?
                WHERE company_id = ? AND external_id IN ({placeholders})
                """,
                [now, report.company_id] + active_external_ids,
            )

        # Record run snapshot
        cursor.execute(
            """
            INSERT INTO run_snapshots (
                run_id, run_date, company_id, jobs_found,
                new_jobs, removed_jobs, status
            ) VALUES (?, ?, ?, ?, ?, ?, 'success')
            """,
            (
                report.run_id,
                report.run_date.date().isoformat(),
                report.company_id,
                report.total_active,
                len(report.new_jobs),
                len(report.removed_jobs),
            ),
        )

        self.conn.commit()
