"""Lightweight job URL verification using HEAD requests."""

import asyncio
import logging
from typing import List, Dict, Tuple

import httpx

logger = logging.getLogger(__name__)


class JobVerifier:
    """
    Verify job URLs are still live using async HEAD requests.

    Uses HEAD requests for efficiency (no body download).
    Batches requests to avoid overwhelming servers.
    """

    def __init__(self, timeout: float = 5.0, batch_size: int = 20):
        """
        Initialize the job verifier.

        Args:
            timeout: Timeout in seconds for each HEAD request.
            batch_size: Number of URLs to verify concurrently.
        """
        self.timeout = timeout
        self.batch_size = batch_size

    async def verify_jobs(self, jobs: List[Dict]) -> List[Tuple[int, str]]:
        """
        Verify job URLs in batches.

        Args:
            jobs: List of dicts with 'id' and 'job_url' keys.

        Returns:
            List of (job_id, status) tuples where status is:
            - 'verified': URL returned 2xx or 3xx (job likely still active)
            - 'stale': URL returned 4xx or 5xx (job likely removed)
            - 'unverified': Network error or timeout (couldn't determine)
        """
        results = []

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            follow_redirects=True,
        ) as client:
            for i in range(0, len(jobs), self.batch_size):
                batch = jobs[i : i + self.batch_size]
                batch_results = await asyncio.gather(
                    *[self._check_url(client, job) for job in batch],
                    return_exceptions=True,
                )

                for result in batch_results:
                    if isinstance(result, tuple):
                        results.append(result)
                    # Skip exceptions - they're logged in _check_url

        return results

    async def _check_url(
        self, client: httpx.AsyncClient, job: Dict
    ) -> Tuple[int, str]:
        """
        Check if a single job URL is still accessible.

        Args:
            client: HTTP client to use.
            job: Dict with 'id' and 'job_url' keys.

        Returns:
            Tuple of (job_id, status).
        """
        job_id = job["id"]
        job_url = job["job_url"]

        try:
            response = await client.head(job_url)

            # 2xx and 3xx = job page exists
            if response.status_code < 400:
                return (job_id, "verified")
            # 4xx = page not found (job likely removed)
            elif response.status_code < 500:
                logger.info(f"Job {job_id} appears stale (HTTP {response.status_code})")
                return (job_id, "stale")
            # 5xx = server error, can't determine
            else:
                logger.warning(
                    f"Job {job_id} verification inconclusive (HTTP {response.status_code})"
                )
                return (job_id, "unverified")

        except httpx.TimeoutException:
            logger.debug(f"Job {job_id} verification timed out")
            return (job_id, "unverified")
        except httpx.RequestError as e:
            logger.debug(f"Job {job_id} verification failed: {e}")
            return (job_id, "unverified")
        except Exception as e:
            logger.error(f"Unexpected error verifying job {job_id}: {e}")
            return (job_id, "unverified")
