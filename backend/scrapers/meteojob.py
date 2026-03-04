"""
Scraper for Meteojob platform.
Handles dynamic Next.js payload parsing and HTML evaluation.
"""

import asyncio
import re
import json
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_profile, normalize_salary

class MeteojobScraper(BaseScraper):
    """Scraper for Meteojob."""

    BASE_URL = "https://www.meteojob.com"
    SEARCH_URL = f"{BASE_URL}/emploi/offres-emploi-alternance"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    }

    def __init__(self):
        super().__init__("meteojob")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """Scrape Meteojob alternance offers."""
        keywords = kwargs.get("keywords", ["informatique", "développeur", "data", "alternance"])
        max_pages = kwargs.get("max_pages", 2)

        all_offers = []
        seen_ids = set()

        async with httpx.AsyncClient(timeout=30.0, headers=self.HEADERS, follow_redirects=True) as client:
            semaphore = asyncio.Semaphore(3)

            async def fetch_page(kw: str, p: int):
                async with semaphore:
                    return await self._scrape_page(client, kw, p)

            tasks = []
            for keyword in keywords:
                for page in range(1, max_pages + 1):
                    tasks.append(fetch_page(keyword, page))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            initial_offers = []

            for page_results in results:
                if isinstance(page_results, Exception) or not page_results:
                    continue
                for raw in page_results:
                    offer_id = str(raw.get("id") or raw.get("jobId", ""))
                    if offer_id and offer_id not in seen_ids:
                        seen_ids.add(offer_id)
                        initial_offers.append(raw)
                    elif not offer_id:
                        initial_offers.append(raw)

            self.logger.info(f"Meteojob collected {len(initial_offers)} raw items, fetching descriptions...")

            # Fetch full descriptions using concurrency
            desc_semaphore = asyncio.Semaphore(5)

            async def enrich_description(off):
                url = off.get("url")
                if url:
                    async with desc_semaphore:
                        full_desc = await self._fetch_description(client, url)
                        if full_desc:
                            off["description"] = full_desc
                return off

            enrich_tasks = [enrich_description(off) for off in initial_offers]
            all_offers = await asyncio.gather(*enrich_tasks)

        self.logger.info(f"Meteojob finished with {len(all_offers)} enriched offers")
        return all_offers

    async def _fetch_description(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        """Fetch the full job description from the detail page."""
        try:
            res = await client.get(url)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                # Look for typical Meteojob description containers
                desc_el = (
                    soup.select_one(".job-description")
                    or soup.select_one("section[data-test='job-description']")
                    or soup.select_one(".description-body")
                    or soup.select_one("article")
                )
                if desc_el:
                    return desc_el.get_text(separator="\n", strip=True)
        except Exception as e:
            self.logger.debug(f"Error fetching Meteojob description from {url}: {e}")
        return None

    async def _scrape_page(self, client: httpx.AsyncClient, keyword: str, page: int) -> List[Dict[str, Any]]:
        """Fetch a single search page from Meteojob."""
        params = {"motsCles": f"{keyword}", "page": page}
        
        try:
            res = await client.get(self.SEARCH_URL, params=params)
            if res.status_code != 200:
                self.logger.warning(f"Meteojob HTTP {res.status_code} on page {page}")
                return []

            soup = BeautifulSoup(res.text, "html.parser")
            results = []

            # 1. First attempt to extract NEXT_DATA (SSR React State)
            script = soup.find("script", id="__NEXT_DATA__")
            if script:
                try:
                    data = json.loads(script.string)
                    # Finding the jobs array inside the nested props structure
                    queries = data.get("props", {}).get("pageProps", {}).get("dehydratedState", {}).get("queries", [])
                    for query in queries:
                        jobs = query.get("state", {}).get("data", {}).get("jobs", [])
                        if jobs:
                            for job in jobs:
                                job_id = str(job.get("id", ""))
                                url_slug = job.get("slug", "")
                                company_dict = job.get("company", {}) or {}
                                company = company_dict.get("name", "Meteojob Entreprise")
                                location_dict = job.get("location", {}) or {}
                                location = location_dict.get("city", "")
                                
                                results.append({
                                    "id": job_id,
                                    "title": job.get("title", ""),
                                    "company": company,
                                    "location": location,
                                    "contract": "Alternance",
                                    "description": job.get("excerpt", "Voir l'offre pour plus de détails."),
                                    "url": f"{self.BASE_URL}/emploi/{url_slug}" if url_slug else f"{self.BASE_URL}/emploi/{job_id}",
                                })
                            return results  # Stop here if we successfully found it via NEXT_DATA
                except Exception as e:
                    self.logger.debug(f"Meteojob NEXT_DATA extraction failed: {e}")

            # 2. Fallback to HTML DOM parsing
            cards = soup.select(".job-card, article.job, div[data-test='job-card']")
            if not cards:
                return []

            for card in cards:
                try:
                    title_el = card.select_one(".job-title, h2")
                    title = title_el.get_text(strip=True) if title_el else ""
                    if not title:
                        continue
                        
                    company_el = card.select_one(".company-name, .job-company")
                    company = company_el.get_text(strip=True) if company_el else "Meteojob Entreprise"

                    loc_el = card.select_one(".job-location, .location")
                    location = loc_el.get_text(strip=True) if loc_el else ""

                    desc_el = card.select_one(".job-excerpt, p")
                    desc = desc_el.get_text(strip=True) if desc_el else "Voir plus."

                    link_el = card.find("a")
                    href = link_el.get("href", "") if link_el else ""
                    url = href if href.startswith("http") else f"{self.BASE_URL}{href}"

                    # extract id from URL
                    match = re.search(r"-(\d+)$", href)
                    offer_id = match.group(1) if match else str(hash(url))

                    results.append({
                        "id": offer_id,
                        "title": title,
                        "company": company,
                        "location": location,
                        "contract": "Alternance",
                        "description": desc,
                        "url": url,
                    })

                except Exception as e:
                    continue

            return results
        except Exception as e:
            self.logger.error(f"Meteojob scrape error: {e}")
            return []

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        title = raw_data.get("title", "")
        url = raw_data.get("url", "")
        if not title or not url:
            return None

        clean_loc = clean_text(raw_data.get("location", ""))
        enriched_loc, dept = enrich_location(clean_loc)

        desc = raw_data.get("description", "")
        clean_company = clean_text(raw_data.get("company", "Meteojob Entreprise"))

        return {
            "title": clean_text(title),
            "company": clean_company,
            "location": enriched_loc or clean_loc,
            "department": dept,
            "contract_type": "Alternance",
            "salary": None,  # Meteojob often hides salaries in alternance
            "salary_min": None,
            "salary_max": None,
            "description": clean_text(desc),
            "profile": None,
            "category": None,
            "publication_date": datetime.utcnow(),
            "source": "meteojob",
            "url": url,
            "source_id": f"meteojob_{raw_data.get('id', '')}",
            "is_school": is_school_offer(clean_company, desc),
        }
