"""
Scraper for Welcome to the Jungle.
Uses the Algolia search API directly (the site is fully client-side rendered).
"""

import asyncio
import httpx
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location


class WelcomeToTheJungleScraper(BaseScraper):
    """Scraper for WTTJ using Algolia API."""

    BASE_URL = "https://www.welcometothejungle.com"

    # Algolia credentials (public client-side keys from WTTJ frontend)
    ALGOLIA_APP_ID = "CSEKHVMS53"
    ALGOLIA_API_KEY = "4bd8f6215d0cc52b26430765769e65a0"
    ALGOLIA_INDEX = "wttj_jobs_production_fr"

    ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/{ALGOLIA_INDEX}/query"

    HEADERS = {
        "X-Algolia-Application-Id": ALGOLIA_APP_ID,
        "X-Algolia-API-Key": ALGOLIA_API_KEY,
        "Content-Type": "application/json",
        "Referer": "https://www.welcometothejungle.com/",
        "Origin": "https://www.welcometothejungle.com",
    }

    def __init__(self):
        super().__init__("wttj")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """Scrape WTTJ via Algolia search API."""
        all_offers = []
        max_pages = kwargs.get("max_pages", 3)
        hits_per_page = 20

        queries = [
            "alternance informatique",
            "alternance développeur",
            "alternance data",
        ]

        seen_ids = set()

        async with httpx.AsyncClient(timeout=30.0) as client:
            semaphore = asyncio.Semaphore(5)

            async def fetch_page(query: str, page: int):
                async with semaphore:
                    try:
                        payload = {
                            "query": query,
                            "hitsPerPage": hits_per_page,
                            "page": page,
                        }
                        res = await client.post(
                            self.ALGOLIA_URL,
                            json=payload,
                            headers=self.HEADERS,
                        )
                        if res.status_code != 200:
                            self.logger.warning(
                                f"WTTJ Algolia HTTP {res.status_code}: {res.text[:200]}"
                            )
                            return []
                        data = res.json()
                        hits = data.get("hits", [])
                        self.logger.debug(
                            f"WTTJ query='{query}' page={page}: {len(hits)} hits"
                        )
                        return hits
                    except Exception as e:
                        self.logger.error(f"WTTJ scrape error: {e}")
                        return []

            tasks = []
            for query in queries:
                for page in range(0, max_pages):
                    tasks.append(fetch_page(query, page))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for hits in results:
                if isinstance(hits, Exception) or not hits:
                    continue
                for hit in hits:
                    obj_id = hit.get("objectID", "")
                    if obj_id and obj_id not in seen_ids:
                        seen_ids.add(obj_id)
                        all_offers.append(hit)

        self.logger.info(f"WTTJ collected {len(all_offers)} raw items")
        return all_offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            title = raw_data.get("name", "")
            if not title:
                return None

            org = raw_data.get("organization", {})
            company = org.get("name", "Entreprise confidentielle")
            org_slug = org.get("slug", "")

            slug = raw_data.get("slug", "")
            url = (
                f"{self.BASE_URL}/fr/companies/{org_slug}/jobs/{slug}"
                if slug and org_slug
                else ""
            )

            # Build description from summary + key_missions + profile
            parts = []
            summary = raw_data.get("summary", "")
            if summary:
                parts.append(summary)

            missions = raw_data.get("key_missions", [])
            if missions:
                parts.append("Missions : " + " | ".join(missions))

            profile_text = raw_data.get("profile", "")
            if profile_text:
                parts.append(profile_text)

            desc = " ".join(parts) if parts else "Voir l'offre pour la description"

            date_str = raw_data.get("published_at", "")
            pub_date = (
                datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                if date_str
                else datetime.utcnow()
            )

            # Location from offices
            offices = raw_data.get("offices", [])
            city = offices[0].get("city", "") if offices else ""
            clean_loc = clean_text(city)
            enriched_loc, dept = enrich_location(clean_loc)

            # Education level mapping
            edu = raw_data.get("education_level", "")
            profile_val = None
            edu_map = {
                "bac": "Bac",
                "bac_2": "Bac+2",
                "bac_3": "Bac+3",
                "bac_4": "Bac+4",
                "bac_5": "Bac+5",
            }
            if edu in edu_map:
                profile_val = edu_map[edu]

            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or clean_loc,
                "department": dept,
                "contract_type": "Alternance",
                "salary": None,
                "salary_min": raw_data.get("salary_minimum"),
                "salary_max": raw_data.get("salary_maximum"),
                "description": desc,
                "profile": profile_val,
                "category": None,
                "publication_date": pub_date,
                "source": "wttj",
                "url": url,
                "source_id": f"wttj_{raw_data.get('objectID', '')}",
                "is_school": is_school_offer(clean_text(company), desc),
            }
        except Exception as e:
            self.logger.debug(f"WTTJ parse error: {e}")
            return None
