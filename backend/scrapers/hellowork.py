"""
Scraper for HelloWork platform.
Uses data-cy selectors and aria-label parsing for the updated HTML structure.
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


class HelloWorkScraper(BaseScraper):
    """Scraper for HelloWork public search pages."""

    BASE_URL = "https://www.hellowork.com"
    SEARCH_URL = f"{BASE_URL}/fr-fr/emploi/recherche.html"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Cache-Control": "max-age=0",
    }

    def __init__(self):
        super().__init__("hellowork")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """Scrape HelloWork alternance offers."""
        keywords = kwargs.get(
            "keywords",
            ["alternance informatique", "alternance développeur", "alternance data"],
        )
        max_pages = kwargs.get("max_pages", 2)

        all_offers = []
        seen_ids = set()

        async with httpx.AsyncClient(
            timeout=30.0, headers=self.HEADERS, follow_redirects=True
        ) as client:
            semaphore = asyncio.Semaphore(4)
            
            async def fetch_page(kw: str, p: int):
                async with semaphore:
                    return await self._scrape_page(client, kw, p)

            tasks = []
            for keyword in keywords:
                for page in range(1, max_pages + 1):
                    tasks.append(fetch_page(keyword, page))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for offers in results:
                if isinstance(offers, Exception) or not offers:
                    continue
                for raw in offers:
                    offer_id = raw.get("_id", "")
                    if offer_id and offer_id not in seen_ids:
                        seen_ids.add(offer_id)
                        all_offers.append(raw)
                    elif not offer_id:
                        all_offers.append(raw)

        self.logger.info(f"HelloWork collected {len(all_offers)} raw items")
        return all_offers

    async def _scrape_page(
        self, client: httpx.AsyncClient, keyword: str, page: int
    ) -> List[Dict[str, Any]]:
        params = {"k": keyword, "c": "Alternance", "p": page}
        try:
            res = await client.get(self.SEARCH_URL, params=params)
            if res.status_code != 200:
                self.logger.warning(f"HelloWork HTTP {res.status_code}")
                return []

            soup = BeautifulSoup(res.text, "html.parser")
            results = []

            # Use data-cy='serpCard' to find job cards
            cards = soup.find_all(attrs={"data-cy": "serpCard"})

            if not cards:
                self.logger.warning(
                    f"HelloWork: no serpCard elements found for '{keyword}' page {page}"
                )
                return []

            self.logger.debug(
                f"HelloWork: {len(cards)} cards for '{keyword}' page {page}"
            )

            for card in cards:
                try:
                    # Get the main link with aria-label (contains structured info)
                    main_link = card.find("a", attrs={"aria-label": True})
                    aria_label = main_link.get("aria-label", "") if main_link else ""

                    # Extract title from data-cy='offerTitle'
                    title_el = card.find(attrs={"data-cy": "offerTitle"})
                    raw_title = title_el.get_text(strip=True) if title_el else ""

                    if not raw_title:
                        continue

                    # Extract company from aria-label:
                    # "Voir offre de TITLE à LOCATION, chez COMPANY, pour un CONTRACT, ..."
                    company = "Entreprise confidentielle"
                    if "chez " in aria_label:
                        after_chez = aria_label.split("chez ", 1)[1]
                        company = after_chez.split(",")[0].strip()

                    # Clean title: sometimes company name is appended to title text
                    title = raw_title
                    if company != "Entreprise confidentielle" and raw_title.endswith(
                        company
                    ):
                        title = raw_title[: -len(company)].strip()

                    # URL
                    href = main_link.get("href", "") if main_link else ""
                    url = href if href.startswith("http") else f"{self.BASE_URL}{href}"

                    # Location
                    loc_el = card.find(attrs={"data-cy": "localisationCard"})
                    location = loc_el.get_text(strip=True) if loc_el else ""

                    # Contract type
                    contract_el = card.find(attrs={"data-cy": "contractCard"})
                    contract = (
                        contract_el.get_text(strip=True) if contract_el else "Alternance"
                    )

                    # Offer ID from analytics data or URL
                    offer_id = ""
                    analytics_param = card.get("data-analytics-values-param", "")
                    if analytics_param:
                        try:
                            analytics = json.loads(analytics_param)
                            products = analytics.get("product_data", [])
                            if products:
                                offer_id = str(products[0].get("product_id", ""))
                        except (json.JSONDecodeError, TypeError):
                            pass

                    if not offer_id and href:
                        match = re.search(r"/emplois/(\d+)", href)
                        if match:
                            offer_id = match.group(1)

                    # Extract salary from aria-label if present
                    salary_text = ""
                    if "salaire de " in aria_label:
                        salary_part = aria_label.split("salaire de ", 1)[1]
                        salary_text = salary_part.split(",")[0].strip()

                    results.append(
                        {
                            "title": title,
                            "url": url,
                            "company": company,
                            "location": location,
                            "contract": contract,
                            "description": "Voir l'offre pour la description complète",
                            "salary_text": salary_text,
                            "_id": offer_id,
                        }
                    )
                except Exception as e:
                    self.logger.debug(f"HelloWork card parse error: {e}")
                    continue

            return results
        except Exception as e:
            self.logger.error(f"HelloWork scrape logic error: {e}")
            return []

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not raw_data.get("title") or not raw_data.get("url"):
            return None

        clean_loc = clean_text(raw_data.get("location", ""))
        enriched_loc, dept = enrich_location(clean_loc)

        salary_min, salary_max = None, None
        salary_text = raw_data.get("salary_text", "")
        if salary_text:
            try:
                salary_min, salary_max = normalize_salary(salary_text)
            except Exception:
                pass

        return {
            "title": clean_text(raw_data["title"]),
            "company": clean_text(raw_data["company"]),
            "location": enriched_loc or clean_loc,
            "department": dept,
            "contract_type": "Alternance",
            "salary": salary_text if salary_text else None,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "description": clean_text(raw_data["description"]),
            "profile": None,
            "category": None,
            "publication_date": datetime.utcnow(),
            "source": "hellowork",
            "url": raw_data["url"],
            "source_id": f"hw_{raw_data.get('_id', '')}",
            "is_school": is_school_offer(clean_text(raw_data["company"]), clean_text(raw_data["description"])),
        }
