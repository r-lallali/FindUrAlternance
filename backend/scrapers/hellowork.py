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
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_profile, normalize_salary, parse_french_date


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
            ["alternance", "apprentissage"],
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
            initial_offers = []
            for offers in results:
                if isinstance(offers, Exception) or not offers:
                    continue
                for raw in offers:
                    offer_id = raw.get("_id", "")
                    if offer_id and offer_id not in seen_ids:
                        seen_ids.add(offer_id)
                        initial_offers.append(raw)
                    elif not offer_id:
                        initial_offers.append(raw)

            self.logger.info(f"HelloWork collected {len(initial_offers)} raw items, fetching descriptions...")

            # Fetch full descriptions with concurrency limit
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

        self.logger.info(f"HelloWork finished with {len(all_offers)} enriched offers")
        return all_offers

    async def _fetch_description(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        """Fetch the full job description from the detail page."""
        try:
            res = await client.get(url)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, "html.parser")
                # Look for description panels
                desc_el = (
                    soup.select_one("#offer-panel")
                    or soup.select_one("section.tw-peer")
                    or soup.select_one("[data-cy='descriptionJob']")
                    or soup.select_one(".description")
                )
                if desc_el:
                    return desc_el.get_text(separator="\n", strip=True)
        except Exception as e:
            self.logger.debug(f"Error fetching HelloWork description from {url}: {e}")
        return None

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
                            "date_text": card.find(attrs={"data-cy": "publishDateCard"}).get_text(strip=True) if card.find(attrs={"data-cy": "publishDateCard"}) else "",
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

        # Date parsing logic:
        # 1. Try from the card's relative/absolute text
        # 2. Try to find a date in the full description (e.g. "Publiée le 25/02/2026")
        pub_date = parse_french_date(raw_data.get("date_text", ""))
        
        if not pub_date and raw_data.get("description"):
            # Look for common patterns in description footer
            desc = raw_data["description"]
            match = re.search(r"publiée le\s+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})", desc, re.IGNORECASE)
            if match:
                pub_date = parse_french_date(match.group(1))
            else:
                # Try textual match "25 février 2026"
                match = re.search(r"publiée le\s+(\d{1,2}\s+[a-zéû\.]+\s+\d{4})", desc, re.IGNORECASE)
                if match:
                    pub_date = parse_french_date(match.group(1))

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
            "publication_date": pub_date or datetime.utcnow(),
            "source": "hellowork",
            "url": raw_data["url"],
            "source_id": f"hw_{raw_data.get('_id', '')}",
            "is_school": is_school_offer(clean_text(raw_data["company"]), clean_text(raw_data["description"])),
        }
