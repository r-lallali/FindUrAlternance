"""
Scraper for France Travail (ex Pôle Emploi) via public search.

Scrapes the public job search results for alternance offers.
"""

import asyncio
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_profile, normalize_salary, parse_french_date


class FranceTravailScraper(BaseScraper):
    """Scraper for France Travail using public search pages."""

    SEARCH_URL = "https://candidat.francetravail.fr/offres/recherche"
    API_URL = "https://candidat.francetravail.fr/offres/recherche/avancee"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

    SEARCH_TERMS = [
        "alternance",
        "apprentissage",
        "contrat de professionnalisation",
    ]

    def __init__(self):
        super().__init__("francetravail")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape alternance offers from France Travail.

        Keyword args:
            search_terms: list - Search terms (defaults to SEARCH_TERMS)
            max_pages: int - Max pages per search term (default: 3)
        """
        search_terms = kwargs.get("search_terms", self.SEARCH_TERMS[:5])
        max_pages = kwargs.get("max_pages", 2)

        all_offers = []
        seen_ids = set()

        async with httpx.AsyncClient(
            timeout=30.0,
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            semaphore = asyncio.Semaphore(3)

            async def fetch_term_page(term, page):
                async with semaphore:
                    try:
                        return await self._search_page(client, term, page)
                    except Exception as e:
                        self.logger.warning(f"Error scraping page {page} for '{term}': {e}")
                        return []

            tasks = []
            for term in search_terms:
                for page in range(1, max_pages + 1):
                    tasks.append(fetch_term_page(term, page))
            
            results = await asyncio.gather(*tasks)
            initial_offers = []
            for offers in results:
                for offer in offers:
                    oid = offer.get("_id", "")
                    if oid and oid not in seen_ids:
                        seen_ids.add(oid)
                        initial_offers.append(offer)

            self.logger.info(f"France Travail: {len(initial_offers)} unique offers collected, fetching descriptions...")

            # Fetch full descriptions with higher concurrency for detail pages
            desc_semaphore = asyncio.Semaphore(5)

            async def enrich_description(off):
                oid = off.get("_id")
                if oid:
                    async with desc_semaphore:
                        detail = await self._fetch_description(client, oid)
                        if detail and isinstance(detail, dict):
                            if detail.get("description"):
                                off["description"] = detail["description"]
                            if detail.get("location"):
                                off["location"] = detail["location"]
                return off

            enrich_tasks = [enrich_description(off) for off in initial_offers]
            all_offers = await asyncio.gather(*enrich_tasks)

        self.logger.info(f"France Travail: {len(all_offers)} offers enriched and ready")
        return all_offers

    async def _fetch_description(self, client: httpx.AsyncClient, offer_id: str) -> Optional[Dict[str, str]]:
        """Fetch the full job description and location from the detail page."""
        try:
            url = f"https://candidat.francetravail.fr/offres/recherche/detail/{offer_id}"
            response = await client.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                result = {}

                # Extract description
                desc_el = (
                    soup.select_one("div[itemprop='description']")
                    or soup.select_one(".description")
                    or soup.select_one(".offre-detail-description")
                )
                if desc_el:
                    result["description"] = desc_el.get_text(separator="\n", strip=True)

                # Extract location from detail page
                # Location is in the <p> right after the <h1> title
                # Format: "76 - Saint-Aubin-lès-Elbeuf-Localiser avec Mappy"
                import re
                h1 = soup.select_one("h1")
                if h1:
                    loc_el = h1.find_next_sibling()
                    if loc_el:
                        loc_text = loc_el.get_text(strip=True)
                        # Remove "Localiser avec Mappy" and everything after
                        loc_text = re.split(r'-?\s*Localiser avec Mappy', loc_text)[0].strip()
                        # Remove trailing dash if any
                        loc_text = loc_text.rstrip('-').strip()
                        if loc_text and len(loc_text) > 2:
                            result["location"] = loc_text

                return result if result else None
        except Exception as e:
            self.logger.debug(f"Error fetching description for {offer_id}: {e}")
        return None

    async def _search_page(
        self, client: httpx.AsyncClient, keyword: str, page: int
    ) -> List[Dict[str, Any]]:
        """Scrape a single search results page."""
        params = {
            "motsCles": keyword,
            "typeContrat": "CDD,CDI",
            "page": page,
        }

        response = await client.get(self.SEARCH_URL, params=params)

        if response.status_code != 200:
            self.logger.warning(f"France Travail returned {response.status_code}")
            return []

        return self._parse_search_page(response.text)

    def _parse_search_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse the HTML search results page."""
        soup = BeautifulSoup(html, "html.parser")
        offers = []

        # France Travail uses result-list items
        results = soup.select("[id^='result-']") or soup.select(".result")

        if not results:
            # Try alternative selectors
            results = soup.select("li[data-id-offre]") or soup.select(".media")

        for result in results:
            try:
                offer = {}

                # Extract offer ID
                offer_id = (
                    result.get("data-id-offre", "")
                    or result.get("id", "").replace("result-", "")
                )
                offer["_id"] = offer_id

                # Title
                title_el = (
                    result.select_one("h2 a")
                    or result.select_one(".media-heading a")
                    or result.select_one("a[data-intitule]")
                    or result.select_one("h2")
                )
                if title_el:
                    offer["title"] = title_el.get_text(strip=True)
                    href = title_el.get("href", "")
                if offer.get("_id"):
                    offer["url"] = f"https://candidat.francetravail.fr/offres/recherche/detail/{offer['_id']}"
                elif title_el:
                    href = title_el.get("href", "")
                    if href and not href.startswith("http"):
                        href = f"https://candidat.francetravail.fr{href}"
                    offer["url"] = href

                # Company
                company_el = (
                    result.select_one("[data-entreprise]")
                    or result.select_one(".subtext")
                    or result.select_one(".company")
                )
                if company_el:
                    comp_text = company_el.get_text(strip=True)
                    import re
                    # Remove merged location like " -93 - DRANCY"
                    comp_text = re.split(r'\s*-\s*\d{2,3}\s*-\s*', comp_text)[0].strip()
                    offer["company"] = comp_text
                else:
                    offer["company"] = "Entreprise confidentielle"

                # Location
                loc_el = (
                    result.select_one("[data-lieu]")
                    or result.select_one(".subtext + .subtext")
                    or result.select_one(".location")
                )
                if loc_el:
                    offer["location"] = loc_el.get_text(strip=True)

                # Contract type
                contract_el = result.select_one("[data-contrat]") or result.select_one(".contract-type")
                if contract_el:
                    offer["contract_type"] = contract_el.get_text(strip=True)
                else:
                    offer["contract_type"] = "Alternance"

                # Description snippet
                desc_el = result.select_one(".description") or result.select_one("p")
                if desc_el:
                    offer["description"] = desc_el.get_text(strip=True)

                # Date
                date_el = result.select_one("[data-date]") or result.select_one("time") or result.select_one(".date")
                if date_el:
                    offer["date_text"] = date_el.get_text(strip=True)
                    offer["datetime"] = date_el.get("datetime", "")

                if offer.get("title"):
                    offers.append(offer)

            except Exception as e:
                self.logger.debug(f"Error parsing result: {e}")
                continue

        return offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a raw scraped offer."""
        try:
            title = raw_data.get("title", "")
            if not title:
                return None

            company = raw_data.get("company", "Entreprise confidentielle")
            description = raw_data.get("description", "")

            # School detection
            is_school = is_school_offer(company, description)

            # Location
            location = raw_data.get("location", "")

            # Publication date
            pub_date = None
            dt = raw_data.get("datetime", "")
            if dt:
                try:
                    pub_date = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    pass
            if not pub_date:
                date_text = raw_data.get("date_text", "")
                pub_date = parse_french_date(date_text)
                
            if not pub_date:
                pub_date = datetime.utcnow()

            # Profile
            profile = normalize_profile(raw_data.get("profile", ""))

            # URL - Force direct detail URL for consistency and reliability
            offer_id = raw_data.get("_id", "")
            if offer_id:
                url = f"https://candidat.francetravail.fr/offres/recherche/detail/{offer_id}"
            else:
                url = raw_data.get("url", "")

            # Salary
            salary = raw_data.get("salary", "")
            salary_min, salary_max = normalize_salary(salary)

            clean_loc = clean_text(location)
            enriched_loc, dept = enrich_location(clean_loc)

            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or clean_loc,
                "department": dept,
                "contract_type": raw_data.get("contract_type", "Alternance"),
                "salary": salary or None,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "description": clean_text(description),
                "profile": profile,
                "category": None,
                "publication_date": pub_date,
                "source": "francetravail",
                "url": url,
                "source_id": f"ft_{offer_id}" if offer_id else None,
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.warning(f"Error parsing France Travail offer: {e}")
            return None
