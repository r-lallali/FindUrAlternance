"""
Scraper for LinkedIn Jobs via public search.

Scrapes LinkedIn's public job search page (no authentication required).
Uses the guest job search which is publicly accessible.
"""

import asyncio
import re
import json
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_profile, normalize_salary, parse_french_date


class LinkedInScraper(BaseScraper):
    """Scraper for LinkedIn public job search."""

    BASE_URL = "https://www.linkedin.com"
    # LinkedIn's guest job search API
    SEARCH_URL = f"{BASE_URL}/jobs-guest/jobs/api/seeMoreJobPostings/search"
    PUBLIC_URL = f"{BASE_URL}/jobs/search"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
    }

    SEARCH_TERMS = [
        "alternance",
        "apprentissage",
    ]

    # LinkedIn location GeoIds for French cities
    GEO_IDS = {
        "France": "105015875",
        "Paris": "105526943",
        "Lyon": "100727962",
        "Marseille": "102816511",
        "Toulouse": "104885321",
        "Lille": "105085655",
    }

    def __init__(self):
        super().__init__("linkedin")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape alternance offers from LinkedIn.

        Keyword args:
            search_terms: list - Search keywords
            max_pages: int - Max pages per search (default: 2)
            geo_id: str - LinkedIn geo ID (default: France)
        """
        search_terms = kwargs.get("search_terms", self.SEARCH_TERMS[:5])
        max_pages = kwargs.get("max_pages", 2)
        geo_id = kwargs.get("geo_id", self.GEO_IDS["France"])

        all_offers = []
        seen_ids = set()

        async with httpx.AsyncClient(
            timeout=30.0,
            headers=self.HEADERS,
            follow_redirects=True,
        ) as client:
            for term in search_terms:
                for page in range(max_pages):
                    try:
                        start = page * 25  # LinkedIn uses 25 per page
                        offers = await self._search_page(client, term, geo_id, start)

                        # Fetch descriptions with concurrency limit to avoid 429
                        semaphore = asyncio.Semaphore(5)

                        async def enrich_desc(off):
                            j_id = off.get("_id")
                            if j_id:
                                async with semaphore:
                                    off["description"] = await self._fetch_description(client, j_id)
                            return off

                        tasks = [enrich_desc(o) for o in offers]
                        enriched_offers = await asyncio.gather(*tasks)

                        for offer in enriched_offers:
                            oid = offer.get("_id", "")
                            if oid and oid not in seen_ids:
                                seen_ids.add(oid)
                                all_offers.append(offer)

                        # Rate limiting (reduced for speed, handled by semaphore)
                        await asyncio.sleep(0.5)
                    except Exception as e:
                        self.logger.warning(f"Error on LinkedIn page {page} for '{term}': {e}")

        self.logger.info(f"LinkedIn: {len(all_offers)} unique offers collected")
        return all_offers

    async def _search_page(
        self, client: httpx.AsyncClient, keyword: str, geo_id: str, start: int
    ) -> List[Dict[str, Any]]:
        """Scrape a LinkedIn job search results page."""
        params = {
            "keywords": keyword,
            "location": "France",
            "geoId": geo_id,
            "start": start,
            "f_TPR": "r604800",  # Past week
            "position": 1,
            "pageNum": 0,
        }

        try:
            # Try the guest API endpoint first
            response = await client.get(self.SEARCH_URL, params=params)

            if response.status_code == 200:
                return self._parse_search_page(response.text)

            # Fallback to the public search page
            self.logger.debug(f"Guest API returned {response.status_code}, trying public URL")
            response = await client.get(self.PUBLIC_URL, params={
                "keywords": keyword,
                "location": "France",
                "geoId": geo_id,
                "start": start,
            })

            if response.status_code == 200:
                return self._parse_search_page(response.text)

            self.logger.warning(f"LinkedIn returned {response.status_code}")
            return []

        except httpx.TimeoutException:
            self.logger.warning(f"Timeout on LinkedIn for '{keyword}'")
            return []

    async def _fetch_description(self, client: httpx.AsyncClient, job_id: str) -> Optional[str]:
        """Fetch the job description from its LinkedIn public page."""
        try:
            url = f"https://www.linkedin.com/jobs/view/{job_id}/"
            response = await client.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                desc_el = (
                    soup.select_one("div.description__text")
                    or soup.select_one(".show-more-less-html__markup")
                    or soup.select_one(".description__text")
                )
                if desc_el:
                    return desc_el.get_text(separator="\n", strip=True)
        except Exception as e:
            self.logger.debug(f"Error fetching description for {job_id}: {e}")
        return None

    def _parse_search_page(self, html: str) -> List[Dict[str, Any]]:
        """Parse LinkedIn search results HTML."""
        soup = BeautifulSoup(html, "html.parser")
        offers = []

        # LinkedIn guest API returns job cards in various formats
        job_cards = (
            soup.select("li div.base-card")
            or soup.select("div.base-card")
            or soup.select("ul.jobs-search__results-list li")
            or soup.select(".job-search-card")
            or soup.select("li")
        )

        for card in job_cards:
            try:
                offer = {}

                # Job ID from data attributes or href
                data_id = card.get("data-entity-urn", "")
                if data_id:
                    # Extract numeric ID from URN: urn:li:jobPosting:1234567
                    match = re.search(r"(\d+)$", data_id)
                    if match:
                        offer["_id"] = match.group(1)

                if not offer.get("_id"):
                    link = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/view/']")
                    if link:
                        href = link.get("href", "")
                        match = re.search(r"(?:/|-)(\d{8,})(?:\?|/|$)", href)
                        if match:
                            offer["_id"] = match.group(1)
                        offer["url"] = href.split("?")[0] if href else ""

                if not offer.get("_id"):
                    continue  # Skip if we can't identify the job

                # Title
                title_el = (
                    card.select_one("h3.base-search-card__title")
                    or card.select_one("h3")
                    or card.select_one(".base-card__title")
                    or card.select_one("a.base-card__full-link")
                )
                if title_el:
                    offer["title"] = title_el.get_text(strip=True)

                # URL (if not already set)
                if not offer.get("url"):
                    link = card.select_one("a.base-card__full-link") or card.select_one("a[href*='/jobs/']")
                    if link:
                        offer["url"] = link.get("href", "").split("?")[0]

                # Company
                company_el = (
                    card.select_one("h4.base-search-card__subtitle a")
                    or card.select_one("h4.base-search-card__subtitle")
                    or card.select_one("h4")
                    or card.select_one("a.hidden-nested-link")
                )
                if company_el:
                    offer["company"] = company_el.get_text(strip=True)

                # Location
                loc_el = (
                    card.select_one("span.job-search-card__location")
                    or card.select_one(".base-search-card__metadata span")
                    or card.select_one(".job-result-card__location")
                )
                if loc_el:
                    offer["location"] = loc_el.get_text(strip=True)

                # Date
                time_el = card.select_one("time") or card.select_one("time.job-search-card__listdate")
                if time_el:
                    offer["datetime"] = time_el.get("datetime", "")
                    offer["date_text"] = time_el.get_text(strip=True)

                # Company logo for potential filtering
                img_el = card.select_one("img")
                if img_el:
                    offer["company_logo"] = img_el.get("data-delayed-url", "") or img_el.get("src", "")

                if offer.get("title"):
                    offers.append(offer)

            except Exception as e:
                self.logger.debug(f"Error parsing LinkedIn card: {e}")
                continue

        return offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a raw scraped LinkedIn offer."""
        try:
            title = raw_data.get("title", "")
            if not title:
                return None

            company = raw_data.get("company", "")
            if not company:
                company = "Entreprise non renseignée"

            # School detection
            is_school = is_school_offer(company, title)

            # Location
            location = raw_data.get("location", "")

            # Publication date
            pub_date = None
            dt = raw_data.get("datetime", "")
            if dt:
                try:
                    pub_date = datetime.fromisoformat(dt.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    try:
                        pub_date = datetime.strptime(dt[:10], "%Y-%m-%d")
                    except (ValueError, TypeError):
                        pass

            if not pub_date:
                pub_date = parse_french_date(raw_data.get("date_text", ""))

            # Try to extract profile from title
            profile = normalize_profile(title)

            offer_id = raw_data.get("_id", "")
            url = raw_data.get("url", "")
            if not url and offer_id:
                url = f"https://www.linkedin.com/jobs/view/{offer_id}"

            clean_loc = clean_text(location)
            enriched_loc, dept = enrich_location(clean_loc)

            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or clean_loc,
                "department": dept,
                "contract_type": "Alternance",
                "salary": None,
                "salary_min": None,
                "salary_max": None,
                "description": clean_text(raw_data.get("description", "")) or None,
                "profile": profile,
                "category": None,
                "publication_date": pub_date,
                "source": "linkedin",
                "url": url,
                "source_id": f"li_{offer_id}" if offer_id else None,
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.warning(f"Error parsing LinkedIn offer: {e}")
            return None

    def _parse_relative_date(self, text: str) -> Optional[datetime]:
        """Parse LinkedIn's relative date strings."""
        if not text:
            return None

        text = text.lower().strip()
        now = datetime.utcnow()

        if "aujourd" in text or "just" in text or "il y a 0" in text:
            return now

        # French: "il y a X jours/heures/semaines"
        match = re.search(r"(\d+)\s*(jour|day|heure|hour|semaine|week|mois|month)", text)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            if "jour" in unit or "day" in unit:
                return now - timedelta(days=num)
            elif "heure" in unit or "hour" in unit:
                return now - timedelta(hours=num)
            elif "semaine" in unit or "week" in unit:
                return now - timedelta(weeks=num)
            elif "mois" in unit or "month" in unit:
                return now - timedelta(days=num * 30)

        return None
