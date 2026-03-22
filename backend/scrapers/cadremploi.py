"""
Scraper for Cadremploi.fr.
Extracts offers by parsing the Nuxt payload (devalue format) embedded in the page HTML.
"""

import asyncio
import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from curl_cffi.requests import AsyncSession

from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location


def _revive_nuxt(payload: list) -> Any:
    """Deserialize Nuxt/devalue serialized payload into plain Python objects."""
    cache: Dict[int, Any] = {}
    resolving: set = set()

    def resolve(idx):
        if not isinstance(idx, int):
            return idx
        if idx in cache:
            return cache[idx]
        if idx in resolving:
            return None  # cycle guard
        resolving.add(idx)
        node = payload[idx]
        if (
            isinstance(node, list)
            and len(node) == 2
            and isinstance(node[0], str)
            and node[0] in ("ShallowReactive", "Reactive", "Ref", "ShallowRef")
        ):
            result = resolve(node[1])
        elif isinstance(node, dict):
            result = {k: resolve(v) for k, v in node.items()}
        elif isinstance(node, list):
            result = [resolve(i) for i in node]
        else:
            result = node
        resolving.discard(idx)
        cache[idx] = result
        return result

    return resolve(1)


class CadremploiScraper(BaseScraper):
    """Scraper for Cadremploi using Nuxt SSR payload embedded in HTML."""

    BASE_URL = "https://www.cadremploi.fr"

    def __init__(self):
        super().__init__("cadremploi")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        all_offers = []
        seen_ids: set = set()
        max_pages = kwargs.get("max_pages", 5)

        search_terms = ["alternance", "apprentissage", "contrat de professionnalisation"]

        async with AsyncSession(impersonate="chrome110") as session:
            for term in search_terms:
                self.logger.info(f"Cadremploi: Searching for '{term}'")

                for page in range(1, max_pages + 1):
                    url = f"{self.BASE_URL}/emploi/liste_offres?motsCles={term}&page={page}"

                    try:
                        resp = await session.get(url, timeout=20)
                        if resp.status_code != 200:
                            self.logger.warning(
                                f"Cadremploi search HTTP {resp.status_code} for page {page}"
                            )
                            break

                        offers = self._extract_offers(resp.text)
                        if not offers:
                            self.logger.info(
                                f"Cadremploi: No offers found on page {page} for '{term}'"
                            )
                            break

                        added = 0
                        for offer in offers:
                            oid = str(offer.get("id") or "")
                            if oid and oid not in seen_ids:
                                seen_ids.add(oid)
                                all_offers.append(offer)
                                added += 1

                        if added == 0:
                            break

                        await asyncio.sleep(1)

                    except Exception as e:
                        self.logger.error(
                            f"Error scraping Cadremploi page {page} for '{term}': {e}"
                        )
                        break

        self.logger.info(f"Cadremploi collected {len(all_offers)} raw items")
        return all_offers

    def _extract_offers(self, html: str) -> List[Dict[str, Any]]:
        """Extract job postings from the Nuxt SSR payload in the HTML."""
        # The payload is in an inline <script> tag as a JSON array (devalue format)
        scripts = re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL)

        for script in scripts:
            if not script.strip().startswith("["):
                continue
            try:
                payload = json.loads(script)
            except (json.JSONDecodeError, ValueError):
                continue

            if not (isinstance(payload, list) and len(payload) > 2):
                continue

            # Check it's the Nuxt devalue payload
            first = payload[0] if payload else None
            if not (isinstance(first, list) and first and first[0] == "ShallowReactive"):
                continue

            try:
                data = _revive_nuxt(payload)
                jobs_list = data.get("data", {}).get("jobs-list", {})
                jobs = jobs_list.get("jobPostingsExpanded") or []
                if isinstance(jobs, list):
                    return jobs
            except Exception as e:
                self.logger.debug(f"Cadremploi: Failed to revive Nuxt payload: {e}")

        return []

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            oid = str(raw_data.get("id") or "")
            if not oid:
                return None

            title = raw_data.get("title") or ""

            # Company
            company_info = raw_data.get("company") or {}
            if isinstance(company_info, str):
                company = company_info
            else:
                company = company_info.get("name") or "Entreprise confidentielle"

            company_lower = company.lower()
            if "cadremploi" in company_lower or "figaro" in company_lower:
                company = "Entreprise confidentielle"

            # URL
            url = raw_data.get("url") or ""
            if url.startswith("/"):
                url = self.BASE_URL + url
            if not url:
                url = f"{self.BASE_URL}/emploi/detail_offre?offreId={oid}"

            # Location
            location = raw_data.get("location") or ""
            if isinstance(location, dict):
                location = location.get("name") or location.get("city") or ""
            cloc = clean_text(str(location))
            enriched_loc, dept = enrich_location(cloc)

            # Description (snippet only — full description requires a detail page fetch)
            description = raw_data.get("snippet") or ""

            # Contract type
            contract_type = raw_data.get("contract") or "Alternance"

            # Salary
            salary = raw_data.get("salary") or None

            # Publication date (e.g. "Publiée il y a 1 heure" — no ISO date available here)
            pub_date = datetime.utcnow()

            is_school = is_school_offer(
                clean_text(company), clean_text(description), clean_text(title)
            )

            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or cloc,
                "department": dept,
                "contract_type": clean_text(str(contract_type)) or "Alternance",
                "salary": salary,
                "description": clean_text(description, preserve_newlines=True)
                or "Voir l'offre pour la description",
                "profile": None,
                "category": None,
                "publication_date": pub_date,
                "source": "cadremploi",
                "url": url,
                "source_id": f"cadremploi_{oid}",
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.debug(f"Cadremploi parse error: {e}")
            return None
