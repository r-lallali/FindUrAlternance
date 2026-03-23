"""
Scraper for Cadremploi.fr.
Extracts offers by parsing the Nuxt payload (devalue format) embedded in the page HTML.
Fetches full descriptions from detail pages in a second pass.
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

        async with AsyncSession(impersonate="chrome120") as session:
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

            self.logger.info(f"Cadremploi collected {len(all_offers)} raw items, fetching full descriptions...")

            # Second pass: fetch full descriptions from detail pages
            semaphore = asyncio.Semaphore(5)

            async def enrich(offer):
                oid = str(offer.get("id") or "")
                if not oid:
                    return offer
                detail_url = f"{self.BASE_URL}/emploi/detail_offre?offreId={oid}"
                async with semaphore:
                    detail = await self._fetch_detail(session, detail_url)
                    if detail:
                        offer["_detail"] = detail
                return offer

            all_offers = await asyncio.gather(*[enrich(o) for o in all_offers])

        self.logger.info(f"Cadremploi finished enriching {len(all_offers)} offers")
        return list(all_offers)

    async def _fetch_detail(self, session: AsyncSession, url: str) -> Optional[Dict[str, Any]]:
        """Fetch the detail page and extract offer data from the Nuxt payload."""
        try:
            resp = await session.get(url, timeout=20)
            if resp.status_code != 200:
                return None
            scripts = re.findall(r"<script[^>]*>(.*?)</script>", resp.text, re.DOTALL)
            for script in scripts:
                s2 = script.strip()
                if not s2.startswith("["):
                    continue
                try:
                    payload = json.loads(s2)
                except (json.JSONDecodeError, ValueError):
                    continue
                try:
                    data = _revive_nuxt(payload)
                    d = data.get("data", {})
                    detail_key = next((k for k in d if k.startswith("offer-detail")), None)
                    if detail_key:
                        return d[detail_key].get("offer") or {}
                except Exception:
                    continue
        except Exception as e:
            self.logger.debug(f"Cadremploi detail fetch error for {url}: {e}")
        return None

    def _extract_offers(self, html: str) -> List[Dict[str, Any]]:
        """Extract job postings from the Nuxt SSR payload in the HTML."""
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

            # Full description from detail page (poste + profil)
            detail = raw_data.get("_detail") or {}
            poste = detail.get("poste") or {}
            profil = detail.get("profil") or {}
            full_desc = "\n\n".join(
                filter(None, [
                    poste.get("description") or "",
                    profil.get("description") or "",
                ])
            )
            description = full_desc or raw_data.get("snippet") or ""

            # Contract type
            contract_detail = detail.get("contrat") or {}
            if isinstance(contract_detail, dict):
                contract_type = (contract_detail.get("typeContrat") or {}).get("label") or raw_data.get("contract") or "Alternance"
            else:
                contract_type = raw_data.get("contract") or "Alternance"

            # Salary
            remun = detail.get("remuneration") or {}
            if isinstance(remun, dict) and not remun.get("masquer"):
                sal_min = remun.get("salaireMin")
                sal_max = remun.get("salaireMax")
                if sal_min and sal_max:
                    salary = f"{sal_min}–{sal_max} €"
                elif sal_min:
                    salary = f"{sal_min} €"
                else:
                    salary = None
            else:
                salary = None

            # Publication date from detail page (ISO format)
            pub_date = datetime.utcnow()
            date_str = detail.get("datePublication")
            if date_str:
                try:
                    pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    pass

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
                "profile": clean_text(profil.get("description") or "", preserve_newlines=True) or None,
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
