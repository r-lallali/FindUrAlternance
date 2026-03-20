"""
Scraper for Cadremploi.fr.
Extracts offers by parsing the Next.js '__NEXT_DATA__' JSON object.
"""

import asyncio
import json
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from curl_cffi.requests import AsyncSession

from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location

class CadremploiScraper(BaseScraper):
    """Scraper for Cadremploi using HTML-embedded React state."""

    BASE_URL = "https://www.cadremploi.fr"

    def __init__(self):
        super().__init__("cadremploi")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        all_offers = []
        seen_ids = set()
        max_pages = kwargs.get("max_pages", 5)

        # Keyword queries to also catch CDD/Stage that are actually alternance
        search_terms = ["alternance", "apprentissage", "contrat de professionnalisation"]

        async with AsyncSession(impersonate="chrome110") as session:
            for term in search_terms:
                self.logger.info(f"Cadremploi: Searching for '{term}'")
                
                for page in range(1, max_pages + 1):
                    # contrat=4 is Alternance usually, but we keep it broad and let our NLP filter.
                    url = f"{self.BASE_URL}/emploi/liste_offres?q={term}&page={page}"
                    
                    try:
                        resp = await session.get(url, timeout=15)
                        if resp.status_code != 200:
                            self.logger.warning(f"Cadremploi search HTTP {resp.status_code} for page {page}")
                            break
                        
                        match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', resp.text, re.DOTALL)
                        if not match:
                            self.logger.warning(f"Cadremploi: No __NEXT_DATA__ found on page {page}")
                            break

                        next_data = json.loads(match.group(1))
                        
                        offers = self._extract_offers_from_json(next_data)
                        if not offers:
                            self.logger.info(f"Cadremploi: No more offers found on page {page}")
                            break
                            
                        added = 0
                        for offer in offers:
                            oid = str(offer.get("id") or offer.get("reference") or offer.get("job_id") or "")
                            if oid and oid not in seen_ids:
                                seen_ids.add(oid)
                                all_offers.append(offer)
                                added += 1
                                
                        if added == 0:
                            break # No new offers, probably reached end
                            
                        await asyncio.sleep(1) # delay
                        
                    except Exception as e:
                        self.logger.error(f"Error scraping Cadremploi page {page} for '{term}': {e}")
                        break

        self.logger.info(f"Cadremploi collected {len(all_offers)} raw items")
        return all_offers

    def _extract_offers_from_json(self, data: Any) -> List[Dict[str, Any]]:
        """Recursively find lists of objects that look like job offers."""
        offers = []
        
        def search(node):
            if isinstance(node, dict):
                # An offer object typically has 'id', 'title', 'company'
                if ("title" in node or "intitule" in node) and ("company" in node or "companyName" in node or "entreprise" in node) and ("id" in node or "reference" in node or "job_id" in node):
                    offers.append(node)
                else:
                    for v in node.values():
                        search(v)
            elif isinstance(node, list):
                for item in node:
                    search(item)
                    
        search(data)
        
        # Filter purely false positives (like recommended searches, etc)
        valid_offers = [o for o in offers if o.get("description") or o.get("url") or o.get("id")]
        
        # Prevent returning the same offer multiple times if it appears in different parts of the state
        unique_offers = {}
        for o in valid_offers:
            oid = str(o.get("id") or o.get("reference") or o.get("url") or "")
            if oid and oid not in unique_offers:
                unique_offers[oid] = o
                
        return list(unique_offers.values())

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            oid = str(raw_data.get("id") or raw_data.get("reference") or raw_data.get("job_id") or "")
            if not oid:
                return None

            title = raw_data.get("title") or raw_data.get("name") or raw_data.get("intitule") or ""
            
            # Extract Company
            company_info = raw_data.get("company") or raw_data.get("entreprise") or {}
            if isinstance(company_info, str):
                company = company_info
            else:
                company = company_info.get("name") or raw_data.get("companyName") or "Entreprise confidentielle"
                
            # If company is "Cadremploi" or "Figaro", hide it so it gets re-extracted from description
            company_lower = company.lower()
            if "cadremploi" in company_lower or "cadre emploi" in company_lower or "figaro" in company_lower:
                company = "Entreprise confidentielle"

            # URL
            url = raw_data.get("url") or raw_data.get("link")
            if url and url.startswith("/"):
                url = self.BASE_URL + url
            elif not url:
                url = f"{self.BASE_URL}/emploi/offre-detail/{oid}"
                
            # Location
            location_info = raw_data.get("location") or raw_data.get("lieu") or {}
            if isinstance(location_info, str):
                location = location_info
            else:
                location = location_info.get("name") or location_info.get("city") or raw_data.get("city") or ""
                
            cloc = clean_text(location)
            enriched_loc, dept = enrich_location(cloc)

            # Description
            description = raw_data.get("description") or raw_data.get("texte") or ""
            
            # Contract type
            contract_type = raw_data.get("contractType") or "Alternance"
            
            # Pub Date
            date_str = raw_data.get("publicationDate") or raw_data.get("datePublication")
            pub_date = None
            if date_str:
                try:
                    pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                except BaseException:
                    pass
            if not pub_date:
                pub_date = datetime.utcnow()

            is_school = is_school_offer(clean_text(company), clean_text(description), clean_text(title))

            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or cloc,
                "department": dept,
                "contract_type": clean_text(contract_type) or "Alternance",
                "salary": raw_data.get("salary") or raw_data.get("salaire"),
                "description": clean_text(description, preserve_newlines=True) or "Voir l'offre pour la description",
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
