import asyncio
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from curl_cffi.requests import AsyncSession
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_salary

class ApecScraper(BaseScraper):
    """Scraper for Apec.fr using the CMS webservices API."""

    BASE_URL = "https://www.apec.fr"
    SEARCH_API_URL = "https://www.apec.fr/cms/webservices/rechercheOffre"
    DETAIL_API_URL = "https://www.apec.fr/cms/webservices/offre/public"

    def __init__(self):
        super().__init__("apec")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        search_terms = kwargs.get("search_terms", ["alternance", "apprentissage"])
        all_offers = []
        seen_ids = set()

        async with AsyncSession(impersonate="chrome110") as session:
            for term in search_terms:
                self.logger.info(f"Apec: Searching for '{term}'")
                
                # Fetch up to 10 pages (100 results per page = 1000+ offers per term)
                # Note: Apec might cap results, but we try.
                for start_index in range(0, 1000, 100):
                    payload = {
                        "lieux": [],
                        "fonctions": [],
                        "statutPoste": [],
                        "typesContrat": [],
                        "typesConvention": ["143684", "143685", "143686", "143687", "143706"],
                        "pagination": {"range": 100, "startIndex": start_index},
                        "motsCles": term,
                        "typeClient": "CADRE",
                        "sorts": [{"type": "SCORE", "direction": "DESCENDING"}],
                        "activeFiltre": True
                    }
                    
                    try:
                        response = await session.post(
                            self.SEARCH_API_URL, 
                            json=payload,
                            headers={
                                "Accept": "application/json, text/plain, */*",
                                "Origin": "https://www.apec.fr",
                                "Referer": f"https://www.apec.fr/candidat/recherche-emploi.html/emploi?motsCles={term}"
                            }
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            results = data.get("resultats", [])
                            if not results:
                                break
                                
                            self.logger.info(f"Apec: Processing {len(results)} results from index {start_index} for '{term}'")
                            
                            for item in results:
                                oid = item.get("numeroOffre")
                                if oid and oid not in seen_ids:
                                    seen_ids.add(oid)
                                    
                                    # Fetch full details
                                    try:
                                        detail_res = await session.get(
                                            f"{self.DETAIL_API_URL}?numeroOffre={oid}",
                                            headers={"Referer": f"https://www.apec.fr/candidat/recherche-emploi.html/emploi/detail-offre/{oid}"}
                                        )
                                        if detail_res.status_code == 200:
                                            detail_data = detail_res.json()
                                            # Merge detail data into item
                                            item["full_details"] = detail_data
                                        
                                        # Tiny sleep between detail requests to avoid being blocked
                                        await asyncio.sleep(0.2)
                                    except Exception as detail_err:
                                        self.logger.debug(f"Apec: Error fetching details for {oid}: {detail_err}")
                                    
                                    all_offers.append(item)
                        else:
                            self.logger.warning(f"Apec search failed with status {response.status_code} for '{term}'")
                            break
                    except Exception as e:
                        self.logger.error(f"Error in Apec scrape for '{term}' at index {start_index}: {e}")
                        break
                    
                    await asyncio.sleep(1)

        return all_offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            oid = raw_data.get("numeroOffre")
            if not oid:
                return None
            
            full_details = raw_data.get("full_details", {})
            
            title = raw_data.get("intitule") or full_details.get("intitule") or ""
            company = raw_data.get("nomCommercial") or full_details.get("nomEntreprise") or "Entreprise confidentielle"
            location = raw_data.get("lieuTexte") or full_details.get("lieu") or ""
            
            # Combine multiple description fields for a full text
            desc_parts = [
                full_details.get("texteHtml", ""),
                full_details.get("texteHtmlProfil", ""),
                full_details.get("texteHtmlEntreprise", "")
            ]
            description = "\n".join(filter(None, desc_parts))
            
            # Fallback to snippet if no details
            if not description:
                description = raw_data.get("texteOffre", "")
            
            # Publication date
            pub_date = None
            ts = raw_data.get("datePublication") or full_details.get("datePublication")
            if ts:
                try:
                    pub_date = datetime.fromtimestamp(int(ts) / 1000.0)
                except (ValueError, TypeError):
                    pass
            
            # URL
            url = f"https://www.apec.fr/candidat/recherche-emploi.html/emploi/detail-offre/{oid}"
            
            is_school = is_school_offer(company, description)
            cloc = clean_text(location)
            enriched_loc, dept = enrich_location(cloc)
            
            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or cloc,
                "department": dept,
                "contract_type": "Alternance",
                "salary": full_details.get("salaireTexte") or raw_data.get("salaireTexte"),
                "description": clean_text(description, preserve_newlines=True),
                "profile": None,
                "category": None,
                "publication_date": pub_date or datetime.now(),
                "source": "apec",
                "url": url,
                "source_id": f"apec_{oid}",
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.warning(f"Error parsing Apec offer {oid}: {e}")
            return None


