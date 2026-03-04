"""
Scraper for Apec platform.
Targeting alternance offers (Bac+3 to Bac+5).
"""

import asyncio
import json
import httpx
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_profile, normalize_salary

class ApecScraper(BaseScraper):
    """Scraper for Apec.fr."""

    BASE_URL = "https://www.apec.fr"
    # Apec's main REST API uses this endpoint for searches
    SEARCH_API_URL = "https://www.apec.fr/api/emploi/v1/recherche/"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
    }

    def __init__(self):
        super().__init__("apec")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """Scrape Apec alternance offers."""
        keywords = kwargs.get("keywords", ["alternance", "apprentissage", "contrat de professionnalisation"])
        max_pages = kwargs.get("max_pages", 2)
        
        all_offers = []
        seen_ids = set()

        async with httpx.AsyncClient(timeout=30.0, headers=self.HEADERS, follow_redirects=True) as client:
            semaphore = asyncio.Semaphore(2)

            async def fetch_page(kw: str, p: int):
                async with semaphore:
                    return await self._scrape_page(client, kw, p)

            tasks = []
            for keyword in keywords:
                for page in range(max_pages):
                    tasks.append(fetch_page(keyword, page))
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            initial_offers = []
            
            for page_results in results:
                if isinstance(page_results, Exception) or not page_results:
                    continue
                for raw in page_results:
                    offer_id = str(raw.get("numeroOffre", ""))
                    if offer_id and offer_id not in seen_ids:
                        seen_ids.add(offer_id)
                        initial_offers.append(raw)
            
            self.logger.info(f"Apec collected {len(initial_offers)} raw items.")

            # Depending on the payload, Apec returns the full description directly.
            # If so, we avoid a second round of fetching.
            # For robustness, we will map it nicely.
            all_offers = initial_offers

        self.logger.info(f"Apec finished with {len(all_offers)} offers")
        return all_offers

    async def _scrape_page(self, client: httpx.AsyncClient, keyword: str, page: int) -> List[Dict[str, Any]]:
        """Fetch a page via Apec GraphQL/REST API or DOM if backend changes."""
        # 143685 = Contrat d'alternance / professionnalisation / apprentissage
        payload = {
            "pagination": {"page": page, "motsCles": keyword, "count": 20},
            "typesConvention": ["143685"],
            "statutPoste": []
        }
        try:
            res = await client.post(self.SEARCH_API_URL, json=payload)
            if res.status_code == 200:
                data = res.json()
                # Offres array is often inside 'resultats' or directly array
                # Depending on the specific iteration of the apec API mapping
                if "resultats" in data:
                    return data["resultats"]
                elif isinstance(data, list):
                    return data
            else:
                self.logger.warning(f"Apec HTTP {res.status_code}. API blocking likely, attempting HTML fallback...")
                return await self._scrape_html_fallback(client, keyword, page)
                
        except Exception as e:
            self.logger.error(f"Apec scrape logic error: {e}")
            return []
        
        return []

    async def _scrape_html_fallback(self, client: httpx.AsyncClient, keyword: str, page: int) -> List[Dict[str, Any]]:
        """Fallback to standard web scraping if API is restricted without session tokens."""
        html_url = f"{self.BASE_URL}/candidat/recherche-emploi.html/emploi"
        params = {"motsCles": keyword, "typesConvention": "143685", "page": str(page)}
        
        try:
            # Drop content-type for HTML request
            headers = {**self.HEADERS, "Accept": "text/html"}
            res = await client.get(html_url, params=params, headers=headers)
            if res.status_code != 200:
                 return []
                 
            soup = BeautifulSoup(res.text, "html.parser")
            results = []
            
            # Scrape basic cards (Apec uses diverse class names depending on redesigns)
            cards = soup.select(".offer-card, .container-result a, div[class*='offer'] article")
            
            for card in cards:
                try:
                    title_el = card.select_one("h2, h3, .card-title")
                    title = title_el.get_text(strip=True) if title_el else ""
                    if not title:
                        continue
                        
                    company_el = card.select_one(".company-name, p.mb-0.text-truncate")
                    company = company_el.get_text(strip=True) if company_el else "Apec - Entreprise"
                    
                    loc_el = card.select_one(".offer-location, i.fa-map-marker-alt + span")
                    location = loc_el.get_text(strip=True) if loc_el else ""
                    
                    desc_el = card.select_one(".offer-desc, p.text-wrap")
                    desc = desc_el.get_text(strip=True) if desc_el else "Voir l'offre pour plus de détails."
                    
                    link_el = card if card.name == "a" else card.find("a")
                    href = link_el.get("href", "") if link_el else ""
                    url = href if href.startswith("http") else f"{self.BASE_URL}{href}"
                    
                    # extract id from URL or random hash
                    import re
                    match = re.search(r"detail-offre/(\d+)", href)
                    offer_id = match.group(1) if match else str(hash(url))
                    
                    results.append({
                        "intitule": title,
                        "nomEntreprise": company,
                        "lieu": location,
                        "texteBrut": desc,
                        "url": url,
                        "numeroOffre": offer_id,
                        "typeContrat": "Alternance"
                    })
                except Exception as e:
                    continue
            return results
        except Exception as e:
            return []

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        title = raw_data.get("intitule", "")
        if not title:
            return None
            
        company = raw_data.get("nomEntreprise", "")
        # Apec handles locations differently (sometimes a dict, sometimes flat)
        location_data = raw_data.get("lieu") or raw_data.get("lieux", [{"libelle": ""}])
        location = location_data[0].get("libelle", "") if isinstance(location_data, list) and location_data else str(location_data)
        
        clean_loc = clean_text(location)
        enriched_loc, dept = enrich_location(clean_loc)

        salary_text = raw_data.get("salaireTexte", "")
        salary_min, salary_max = None, None
        if salary_text:
            try:
                salary_min, salary_max = normalize_salary(salary_text)
            except Exception:
                pass

        desc = raw_data.get("texteBrut", raw_data.get("texteHtml", "Voir les détails sur Apec.fr"))
        desc_clean = clean_text(desc)
        
        # Determine URLs
        url = raw_data.get("url")
        offer_num = str(raw_data.get("numeroOffre", ""))
        if not url and offer_num:
            url = f"{self.BASE_URL}/candidat/recherche-emploi.html/emploi/detail-offre/{offer_num}"

        date_str = raw_data.get("datePublication")
        pub_date = datetime.utcnow()
        if date_str:
            try:
                # typically "2024-03-01T10:00:00"
                pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00").split("+")[0])
            except Exception:
                pass

        is_school = is_school_offer(clean_text(company), desc_clean)

        return {
            "title": clean_text(title),
            "company": clean_text(company) if clean_text(company) else "Apec Emploi",
            "location": enriched_loc or clean_loc,
            "department": dept,
            "contract_type": "Alternance",
            "salary": salary_text if salary_text else None,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "description": desc_clean,
            "profile": None,
            "category": None,
            "publication_date": pub_date,
            "source": "apec",
            "url": url,
            "source_id": f"apec_{offer_num}",
            "is_school": is_school,
        }
