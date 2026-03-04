import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from curl_cffi.requests import AsyncSession
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_salary

class MeteojobScraper(BaseScraper):
    """Scraper for Meteojob using their official search API."""

    BASE_URL = "https://www.meteojob.com"
    # New working API endpoint discovered by subagent
    SEARCH_API_URL = "https://www.meteojob.com/api/joboffers/search"

    def __init__(self):
        super().__init__("meteojob")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        # Instead of searching specific keywords, we want ALL alternance offers.
        search_terms = [""]
        all_offers = []
        seen_ids = set()

        async with AsyncSession(impersonate="chrome110") as session:
            for term in search_terms:
                self.logger.info(f"Meteojob: Searching all alternances (no keyword restrictions...)")
                
                # Fetch up to 50 pages (2500 results)
                for page in range(1, 51):
                    params = {
                        "serjobsearch": "true",
                        "scoringVersion": "SERJOBSEARCH",
                        "what": term,
                        "where": "France",
                        "sorting": "SCORING",
                        "page": page,
                        "limit": 50,
                        "expandLocations": "true",
                        "facetSince": 30,
                        "facetContract": "APPRENTICE"
                    }
                    
                    try:
                        response = await session.get(
                            self.SEARCH_API_URL, 
                            params=params,
                            headers={
                                "x-meteojob-requester": "candidate-front",
                                "Referer": f"https://www.meteojob.com/jobs?what={term}"
                            }
                        )
                        
                        if response.status_code == 200:
                            data = response.json()
                            content = data.get("content", [])
                            if not content:
                                break
                                
                            self.logger.info(f"Meteojob: Found {len(content)} items on page {page} for '{term}'")
                            for item in content:
                                oid = item.get("id")
                                if oid and oid not in seen_ids:
                                    seen_ids.add(oid)
                                    all_offers.append(item)
                        else:
                            self.logger.warning(f"Meteojob search failed with status {response.status_code} for '{term}' at page {page}")
                            break
                    except Exception as e:
                        self.logger.error(f"Error in Meteojob scrape for '{term}' at page {page}: {e}")
                        break
                    
                    await asyncio.sleep(1)

        return all_offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            oid = raw_data.get("id")
            if not oid:
                return None
                
            title = raw_data.get("title", "")
            company = raw_data.get("company", {}).get("name", "Entreprise confidentielle")
            
            # Locations (plural now)
            locations_list = raw_data.get("locations", [])
            location = ""
            if locations_list:
                # Use the 'name' field from the first priority location or the first one
                loc_item = next((l for l in locations_list if l.get("priority")), locations_list[0])
                location = loc_item.get("name", "")
            
            description = raw_data.get("description", "")
            
            # Publication date
            pub_date = None
            date_str = raw_data.get("publicationDate")
            if date_str:
                try:
                    # Meteojob date can be ISO format
                    pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                except:
                    pass
            
            # URL (dictionary now)
            url_data = raw_data.get("url", {})
            href = url_data.get("jobOffer") or url_data.get("jobOfferShort")
            if href:
                url = f"https://www.meteojob.com{href}" if href.startswith("/") else href
            else:
                # Fallback if URL data is missing
                slug = raw_data.get("slug", "")
                url = f"https://www.meteojob.com/offres-emploi/{slug}-{oid}"
            
            # Salary
            salary_data = raw_data.get("salary", {})
            salary_text = salary_data.get("displaySalary")
            if salary_text == "PROFILE":
                salary_text = "Selon profil"
            elif not salary_text:
                salary_text = salary_data.get("text")

            is_school = is_school_offer(company, description)
            cloc = clean_text(location)
            enriched_loc, dept = enrich_location(cloc)
            
            return {
                "title": clean_text(title),
                "company": clean_text(company),
                "location": enriched_loc or cloc,
                "department": dept,
                "contract_type": "Alternance",
                "salary": salary_text,
                "description": clean_text(description),
                "profile": None,
                "category": None,
                "publication_date": pub_date or datetime.now(),
                "source": "meteojob",
                "url": url,
                "source_id": f"meteojob_{oid}",
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.warning(f"Error parsing Meteojob offer: {e}")
            return None

