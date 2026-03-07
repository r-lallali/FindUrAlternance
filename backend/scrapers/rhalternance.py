
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, parse_french_date

class RHAlternanceScraper(BaseScraper):
    """Scraper for RH Alternance (rhalternance.com)"""

    BASE_URL = "https://rhalternance.com"
    API_URL = "https://rhalternance.com/jobs/ajax"

    def __init__(self):
        super().__init__("rhalternance")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape jobs from RH Alternance using the AJAX API.
        Fetches multiple pages to get a significant volume of offers.
        """
        all_raw_offers = []
        
        async with AsyncSession(impersonate="chrome110") as session:
            # First, visit the main page to get cookies
            try:
                await session.get(self.BASE_URL + "/jobs")
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.warning(f"Failed to fetch main page for cookies: {e}")

            # Fetch up to 200 pages of results (approx 4000 offers)
            # User noted there are many offers and they want older ones.
            max_pages = kwargs.get("max_pages", 200) 
            
            for page in range(1, max_pages + 1):
                try:
                    self.logger.info(f"RH Alternance: Fetching all sectors API page {page}...")
                    payload = {
                        "userCity": "0",
                        "category": "0", # 0 = All categories
                        "page": str(page)
                    }
                    headers = {
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": f"{self.BASE_URL}/jobs",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
                    }
                    
                    response = await session.post(self.API_URL, data=payload, headers=headers)
                    if response.status_code != 200:
                        self.logger.error(f"Failed to fetch RH Alternance API: {response.status_code}")
                        break

                    data = response.json()
                    html_content = data.get("html", "")
                    if not html_content or "job-listing" not in html_content:
                        self.logger.info("RH Alternance: No more jobs found or limit reached.")
                        break

                    soup = BeautifulSoup(html_content, "html.parser")
                    job_listings = soup.select(".job-listing")
                    
                    if not job_listings:
                        break

                    self.logger.info(f"RH Alternance: Found {len(job_listings)} jobs on page {page}.")
                    
                    for job in job_listings:
                        try:
                            href = job.get("href")
                            if not href: continue
                            
                            full_url = href if href.startswith("http") else self.BASE_URL + href
                            
                            # Extract basic info from card
                            title_el = job.select_one(".job-listing-title")
                            title = title_el.get_text(strip=True) if title_el else ""
                            
                            # Footer items: 1:Company, 2:Location, 3:Contract, 4:Date
                            footer_items = job.select(".job-listing-footer li")
                            company = ""
                            location = ""
                            date_text = ""
                            
                            if len(footer_items) >= 1:
                                company = footer_items[0].get_text(strip=True)
                            if len(footer_items) >= 2:
                                location = footer_items[1].get_text(strip=True)
                            if len(footer_items) >= 4:
                                date_text = footer_items[3].get_text(strip=True)
                            
                            sid = None
                            if '-' in full_url:
                                sid = f"rhalternance_{full_url.split('-')[-1]}"
                            else:
                                sid = f"rhalternance_{abs(hash(full_url))}"
                            
                            raw_offer = {
                                "title": title,
                                "company": company,
                                "location": location,
                                "date_text": date_text,
                                "url": full_url,
                                "source_id": sid,
                                "description": ""
                            }
                            all_raw_offers.append(raw_offer)
                        except Exception as e:
                            self.logger.warning(f"Error parsing RH Alternance job card: {e}")
                            continue
                            
                    await asyncio.sleep(0.3)
                        
                except Exception as e:
                    self.logger.error(f"Error scraping RH Alternance API page {page}: {e}")
                    break

            # Deduplicate items before fetching descriptions
            unique_offers = {}
            for o in all_raw_offers:
                unique_offers[o["source_id"]] = o
            
            final_offers = list(unique_offers.values())
            self.logger.info(f"RH Alternance: Fetching descriptions for {len(final_offers)} unique offers...")
            
            # Use chunks to fetch descriptions in parallel
            semaphore = asyncio.Semaphore(10)

            async def fetch_description(raw_offer):
                async with semaphore:
                    for retry in range(2):
                        try:
                            detail_res = await session.get(raw_offer["url"], timeout=30)
                            if detail_res.status_code == 200:
                                detail_soup = BeautifulSoup(detail_res.text, "html.parser")
                                # Updated description detection logic
                                sections = detail_soup.select(".single-page-section")
                                desc_section = None
                                
                                # Priority 1: Section with descriptive title
                                for sec in sections:
                                    h3 = sec.select_one("h3")
                                    if h3:
                                        h3_text = h3.get_text().lower()
                                        if any(kw in h3_text for kw in ["descriptif", "détail", "missions", "profil", "description"]):
                                            desc_section = sec
                                            break
                                            
                                # Priority 2: Section with most text content
                                if not desc_section and sections:
                                    desc_section = max(sections, key=lambda s: len(s.get_text()))
                                
                                if desc_section:
                                    raw_offer["description"] = desc_section.get_text(separator="\n", strip=True)
                                return
                            await asyncio.sleep(0.5)
                        except Exception as e:
                            await asyncio.sleep(1)

            tasks = [fetch_description(offer) for offer in final_offers]
            await asyncio.gather(*tasks)

        return final_offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            title = raw_data.get("title")
            company = raw_data.get("company")
            description = raw_data.get("description")
            url = raw_data.get("url")
            
            if not title or not url:
                return None
            
            # Clean description
            clean_desc = clean_text(description, preserve_newlines=True)
            
            # Date parsing
            raw_date = raw_data.get("date_text", "")
            pub_date = parse_french_date(raw_date)
            if not pub_date:
                 pub_date = datetime.utcnow()
            
            # School check
            is_school = is_school_offer(company, clean_desc)
            
            # Location cleaning
            cloc = clean_text(raw_data.get("location"))
            enriched_loc, dept = enrich_location(cloc)
            
            return {
                "title": clean_text(title),
                "company": clean_text(company) or "Entreprise",
                "location": enriched_loc or cloc,
                "department": dept,
                "contract_type": "Alternance",
                "salary": None,
                "description": clean_desc,
                "profile": None,
                "category": None,
                "publication_date": pub_date,
                "source": "rhalternance",
                "url": url,
                "source_id": raw_data.get("source_id"),
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.warning(f"Error parsing RH Alternance offer: {e}")
            return None
