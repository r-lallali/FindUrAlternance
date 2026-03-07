
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
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
        Scrape jobs from the informatique category using the AJAX API.
        Category 26 = Informatique, internet et télécommunication
        """
        all_raw_offers = []
        
        async with AsyncSession(impersonate="chrome110") as session:
            # First, visit the main page to get cookies
            try:
                await session.get(self.BASE_URL + "/jobs")
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.warning(f"Failed to fetch main page for cookies: {e}")

            # Fetch first 3 pages of results
            for page in range(1, 4):
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
                        self.logger.info("RH Alternance: No more jobs found.")
                        break

                    soup = BeautifulSoup(html_content, "html.parser")
                    job_listings = soup.select(".job-listing")
                    
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
                            
                            sid = f"rhalternance_{full_url.split('-')[-1]}" if '-' in full_url else f"rhalternance_{hash(full_url)}"
                            
                            raw_offer = {
                                "title": title,
                                "company": company,
                                "location": location,
                                "date_text": date_text,
                                "url": full_url,
                                "source_id": sid,
                                "description": "" # Will fetch only if needed or later
                            }
                            
                            # To avoid too many requests during scrape, we could gather descriptions later
                            # or just fetch the top ones.
                            # For now, let's fetch descriptions for the first batch.
                            all_raw_offers.append(raw_offer)
                            
                        except Exception as e:
                            self.logger.warning(f"Error parsing RH Alternance job card: {e}")
                            continue
                            
                    await asyncio.sleep(1)
                        
                except Exception as e:
                    self.logger.error(f"Error scraping RH Alternance API page {page}: {e}")
                    break

            # Now fetch descriptions for the collected offers (limit to 30 to stay polite)
            self.logger.info(f"RH Alternance: Fetching descriptions for {len(all_raw_offers[:30])} offers...")
            for raw_offer in all_raw_offers[:30]:
                try:
                    detail_res = await session.get(raw_offer["url"])
                    if detail_res.status_code == 200:
                        detail_soup = BeautifulSoup(detail_res.text, "html.parser")
                        sections = detail_soup.select(".single-page-section")
                        desc_section = None
                        for sec in sections:
                            h3 = sec.select_one("h3")
                            if h3 and "descriptif" in h3.get_text().lower():
                                desc_section = sec
                                break
                        if not desc_section and sections:
                            desc_section = sections[0]
                        
                        raw_offer["description"] = desc_section.get_text(separator="\n", strip=True) if desc_section else ""
                    await asyncio.sleep(0.3)
                except Exception as e:
                    self.logger.warning(f"Error fetching description for {raw_offer['url']}: {e}")

        return all_raw_offers

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            title = raw_data.get("title")
            company = raw_data.get("company")
            description = raw_data.get("description")
            url = raw_data.get("url")
            
            if not title or not url:
                return None
                
            # Date parsing
            pub_date = parse_french_date(raw_data.get("date_text", "")) or datetime.utcnow()
            
            # School check
            is_school = is_school_offer(company, description)
            
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
                "description": clean_text(description, preserve_newlines=True),
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
