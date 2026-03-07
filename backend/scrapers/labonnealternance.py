"""
Scraper for La Bonne Alternance API.

Documentation: https://labonnealternance.apprentissage.beta.gouv.fr/api/docs
This is an official French government API providing alternance offers.
"""

import asyncio
import httpx
from typing import List, Dict, Any, Optional
from datetime import datetime
from scrapers.base_scraper import BaseScraper
from scrapers.utils import is_school_offer, clean_text, enrich_location, normalize_profile, normalize_salary


class LaBonneAlternanceScraper(BaseScraper):
    """Scraper using the La Bonne Alternance API."""

    BASE_URL = "https://labonnealternance.apprentissage.beta.gouv.fr/api"
    SEARCH_URL = f"{BASE_URL}/v1/jobsEtFormations"

    # Major French cities with their INSEE codes for broader coverage
    CITIES = [
        {"name": "Paris", "insee": "75056", "lat": 48.8566, "lon": 2.3522},
        {"name": "Lyon", "insee": "69123", "lat": 45.7640, "lon": 4.8357},
        {"name": "Marseille", "insee": "13055", "lat": 43.2965, "lon": 5.3698},
        {"name": "Toulouse", "insee": "31555", "lat": 43.6047, "lon": 1.4442},
        {"name": "Lille", "insee": "59350", "lat": 50.6292, "lon": 3.0573},
        {"name": "Bordeaux", "insee": "33063", "lat": 44.8378, "lon": -0.5792},
        {"name": "Nantes", "insee": "44109", "lat": 47.2184, "lon": -1.5536},
        {"name": "Strasbourg", "insee": "67482", "lat": 48.5734, "lon": 7.7521},
        {"name": "Rennes", "insee": "35238", "lat": 48.1173, "lon": -1.6778},
        {"name": "Montpellier", "insee": "34172", "lat": 43.6108, "lon": 3.8767},
    ]

    # ROME codes for common alternance sectors
    ROME_CODES = {
        "informatique": ["M1805", "M1802", "M1803"],
        "commerce": ["D1406", "D1407", "D1402"],
        "comptabilite": ["M1203", "M1202"],
        "communication": ["E1103", "E1101"],
        "rh": ["M1502", "M1503"],
        "marketing": ["M1705", "M1703"],
        "logistique": ["N1301", "N1303"],
        "banque": ["C1201", "C1202"],
        "btp": ["F1106", "F1604", "F1701", "F1704"],
        "sante": ["J1506", "J1301", "J1302"],
        "industrie": ["H1206", "H1401", "H1502", "H2502"],
        "tourisme_hotellerie": ["G1203", "G1803", "G1602"],
        "services_personne": ["K1302", "K1304", "K1305"],
        "immobilier": ["C1501", "C1502", "C1503"],
        "arts_design": ["B1101", "B1805", "E1205"],
    }

    def __init__(self):
        super().__init__("labonnealternance")

    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Scrape offers from La Bonne Alternance API.

        Keyword args:
            cities: list - List of city dicts to search (defaults to CITIES)
            rome_codes: list - Specific ROME codes to search
            radius: int - Search radius in km (default: 100)
        """
        cities = kwargs.get("cities", self.CITIES[:5])  # Top 5 cities by default
        radius = kwargs.get("radius", 100)
        rome_codes = kwargs.get("rome_codes", None)

        # Collect all ROME codes if none specified
        if rome_codes is None:
            codes_to_search = []
            for sector_codes in self.ROME_CODES.values():
                codes_to_search.extend(sector_codes)
        else:
            codes_to_search = rome_codes

        all_offers = []
        seen_ids = set()

        semaphore = asyncio.Semaphore(5)

        async def fetch_batch(city, batch):
            async with semaphore:
                try:
                    return await self._search(
                        rome_codes=batch,
                        insee=city["insee"],
                        latitude=city["lat"],
                        longitude=city["lon"],
                        radius=radius,
                    )
                except Exception as e:
                    self.logger.warning(f"Error in LBA search for {city['name']} batch {batch}: {e}")
                    return []

        tasks = []
        for city in cities:
            batch_size = 5 # increased batch size for parallel
            for i in range(0, len(codes_to_search), batch_size):
                batch = codes_to_search[i:i + batch_size]
                tasks.append(fetch_batch(city, batch))
        
        results = await asyncio.gather(*tasks)
        for offers in results:
            for offer in offers:
                offer_id = offer.get("id", "")
                if offer_id and offer_id not in seen_ids:
                    seen_ids.add(offer_id)
                    all_offers.append(offer)

        self.logger.info(f"Total unique raw offers collected: {len(all_offers)}")
        return all_offers

    async def _search(
        self,
        rome_codes: List[str],
        insee: str,
        latitude: float,
        longitude: float,
        radius: int,
    ) -> List[Dict[str, Any]]:
        """Search for offers with the given parameters."""
        params = {
            "romes": ",".join(rome_codes),
            "latitude": latitude,
            "longitude": longitude,
            "radius": radius,
            "insee": insee,
            "caller": "fua-dashboard",
        }

        for attempt in range(3): # 3 attempts
            try:
                async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
                    response = await client.get(self.SEARCH_URL, params=params)

                    if response.status_code == 200:
                        data = response.json()
                        offers = []

                        # Extract job offers from nested structure
                        jobs = data.get("jobs", {})
                        if isinstance(jobs, dict):
                            for key in ["peJobs", "matchas", "partnerJobs"]:
                                sub = jobs.get(key)
                                if isinstance(sub, dict) and "results" in sub:
                                    offers.extend(sub["results"])
                                elif isinstance(sub, list):
                                    offers.extend(sub)

                        return offers
                    elif response.status_code == 429:
                        wait = 10 * (attempt + 1)
                        self.logger.warning(f"LBA Rate Limited (429). Waiting {wait}s before retry {attempt+1}...")
                        await asyncio.sleep(wait)
                        continue
                    else:
                        self.logger.warning(
                            f"API returned {response.status_code} for ROME {rome_codes}, INSEE {insee}"
                        )
                        return []

            except httpx.TimeoutException:
                self.logger.warning(f"Timeout for ROME {rome_codes}, INSEE {insee} (attempt {attempt+1})")
                if attempt < 2:
                    await asyncio.sleep(5)
                continue
            except Exception as e:
                self.logger.error(f"Error fetching offers: {e}")
                return []
        
        return []

    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a raw offer from La Bonne Alternance API."""
        try:
            idea_type = raw_data.get("ideaType", "")

            # Skip formations (training offers) - we only want job offers
            if idea_type == "formation":
                return None

            # Extract title
            title = raw_data.get("title", "")
            if not title:
                return None

            # Extract company info
            company_data = raw_data.get("company", {})
            contact_data = raw_data.get("contact", {})
            job_data = raw_data.get("job", {}) or {}
            
            company_name = "Entreprise confidentielle"
            if isinstance(company_data, dict):
                company_name = company_data.get("name")
            
            # Identify if the name is truly confidential or missing
            is_confidential = not company_name or any(kw in str(company_name).lower() for kw in ["confidentiel", "anonyme", "non renseigné", "nc"])
            
            if is_confidential:
                # Fallback 1: Check contact name (often contains company name in LBA/FT API)
                if isinstance(contact_data, dict) and contact_data.get("name"):
                    c_name = contact_data["name"]
                    if not any(kw in str(c_name).lower() for kw in ["monsieur", "madame", "direction", "agence"]):
                        company_name = c_name
                        is_confidential = False

            if is_confidential:
                # Fallback 2: Try to extract from title (e.g. "Assistant RH - Nom Entreprise")
                full_title = raw_data.get("title") or job_data.get("title") or ""
                if " - " in full_title:
                    parts = full_title.split(" - ")
                    # If the last part is short and doesn't look like a location
                    potential_name = parts[-1].strip()
                    if potential_name and len(potential_name) > 2 and not any(char.isdigit() for char in potential_name):
                        company_name = potential_name
                        is_confidential = False

            if is_confidential:
                # Fallback 3: Extract from company description if it starts with the name
                c_desc = company_data.get("description") if isinstance(company_data, dict) else ""
                if c_desc and len(c_desc) > 5:
                    # Often starts with "L'entreprise NAME..." or just "NAME est une..."
                    # We take the first sentence or first 5 words
                    first_phrase = c_desc.split(".")[0].split(":")[0]
                    words = first_phrase.split()
                    if words:
                        # Simple heuristic: take first 3-4 words if they don't look like a full sentence
                        if len(words) <= 5:
                            company_name = first_phrase.strip()
            
            if not company_name:
                company_name = "Entreprise confidentielle"

            # Extract job details
            job_data = raw_data.get("job", {})
            if not isinstance(job_data, dict):
                job_data = {}

            description = (job_data.get("description") or "").strip()

            # Fallback to ROME definition if description is missing
            if not description:
                # Check root or nested in job
                rome_details = raw_data.get("romeDetails") or job_data.get("romeDetails")
                if isinstance(rome_details, dict):
                    description = (rome_details.get("definition") or "").strip()
            
            # Second fallback: use ROME labels
            if not description:
                appellation = job_data.get("rome_appellation_label")
                rome_label = job_data.get("rome_label")
                description = appellation or rome_label or ""
                if description:
                    description = f"Poste en tant que {description}. (Description détaillée non disponible)"

            # School detection
            is_school = is_school_offer(company_name, description, title)

            # Extract location
            place = raw_data.get("place", {})
            if isinstance(place, dict):
                city = place.get("city", "")
                address = place.get("fullAddress", "")
                zipcode = place.get("zipCode", "")
                location = city or address
                if zipcode and city and zipcode not in city:
                    location = f"{city} ({zipcode})"
            else:
                location = None

            # Contract type
            contract_type = job_data.get("contractType", "")
            contract_desc = job_data.get("contractDescription", "")
            if contract_desc:
                contract_type = contract_desc
            elif not contract_type:
                contract_type = "Alternance"

            # Diploma / profile
            profile = raw_data.get("target_diploma_level", "")
            profile = normalize_profile(profile)

            # ROME label as category
            romes = raw_data.get("romes", [])
            category = ""
            if romes and isinstance(romes, list) and len(romes) > 0:
                if isinstance(romes[0], dict):
                    category = romes[0].get("label", "")
                elif isinstance(romes[0], str):
                    category = romes[0]

            # Publication date
            pub_date = None
            date_str = job_data.get("creationDate", "")
            if date_str:
                try:
                    if "T" in str(date_str):
                        pub_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
                    else:
                        pub_date = datetime.strptime(str(date_str)[:10], "%Y-%m-%d")
                except (ValueError, TypeError):
                    pass

            # URL
            url = raw_data.get("url", "") or ""
            if not url:
                contact = raw_data.get("contact")
                if isinstance(contact, dict) and contact.get("url"):
                    url = contact["url"]
            if not url:
                company_url = company_data.get("url") if isinstance(company_data, dict) else None
                if company_url:
                    url = company_url

            offer_id = raw_data.get("id", "")

            # Build URL for offers that still don't have one
            if not url and offer_id:
                if idea_type == "matcha":
                    url = f"https://labonnealternance.apprentissage.beta.gouv.fr/recherche-apprentissage?display=list&page=fiche&type=matcha&itemId={offer_id}"
                elif idea_type == "peJob":
                    url = f"https://candidat.francetravail.fr/offres/recherche/detail/{offer_id}"

            # Unique ID
            source_id = f"lba_{idea_type}_{offer_id}" if offer_id else None

            clean_loc = clean_text(location)
            enriched_loc, dept = enrich_location(clean_loc)

            return {
                "title": clean_text(title),
                "company": clean_text(company_name),
                "location": enriched_loc or clean_loc,
                "department": dept,
                "contract_type": contract_type,
                "salary": None,  # LBA doesn't always provide salary
                "salary_min": None,
                "salary_max": None,
                "description": clean_text(description),
                "profile": profile,
                "category": clean_text(category),
                "publication_date": pub_date,
                "source": "labonnealternance",
                "url": url,
                "source_id": source_id,
                "is_school": is_school,
            }
        except Exception as e:
            self.logger.warning(f"Error parsing LBA offer: {e}")
            return None
