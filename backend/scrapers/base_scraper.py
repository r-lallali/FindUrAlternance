"""
Abstract base class for all scrapers.

All scrapers should inherit from BaseScraper and implement:
- scrape(): Fetch raw data from the source
- parse_offer(): Transform raw data into a standardized offer dict
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
from scrapers.skills_extractor import extract_skills, extract_skills_flat, is_alternance_offer, categorize_offer
from scrapers.utils import extract_education_level


class BaseScraper(ABC):
    """Abstract base scraper class."""

    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = logging.getLogger(f"scraper.{source_name}")

    @abstractmethod
    async def scrape(self, **kwargs) -> List[Dict[str, Any]]:
        """Fetch raw offer data from the source."""
        pass

    @abstractmethod
    def parse_offer(self, raw_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Parse a raw offer into a standardized dictionary."""
        pass

    async def run(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Execute the full pipeline: scrape → parse → enrich.

        Returns a list of parsed and enriched offer dicts.
        """
        self.logger.info(f"Starting {self.source_name} scraper")

        raw_data = await self.scrape(**kwargs)
        self.logger.info(f"Fetched {len(raw_data)} raw items from {self.source_name}")

        parsed = []
        for item in raw_data:
            try:
                offer = self.parse_offer(item)
                if offer is not None:
                    # Enrich with skills extraction
                    offer = self._enrich_with_skills(offer)

                    # Check if it's a real alternance offer
                    if not offer.get("is_alternance", True):
                        continue  # Skip non-alternance CDD

                    parsed.append(offer)
            except Exception as e:
                self.logger.debug(f"Error parsing item: {e}")

        self.logger.info(
            f"{self.source_name}: {len(parsed)} valid offers (from {len(raw_data)} raw)"
        )
        return parsed

    def _enrich_with_skills(self, offer: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich an offer with extracted skills and alternance detection."""
        title = offer.get("title", "")
        description = offer.get("description", "")
        contract_type = offer.get("contract_type", "")

        # Extract skills
        skills = extract_skills(title, description)
        offer["skills_languages"] = json.dumps(skills.get("languages", []))
        offer["skills_frameworks"] = json.dumps(skills.get("frameworks", []))
        offer["skills_tools"] = json.dumps(skills.get("tools", []))
        offer["skills_certifications"] = json.dumps(skills.get("certifications", []))
        offer["skills_methodologies"] = json.dumps(skills.get("methodologies", []))
        offer["skills_all"] = json.dumps(extract_skills_flat(title, description))

        # Categorize
        offer["category"] = categorize_offer(title, description)

        # Check if it's a real alternance
        offer["is_alternance"] = is_alternance_offer(title, description, contract_type)

        # Education level: prefer existing valid value, fallback to text extraction
        existing_profile = offer.get("profile")
        if not existing_profile:
            offer["profile"] = extract_education_level(title, description)

        return offer
