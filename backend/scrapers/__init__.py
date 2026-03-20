"""Scraper package initialization."""

from scrapers.base_scraper import BaseScraper
from scrapers.labonnealternance import LaBonneAlternanceScraper
from scrapers.francetravail import FranceTravailScraper

from scrapers.linkedin import LinkedInScraper
from scrapers.hellowork import HelloWorkScraper
from scrapers.wttj import WelcomeToTheJungleScraper
from scrapers.apec import ApecScraper
from scrapers.meteojob import MeteojobScraper
from scrapers.rhalternance import RHAlternanceScraper
from scrapers.cadremploi import CadremploiScraper

__all__ = [
    "BaseScraper",
    "LaBonneAlternanceScraper",
    "FranceTravailScraper",

    "LinkedInScraper",
    "HelloWorkScraper",
    "WelcomeToTheJungleScraper",
    "ApecScraper",
    "MeteojobScraper",
    "RHAlternanceScraper",
    "CadremploiScraper"
]
