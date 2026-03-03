"""Utility functions for scraping: school detection, text cleaning, etc."""

import re
from typing import Optional

# Keywords that indicate an offer is from a school rather than a company
SCHOOL_KEYWORDS = [
    # Generic school terms
    "école", "ecole", "campus", "institut", "cfa", "centre de formation",
    "formation professionnelle", "lycée", "lycee", "université", "universite",
    "iut", "bts", "académie", "academie", "enseignement", "pédagogique",
    "pedagogique", "scolaire", "éducation", "education nationale",
    "école supérieure", "ecole superieure", "grande école", "grande ecole",
    "organisme de formation", "centre d'apprentissage",
    # Well-known IT/engineering schools
    "esiea", "epitech", "epita", "supinfo", "isen", "efrei",
    "isep", "ece", "esilv", "emlyon", "escp", "essec", "hec",
    "kedge", "skema", "neoma", "audencia", "grenoble em",
    # Alternance-focused training schools (often post as "companies")
    "iscod", "icademie", "i-cademie", "livecampus", "live campus",
    "aurlom", "2i academy", "2iacademy", "openclassrooms", "open classrooms",
    "studi", "doranco", "ifocop", "aftec", "pigier", "cogefi",
    "comptalia", "aftral", "afpa", "greta", "cesi", "enaco",
    "esecad", "efc", "cnfdi", "skill and you", "skillandyou",
    "visiplus", "digital campus", "digital school", "digital college",
    "hetic", "web@cadémie", "webacademie", "wild code school",
    "wildcodeschool", "le wagon", "lewagon", "la capsule",
    "iron hack", "ironhack", "simplon", "pop school", "popschool",
    "rocket school", "rocketschool", "holberton", "ada tech school",
    "adatechschool", "o'clock", "oclock", "école 42", "ecole 42",
    "3wa", "3w academy", "3wacademy", "webitech", "webtech",
    "sup de vinci", "supdevinci", "my digital school", "mydigitalschool",
    "esgi", "ynov", "next-u", "nextu", "ingetis", "ipi",
    "esic", "estiam", "itecom", "ican", "isefac", "mbway",
    "bachelor institute", "ecema", "maestris", "esup",
    "efficom", "epsi", "wis", "supinfo", "eni ecole",
    "itescia", "imie", "etna", "esm-a", "esma",
    "sup career", "supcareer", "igs", "groupe igs",
    "cfa des sciences", "cfa codeur", "cfa dev", "cfa informatique",
    "village de l'emploi", "village emploi", "edugroupe",
    "fitec", "m2i formation", "m2i", "diginamic",
    "human booster", "humanbooster", "dawan", "ib formation",
    "ibformation", "plb consultant", "orsys", "cegos",
    "demos", "global knowledge", "globalknowledge",
    "al mahir", "almahir", "alma", "issmi",
    "nextadvance", "next advance", "espl", "sup de pub",
    "école multimédia", "ecole multimedia",
    "Sciences-U", "sciences u", "force plus",
    "alternance azur", "groupe alternance",
    "scholia", "livementor", "live mentor", "cybersup", "cyber sup",
    "stephenson formation", "avlis formation", "avlis",
    "stand up formation", "standup formation",
    "pro-fyl", "profyl", "arefip",
    "ima business school", "business school",
    "cfi formation", "cma formation", "directt formation",
    "nexa digital", "formation alternance superieure",
    "organisation formations informatiques",
    # Generic patterns
    "formation en alternance",
    "notre école", "notre ecole",
    "notre formation",
    "rejoignez notre cursus",
]

# Keywords in description that suggest the offer is from a school
SCHOOL_DESCRIPTION_KEYWORDS = [
    "frais de scolarité", "frais de scolarite",
    "programme de formation certifiant",
    "inscription obligatoire",
    "rejoignez notre formation",
    "intégrez notre école", "integrez notre ecole",
    "nous formons", "notre cursus",
    "diplôme délivré par notre", "diplome delivre par notre",
    "nous recherchons pour l'une de nos entreprises partenaires",
    "nous recherchons pour une de nos entreprises partenaires",
    "pour le compte de l'une de nos entreprises partenaires",
    "pour le compte d'une de nos entreprises partenaires",
    "dans le cadre de notre formation",
    "postulez à notre formation",
    "postulez a notre formation",
    "titre professionnel", "titre rncp",
    "formation gratuite et rémunérée",
    "formation gratuite et remuneree",
    "prise en charge par l'opco",
    "aucun frais pour le candidat",
    "aucun frais de formation",
]


def is_school_offer(company: str, description: Optional[str] = None) -> bool:
    """
    Detect if an offer comes from a school rather than a company.

    Args:
        company: The company/organization name
        description: The offer description text

    Returns:
        True if the offer appears to be from a school
    """
    if not company:
        return False

    company_lower = company.lower().strip()

    # Check company name against school keywords
    for keyword in SCHOOL_KEYWORDS:
        if keyword in company_lower:
            return True

    # Check description for school-related content
    if description:
        desc_lower = description.lower()
        for keyword in SCHOOL_DESCRIPTION_KEYWORDS:
            if keyword in desc_lower:
                return True

    return False


def clean_text(text: Optional[str]) -> Optional[str]:
    """Clean and normalize text content."""
    if not text:
        return None
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Remove HTML tags if any remain
    text = re.sub(r'<[^>]+>', '', text)
    return text if text else None


def extract_department(location: Optional[str]) -> Optional[str]:
    """Extract department code from a location string."""
    if not location:
        return None
    # Match French department codes:
    # 1. 5-digit postal codes (take first 2 digits, or first 3 for DOM)
    # 2. standalone 2-digit codes (01-95)
    # 3. 2A, 2B (Corse)
    # 4. 971-976 (DOM standalone)
    
    # Try 5-digit postal codes first
    match_5_digit = re.search(r'\b(97[1-6]|[0-8]\d|9[0-5])\d{3}\b', location)
    if match_5_digit:
        return match_5_digit.group(1)
        
    # Then standalone codes
    match_standalone = re.search(r'\b(97[1-6]|2[AB]|[01-9][0-9])\b', location)
    if match_standalone:
        return match_standalone.group(1)
        
    # Sometimes it's exactly the string with no boundaries, e.g. "75"
    match_exact = re.fullmatch(r'(97[1-6]|2[AB]|[01-9][0-9])', location.strip())
    if match_exact:
        return match_exact.group(1)
        
    return None

# Mapping of French department codes to their names
DEPARTMENTS = {
    "01": "Ain", "02": "Aisne", "03": "Allier", "04": "Alpes-de-Haute-Provence", "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes", "07": "Ardèche", "08": "Ardennes", "09": "Ariège", "10": "Aube",
    "11": "Aude", "12": "Aveyron", "13": "Bouches-du-Rhône", "14": "Calvados", "15": "Cantal",
    "16": "Charente", "17": "Charente-Maritime", "18": "Cher", "19": "Corrèze", "2A": "Corse-du-Sud",
    "2B": "Haute-Corse", "21": "Côte-d'Or", "22": "Côtes-d'Armor", "23": "Creuse", "24": "Dordogne",
    "25": "Doubs", "26": "Drôme", "27": "Eure", "28": "Eure-et-Loir", "29": "Finistère",
    "30": "Gard", "31": "Haute-Garonne", "32": "Gers", "33": "Gironde", "34": "Hérault",
    "35": "Ille-et-Vilaine", "36": "Indre", "37": "Indre-et-Loire", "38": "Isère", "39": "Jura",
    "40": "Landes", "41": "Loir-et-Cher", "42": "Loire", "43": "Haute-Loire", "44": "Loire-Atlantique",
    "45": "Loiret", "46": "Lot", "47": "Lot-et-Garonne", "48": "Lozère", "49": "Maine-et-Loire",
    "50": "Manche", "51": "Marne", "52": "Haute-Marne", "53": "Mayenne", "54": "Meurthe-et-Moselle",
    "55": "Meuse", "56": "Morbihan", "57": "Moselle", "58": "Nièvre", "59": "Nord",
    "60": "Oise", "61": "Orne", "62": "Pas-de-Calais", "63": "Puy-de-Dôme", "64": "Pyrénées-Atlantiques",
    "65": "Hautes-Pyrénées", "66": "Pyrénées-Orientales", "67": "Bas-Rhin", "68": "Haut-Rhin",
    "69": "Rhône", "70": "Haute-Saône", "71": "Saône-et-Loire", "72": "Sarthe", "73": "Savoie",
    "74": "Haute-Savoie", "75": "Paris", "76": "Seine-Maritime", "77": "Seine-et-Marne",
    "78": "Yvelines", "79": "Deux-Sèvres", "80": "Somme", "81": "Tarn", "82": "Tarn-et-Garonne",
    "83": "Var", "84": "Vaucluse", "85": "Vendée", "86": "Vienne", "87": "Haute-Vienne",
    "88": "Vosges", "89": "Yonne", "90": "Territoire de Belfort", "91": "Essonne", "92": "Hauts-de-Seine",
    "93": "Seine-Saint-Denis", "94": "Val-de-Marne", "95": "Val-d'Oise",
    "971": "Guadeloupe", "972": "Martinique", "973": "Guyane", "974": "La Réunion", "976": "Mayotte"
}

# Mapping of major French cities to their department codes
CITY_TO_DEPT = {
    "paris": "75", "marseille": "13", "lyon": "69", "toulouse": "31", "nice": "06",
    "nantes": "44", "montpellier": "34", "strasbourg": "67", "bordeaux": "33", "lille": "59",
    "rennes": "35", "reims": "51", "toulon": "83", "saint-étienne": "42", "saint-etienne": "42",
    "le havre": "76", "grenoble": "38", "dijon": "21", "angers": "49", "villeurbanne": "69",
    "le mans": "72", "nîmes": "30", "nimes": "30", "aix-en-provence": "13", "clermont-ferrand": "63",
    "brest": "29", "tours": "37", "amiens": "80", "limoges": "87", "annecy": "74",
    "boulogne-billancourt": "92", "perpignan": "66", "besançon": "25", "besancon": "25",
    "metz": "57", "orléans": "45", "orleans": "45", "saint-denis": "93", "rouen": "76",
    "argenteuil": "95", "montreuil": "93", "mulhouse": "68", "caen": "14", "nancy": "54",
    "tourcoing": "59", "roubaix": "59", "nanterre": "92", "vitry-sur-seine": "94", "avignon": "84",
    "créteil": "94", "creteil": "94", "poitiers": "86", "dunkerque": "59", "aubervilliers": "93",
    "versailles": "78", "colombes": "92", "asnières-sur-seine": "92", "asnieres-sur-seine": "92",
    "courbevoie": "92", "pau": "64", "rueil-malmaison": "92", "champigny-sur-marne": "94", "béziers": "34",
    "beziers": "34", "antibes": "06", "la rochelle": "17", "saint-maur-des-fossés": "94",
    "saint-maur-des-fosses": "94", "cannes": "06", "calais": "62", "saint-nazaire": "44", "mérignac": "33",
    "merignac": "33", "drancy": "93", "colmar": "68", "ajaccio": "2A", "bourges": "18",
    "issoudun": "36", "châteauroux": "36", "chateauroux": "36", "pantin": "93"
}

def enrich_location(location: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Enrich location by deducing department code and formatting the output.
    Returns: (enriched_location: Optional[str], department_code: Optional[str])
    """
    if not location:
        return None, None
        
    loc_clean = location.strip()
    loc_lower = loc_clean.lower()
    
    # 1. First, check if there's already a department code explicitly in the string
    dept_code = extract_department(loc_clean)
    
    # 2. If not, try to match cities
    if not dept_code:
        for city, code in CITY_TO_DEPT.items():
            # Check for city as whole word
            if re.search(rf'\b{city}\b', loc_lower):
                dept_code = code
                break
                
    # 3. If still not, try to match department names
    if not dept_code:
        for code, name in DEPARTMENTS.items():
            if re.search(rf'\b{name.lower()}\b', loc_lower):
                dept_code = code
                break
                
    # Format the location nicely
    enriched_location = loc_clean
    if dept_code and dept_code in DEPARTMENTS.keys():
        dept_name = DEPARTMENTS[dept_code]
        # If the string is just the code, or just "department [code]", replace it
        if loc_clean == dept_code or re.fullmatch(r'd[eé]partement\s+' + dept_code, loc_lower):
            enriched_location = f"{dept_name} ({dept_code})"
        # If the location already has the name or code but not nicely formatted together
        elif dept_code not in loc_clean and dept_name.lower() not in loc_lower:
            # We found a city like "Paris", let's append the dept if not present
            # But avoid doing "Paris (75) (75)"
            if not re.search(r'\(\d+[AB]?\)', enriched_location):
                enriched_location = f"{enriched_location} ({dept_code})"
                
    return enriched_location, dept_code


def normalize_profile(profile: Optional[str]) -> Optional[str]:
    """Normalize education level profile strings."""
    if not profile:
        return None

    profile_lower = profile.lower()

    level_mapping = {
        "bac+5": ["bac+5", "bac + 5", "master", "ingénieur", "ingenieur", "m2", "m1"],
        "bac+4": ["bac+4", "bac + 4", "maîtrise", "maitrise"],
        "bac+3": ["bac+3", "bac + 3", "licence", "bachelor", "l3"],
        "bac+2": ["bac+2", "bac + 2", "bts", "dut", "deug"],
        "bac": ["bac pro", "baccalauréat", "baccalaureat", "niveau bac", "niveau 4"],
        "cap/bep": ["cap", "bep", "niveau 3"],
    }

    for level, keywords in level_mapping.items():
        for keyword in keywords:
            if keyword in profile_lower:
                return level

    return profile


def normalize_salary(salary_text: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Extract min and max salary from a salary text.

    Returns:
        Tuple of (salary_min, salary_max) as strings
    """
    if not salary_text:
        return None, None

    # Remove spaces in numbers
    cleaned = salary_text.replace(" ", "").replace("\u00a0", "")

    # Try to find salary ranges like "25000-30000" or "25k-30k"
    range_match = re.search(r'(\d+[.,]?\d*)\s*[kK€]?\s*[-àa]\s*(\d+[.,]?\d*)\s*[kK€]?', cleaned)
    if range_match:
        min_val = range_match.group(1).replace(",", ".")
        max_val = range_match.group(2).replace(",", ".")
        return min_val, max_val

    # Try to find a single salary value
    single_match = re.search(r'(\d+[.,]?\d*)\s*[kK€]', cleaned)
    if single_match:
        val = single_match.group(1).replace(",", ".")
        return val, val

    return None, None
