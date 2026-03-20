"""Utility functions for scraping: school detection, text cleaning, etc."""

import re
import json
from datetime import datetime, timedelta
from typing import Optional, Dict, Tuple

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
    "ifcv", "iscod", "oktogone", "oktogone group", "icademie", "i-cademie", "livecampus", "live campus",
    "aurlom", "2i academy", "2iacademy", "openclassrooms", "open classrooms",
    "studi", "doranco", "ifocop", "aftec", "pigier", "cogefi",
    "comptalia", "aftral", "afpa", "greta", "cesi", "enaco",
    "esecad", "efc", "cnfdi", "skill and you", "skillandyou",
    "visiplus", "digital campus", "digital school", "digital college",
    "jobergroup", "jober group", "vitalliance", "formaposte", "formaposte sud est",
    "formaposte sud-est", "carrevolutis", "boulangerie ange", "maten", "synd dauphinois apprentissage",
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
    "groupe igf", "isim", "mbs education", "mjm graphic design",
    "pigier", "epsi", "wis", "ifag", "idrac", "sup'de com",
    "win sport school", "mbway", "esicad", "esup", "esig",
    "icoges", "esgis", "groupe alternance", "alternance azur", 
    "sciences-u", "campus sciences-u",
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
    "bloom alternance", "acadomia", "complétude", "completude", "anacours",
    # Generic patterns
    "formation en alternance",
    "notre école", "notre ecole",
    "notre formation",
    "rejoignez notre cursus",
    "association imc", "imc alternance", "imc formation",
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
    "pour l'un de ses partenaire",
    "pour l'un de ses partenaires",
    "pour un de ses partenaire",
    "pour un de ses partenaires",
    "notre école recrute",
    "notre ecole recrute",
    "ecole recrute pour",
    "école recrute pour",
    "dans le cadre de notre formation",
    "postulez à notre formation",
    "postulez a notre formation",
    "titre professionnel", "titre rncp",
    "formation gratuite et rémunérée",
    "formation qualifiante", "titre certifié", "titre certifie",
    "formation gratuite et remuneree",
    "prise en charge par l'opco",
    "aucun frais pour le candidat",
    "aucun frais de formation",
    "iscod", "icademie", "i-cademie", "ifcv",
    # Generic CFA / training org patterns
    "organisme de formation",
    "centre de formation",
    "cfa certifi",
    "notre cfa",
    "notre centre de formation",
    "vous propose une formation en alternance",
    "formation en alternance reconnue",
    "nous vous proposons une formation",
    "en partenariat avec ifcv",
    "en partenariat avec l'ifcv",
    "en partenariat avec un cfa",
    "en partenariat avec une école",
    "en partenariat avec une ecole",
    "en partenariat avec notre école",
    "en partenariat avec notre ecole",
    "entreprises partenaires",
    "école partenaire", "ecole partenaire",
    # School posting for a client company (e.g. "Auguste Académie recherche pour l'un de ses clients")
    "pour l'un de ses clients",
    "pour un de ses clients",
    "recherche pour l'un de ses",
    "recherche pour un de ses",
    "recrute pour l'un de ses",
    "recrute pour un de ses",
    # Invitation to join the school
    "rejoignez notre académie", "rejoignez notre academie",
    "rejoignez notre école", "rejoignez notre ecole",
    "rejoignez notre centre de formation",
    "rejoignez notre cfa",
    "construisons ensemble votre avenir",
    # Eligibility framing typical of school offers
    "éligible à un contrat d'apprentissage",
    "eligible a un contrat d'apprentissage",
    "éligible au contrat d'apprentissage",
    "eligible au contrat d'apprentissage",
]


def is_school_offer(company: str, description: Optional[str] = None, title: Optional[str] = None) -> bool:
    """
    Detect if an offer comes from a school rather than a company.

    Args:
        company: The company/organization name
        description: The offer description text
        title: The offer title

    Returns:
        True if the offer appears to be from a school
    """
    if title:
        title_lower = title.lower().strip()
        # "Futur Chauffeur", "Préparez votre BTS", etc.
        if title_lower.startswith("futur") or "préparez" in title_lower or "preparez" in title_lower:
            return True

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


import html
import unicodedata

# Prefixes/suffixes to strip when normalizing company names for duplicate detection
_COMPANY_STRIP = re.compile(
    r"^\s*(groupe|group|société|societe|association|fondation|cabinet|agence|the|les|la|le|l'|l')\s+",
    re.IGNORECASE,
)
_COMPANY_LEGAL = re.compile(
    r"\s+(sa|sas|sarl|sasu|snc|sci|scop|se|gie|eurl|spa|inc|ltd|llc|gmbh|nv|bv|plc|corp|co\.?)\s*$",
    re.IGNORECASE,
)


def normalize_company(name: Optional[str]) -> str:
    """
    Return a normalized version of a company name for duplicate comparison.
    Strips common prefixes (Groupe, Société…) and legal suffixes (SA, SAS…),
    lowercases, removes accents and punctuation.
    """
    if not name:
        return ""
    text = name.strip()
    # Remove legal suffix first, then prefix (order matters: "Groupe TF1 SA" → "TF1")
    text = _COMPANY_LEGAL.sub("", text).strip()
    text = _COMPANY_STRIP.sub("", text).strip()
    # Lowercase + remove accents
    text = unicodedata.normalize("NFD", text.lower())
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    # Collapse spaces and remove punctuation except hyphens
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text

# Canonical company name aliases: any key (case-insensitive exact match) → canonical value
COMPANY_ALIASES: dict = {
    "thales": "Thales",
    "thalès": "Thales",
    "THALES": "Thales",
    "THALÈS": "Thales",
    "tf1": "Groupe TF1",
    "tf1 group": "Groupe TF1",
    "tf1 sa": "Groupe TF1",
    "credit agricole": "Groupe Crédit Agricole",
    "ca": "Groupe Crédit Agricole",
    "bnp": "BNP Paribas",
    "bnp paribas sa": "BNP Paribas",
    "société générale": "Société Générale",
    "societe generale": "Société Générale",
    "sg": "Société Générale",
    "la poste": "La Poste - Branche Services-Courrier-Colis",
    "orange sa": "Orange",
    "france telecom": "Orange",
    "edf sa": "EDF",
    "electricite de france": "EDF",
    "total": "TotalEnergies",
    "total se": "TotalEnergies",
    "totalenergies se": "TotalEnergies",
    "l'oreal": "L'Oréal",
    "loreal": "L'Oréal",
    "l oreal": "L'Oréal",
    "axa sa": "AXA",
    "axa group": "AXA",
    "sncf reseau": "SNCF",
    "sncf voyageurs": "SNCF",
    "lvmh moet hennessy": "LVMH",
    "lvmh group": "LVMH",
    # Groupe BPCE (holding uniquement — entités explicitement nommées)
    "bpce": "Groupe BPCE",
    "bpce sa": "Groupe BPCE",
    "groupe bpce": "Groupe BPCE",
    # Caisses d'Épargne régionales — restent indépendantes
    "caisse d'epargne ile de france": "Caisse d'Épargne Ile-de-France",
    "caisse d'épargne ile de france": "Caisse d'Épargne Ile-de-France",
    "caisse d'epargne ile-de-france": "Caisse d'Épargne Ile-de-France",
    "caisse d'épargne île-de-france": "Caisse d'Épargne Ile-de-France",
    "caisse d'epargne normandie": "Caisse d'Épargne Normandie",
    "caisse d'épargne normandie": "Caisse d'Épargne Normandie",
    "caisse d'epargne grand est europe": "Caisse d'Épargne Grand Est Europe",
    "caisse d'épargne grand est europe": "Caisse d'Épargne Grand Est Europe",
    "caisse d'epargne hauts de france": "Caisse d'Épargne Hauts-de-France",
    "caisse d'épargne hauts de france": "Caisse d'Épargne Hauts-de-France",
    "caisse d'epargne bretagne pays de loire": "Caisse d'Épargne Bretagne-Pays de Loire",
    "caisse d'épargne bretagne pays de loire": "Caisse d'Épargne Bretagne-Pays de Loire",
    "caisse d'epargne loire drôme ardèche": "Caisse d'Épargne Loire Drôme Ardèche",
    "caisse d'epargne rhone alpes": "Caisse d'Épargne Rhône Alpes",
    "caisse d'épargne rhône alpes": "Caisse d'Épargne Rhône Alpes",
    "caisse d'epargne provence alpes corse": "Caisse d'Épargne Provence-Alpes-Corse",
    "caisse d'épargne provence alpes corse": "Caisse d'Épargne Provence-Alpes-Corse",
    "caisse d'epargne midi-pyrenees": "Caisse d'Épargne Midi-Pyrénées",
    "caisse d'épargne midi-pyrénées": "Caisse d'Épargne Midi-Pyrénées",
    "caisse d'epargne languedoc roussillon": "Caisse d'Épargne Languedoc-Roussillon",
    "caisse d'épargne languedoc roussillon": "Caisse d'Épargne Languedoc-Roussillon",
    "caisse d'epargne loire centre": "Caisse d'Épargne Loire-Centre",
    "caisse d'épargne loire centre": "Caisse d'Épargne Loire-Centre",
    # Normalisation orthographique générique
    "caisse d'epargne": "Caisse d'Épargne",
    "caisses d'epargne": "Caisse d'Épargne",
    "caisses d'épargne": "Caisse d'Épargne",
    "banques populaires": "Banque Populaire",
}


def canonicalize_company(name: Optional[str]) -> Optional[str]:
    """
    Return the canonical company name if a known alias is found, otherwise return the original.
    Matching is case-insensitive and strips surrounding whitespace.
    """
    if not name:
        return name
    key = name.strip().lower()
    return COMPANY_ALIASES.get(key, name)


def clean_text(text: Optional[str], preserve_newlines: bool = False) -> Optional[str]:
    """Clean and normalize text content."""
    if not text:
        return None
    
    # Decode HTML entities (e.g., &eacute; -> é, &#39; -> ')
    text = html.unescape(text)
    
    if preserve_newlines:
        # Replace common block tags and breaks with newlines
        text = re.sub(r'<(br|p|div)[^>]*>', '\n', text, flags=re.IGNORECASE)
        # Remove other HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Normalize whitespace but keep newlines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(filter(None, lines))
    else:
        # Original behavior: collapse everything
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'<[^>]+>', '', text)
        
    return text if text and text.strip() else None


def extract_department(location: Optional[str]) -> Optional[str]:
    """Extract department code from a location string."""
    if not location:
        return None
    
    # 1. 5-digit postal codes (take first 2-3 digits) - highly reliable
    match_5_digit = re.search(r'\b(97[1-6]|[0-8]\d|9[0-5])\d{3}\b', location)
    if match_5_digit:
        return match_5_digit.group(1)
        
    # 2. Extract ALL valid standalone codes
    codes = re.findall(r'\b(97[1-6]|2[AB]|0[1-9]|[1-8]\d|9[0-5])\b', location)
    
    if not codes:
        match_exact = re.fullmatch(r'(97[1-6]|2[AB]|0[1-9]|[1-8]\d|9[0-5])', location.strip())
        return match_exact.group(1) if match_exact else None
        
    if len(codes) == 1:
        return codes[0]
    
    # Multiple codes found (e.g. "Paris 01 - 75")
    # Identify which ones LOOK like arrondissements (01-20 in a city context)
    formatted_codes = []
    for code in codes:
        if re.search(rf'({code}\s*-)|(-\s*{code})', location):
            formatted_codes.append(code)
            
    if not formatted_codes:
        formatted_codes = codes

    # If we have multiple, prioritize the one that isn't a risky arrondissement code
    risky = [str(i).zfill(2) for i in range(1, 21)]
    non_risky = [c for c in formatted_codes if c not in risky]
    
    if non_risky:
        return non_risky[0]
        
    return formatted_codes[0]

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
    "issoudun": "36", "châteauroux": "36", "chateauroux": "36", "pantin": "93", "cholet": "49",
    "vannes": "56", "lorient": "56", "quimper": "29", "saint-malo": "35", "niort": "79",
    "la défense": "92", "la defense": "92", "sophia-antipolis": "06", "sophia antipolis": "06",
    "levallois-perret": "92", "neuilly-sur-seine": "92", "issy-les-moulineaux": "92"
}

# Cache for geo API lookups (module-level, persists during a scrape run)
_geo_cache: Dict[str, Optional[str]] = {}


def _resolve_city_department(city_name: str) -> Optional[str]:
    """Resolve a city name to a department code using the French government geo API."""
    if city_name in _geo_cache:
        return _geo_cache[city_name]
    try:
        import httpx
        resp = httpx.get(
            "https://geo.api.gouv.fr/communes",
            params={"nom": city_name, "fields": "codeDepartement", "limit": 1, "boost": "population"},
            timeout=5,
        )
        if resp.status_code == 200:
            data = resp.json()
            if data:
                dept = data[0].get("codeDepartement")
                _geo_cache[city_name] = dept
                return dept
    except Exception:
        pass
    _geo_cache[city_name] = None
    return None


def enrich_location(location: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Enrich location by deducing department code and formatting the output.
    Returns: (enriched_location: Optional[str], department_code: Optional[str])
    """
    if not location:
        return None, None
        
    loc_clean = location.strip()
    # Strip ', France' which is often added by LinkedIn
    import re
    loc_clean = re.sub(r',\s*France$', '', loc_clean, flags=re.IGNORECASE).strip()
    loc_lower = loc_clean.lower()
    
    # 1. Try to find a strong indicator
    extracted_dept = extract_department(loc_clean) # Our best initial guess
    potential_codes = re.findall(r'\b(97[1-6]|2[AB]|0[1-9]|[1-8]\d|9[0-5])\b', loc_clean)
    
    is_zip = bool(re.search(r'\b(97[1-6]|[0-8]\d|9[0-5])\d{3}\b', loc_clean))
    is_code_prefix = bool(re.search(r'^\d{2,3}\s*-\s*', loc_clean))
    is_code_suffix = bool(re.search(r'\s*-\s*\d{2,3}$', loc_clean))
    
    # 2. Match city for disambiguation
    city_dept = None
    for city, code in CITY_TO_DEPT.items():
        if re.search(rf'\b{city}\b', loc_lower):
            city_dept = code
            break
            
    dept_code = extracted_dept
    
    # Priority logic:
    if city_dept:
        # If the city code is among the ones we found, it's almost certainly the right one (overrides "Paris 01")
        if city_dept in potential_codes:
            dept_code = city_dept
        # If it's a strong indicator like a 5-digit zip, keep that
        elif is_zip or is_code_prefix or is_code_suffix:
            pass # Keep extracted_dept's choice
        else:
            # Otherwise believe the city
            dept_code = city_dept
    else:
        # No city found, check for arrondissement protection on the code we found
        is_risky_code = extracted_dept in [str(i).zfill(2) for i in range(1, 21)]
        if is_risky_code and not is_zip and not is_code_prefix and not is_code_suffix:
            # We don't have a city match, but '01' might still be an arrondissement of an unknown city
            # We keep it as is, but this path is now safer.
            pass
            
    # 3. Try department name matching if still not set
    if not dept_code:
        for code, name in DEPARTMENTS.items():
            if re.search(rf'\b{name.lower()}\b', loc_lower):
                dept_code = code
                break
                
    # 4. Fallback to API lookup for unknown cities
    if not dept_code:
        city_name = re.sub(r'^\d{2,3}\s*-\s*', '', loc_clean)  # Remove "76 - " prefix
        city_name = re.sub(r'\s*\(.*\)', '', city_name).strip()  # Remove parentheticals
        city_name = re.sub(r'\s*,.*', '', city_name).strip()  # Remove after comma
        if city_name and len(city_name) >= 2:
            dept_code = _resolve_city_department(city_name)
                
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
def parse_french_date(date_text: str) -> Optional[datetime]:
    """
    Parse French date strings (absolute or relative) into a datetime object.
    Supports:
    - "Aujourd'hui", "Hier"
    - "Il y a X jours", "Il y a X semaines", "Il y a X mois"
    - "25/02/2026", "25 févr. 2026", "25 février 2026"
    """
    if not date_text:
        return None

    date_text = date_text.lower().strip()
    now = datetime.utcnow()

    # 1. Relative dates
    if "aujourd" in date_text or "à l'instant" in date_text:
        return now
    if "hier" in date_text:
        return now - timedelta(days=1)
    
    # "il y a X ..." or "X ... passée(s)" or "X ... ago"
    match_ago = re.search(r"(?:il y a\s+)?(\d+)\s+(jour|semaine|mois|heure|minute|day|week|month|hour|min)(?:s|\(s\))?", date_text)
    if match_ago:
        n = int(match_ago.group(1))
        unit = match_ago.group(2).lower()
        if unit in ["heure", "hour"]:
            return now - timedelta(hours=n)
        if unit in ["minute", "min"]:
            return now - timedelta(minutes=n)
        if unit in ["jour", "day"]:
            return now - timedelta(days=n)
        if unit in ["semaine", "week"]:
            return now - timedelta(weeks=n)
        if unit in ["mois", "month"]:
            return now - timedelta(days=n * 30)

    # 2. Absolute dates (DD/MM/YYYY)
    match_abs = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", date_text)
    if match_abs:
        day, month, year = match_abs.groups()
        if len(year) == 2:
            year = "20" + year
        try:
            return datetime(int(year), int(month), int(day))
        except ValueError:
            pass

    # 3. Textual months (25 février 2026, 25 févr. 2026, Feb 07, 2026)
    months_map = {
        "janv": 1, "févr": 2, "fevr": 2, "mars": 3, "avril": 4, "mai": 5, "juin": 6,
        "juil": 7, "août": 8, "aout": 8, "sept": 9, "octo": 10, "nove": 11, "déce": 12, "dece": 12,
        "janvier": 1, "février": 2, "fevrier": 2, "avril": 4, "juillet": 7, "septembre": 9,
        "octobre": 10, "novembre": 11, "décembre": 12,
        # English fallbacks (observed on some French sites)
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    
    # Match pattern: Day Month Year (e.g. 25 février 2026)
    match_text = re.search(r"(\d{1,2})\s+([a-zéû\.]+)\s+(\d{4})", date_text)
    if match_text:
        day, month_str, year = match_text.groups()
        month_str = month_str.replace(".", "")
        if month_str in months_map:
            try:
                return datetime(int(year), months_map[month_str], int(day))
            except ValueError:
                pass
        # Check prefixes
        for m_name, m_val in months_map.items():
            if month_str.startswith(m_name[:4]):
                try:
                    return datetime(int(year), m_val, int(day))
                except ValueError:
                    continue
    
    # Match pattern: Month Day, Year (e.g. Feb 07, 2026)
    match_eng = re.search(r"([a-z]{3})\s+(\d{1,2}),\s+(\d{4})", date_text)
    if match_eng:
        month_str, day, year = match_eng.groups()
        if month_str in months_map:
            try:
                return datetime(int(year), months_map[month_str], int(day))
            except ValueError:
                pass

    return None

def normalize_for_deduplication(text: Optional[str]) -> str:
    """
    Normalize text (title or company) for de-duplication purposes.
    Removes common noise and special characters.
    """
    if not text:
        return ""
    
    # Lowercase
    text = text.lower()
    
    # Remove H/F, F/H variations
    text = re.sub(r'\(h/f\)|h/f|f/h|\(f/h\)', '', text)
    
    # Remove common job keywords that vary between sources
    noise = [
        "alternance", "apprentissage", "contrat", "apprenti", "apprentie", 
        "en alternance", "stage", "cdd", "cdi", "offre d'", "offre de"
    ]
    for n in noise:
        text = text.replace(n, "")
    
    # Remove non-alphanumeric characters
    text = re.sub(r'[^a-zA-Z0-9]', '', text)
    
    return text.strip()
