"""
IT Skills & Technologies extraction engine.

Extracts programming languages, frameworks, tools, certifications, and
soft skills from job descriptions for the alternance dashboard.
"""

import re
from typing import Dict, List, Optional, Set


# ────────────────────────────────────────────────────────
#  CATEGORIES DICTIONARIES
# ────────────────────────────────────────────────────────

CATEGORIES = {
    "Développement Web & Logiciel": [
        r"\bd[eé]veloppeur\b", r"\bd[eé]veloppeuse\b", r"\bdeveloper\b", r"\bfront.?end\b", r"\bback.?end\b",
        r"\bfull.?stack\b", r"\bing[eé]nieur (t[eé]tude|logiciel|d[eé]veloppement)\b", r"\bsoftware engineer\b",
        r"\bwebmaster\b", r"\bint[eé]grateur\b", r"\bprogrammeur\b", r"\btech lead\b"
    ],
    "Data & IA": [
        r"\bdata\b", r"\bdonn[eé]es\b", r"\bmachine learning\b", r"\bintelligence artificielle\b", r"\bia\b",
        r"\bi\.a\.\b", r"\bbig data\b", r"\bdata analyst\b", r"\bdata scientist\b", r"\banalyste de donn[eé]es\b",
        r"\bbing data engineer\b", r"\bdata engineer\b", r"\bbusiness intelligence\b", r"\bbi\b"
    ],
    "Infrastructure, Cloud & DevOps": [
        r"\binfrastructure\b", r"\bsyst[eè]me(s)?\b", r"\br[eé]seau(x)?\b", r"\bcloud\b", r"\bdevops\b",
        r"\bing[eé]nieur d[eé]ploiement\b", r"\badministrateur\b", r"\bsysadmin\b", r"\barchitecte\b",
        r"\bmaintenance\b", r"\bs[eé]curit[eé]\b", r"\bcybers[eé]curit[eé]\b", r"\bcyber\b", r"\bsecops\b"
    ],
    "Produit & Design (UX/UI)": [
        r"\bux\b", r"\bui\b", r"\bdesign\b", r"\bdesigner\b", r"\bproduct owner\b", r"\bproduct manager\b",
        r"\bproduit\b", r"\bchef(fe)? de produit\b", r"\bwebdesigner\b", r"\bint[eé]rface\b"
    ],
    "Support & Assistance": [
        r"\bsupport\b", r"\bhelp desk\b", r"\bassistance technique\b", r"\btechnicien(ne)?\b",
        r"\bmaintenance informatique\b", r"\bd[eé]pannage\b", r"\bservice client\b", r"\bniveau 1\b", r"\bniveau 2\b"
    ],
    "Chefferie de Projet & PMO": [
        r"\bchef(fe)? de projet\b", r"\bproject manager\b", r"\bpmo\b", r"\bma[iî]trise d.?ouvrage\b",
        r"\bmoa\b", r"\bama\b", r"\bamoe\b", r"\bma[iî]trise d.?oeuvre\b", r"\bamoa\b", r"\bscrum master\b",
        r"\bagile\b", r"\bcoordination\b", r"\bdirecteur de projet\b"
    ],
    "Marketing Digital & SEO": [
        r"\bmarketing\b", r"\bdigital\b", r"\bseo\b", r"\bsea\b", r"\bcommunity manager\b", r"\btraffic manager\b",
        r"\be-commerce\b", r"\bacquisition\b", r"\bcro\b", r"\bstrat[eé]gie digitale\b"
    ],
    "Communication & Création": [
        r"\bcommunication\b", r"\bcomm\b", r"\bcr[eé]ation\b", r"\bcontenu\b", r"\bcontent manager\b",
        r"\bgraphiste\b", r"\bdirecteur artistique\b", r"\b[eé]v[eé]nementiel\b", r"\bévenement\b"
    ],
    "Ressources Humaines & Recrutement": [
        r"\brecruteur\b", r"\brh\b", r"\bressources humaines\b", r"\bchasseur de t[eê]tes\b", r"\btalent acquisition\b",
        r"\bsourcing\b", r"\bcharg[eé](e)? de recrutement\b", r"\bpaie\b", r"\badministration du personnel\b",
        r"\bformation\b", r"\bgestion des talents\b"
    ],
    "Gestion, Finance & Administration": [
        r"\bgestion\b", r"\bfinance\b", r"\bcomptabilit[eé]\b", r"\bcomptable\b", r"\bcontr[ôo]leur de gestion\b",
        r"\bassistant(e) de direction\b", r"\bassistant(e) de gestion\b", r"\badministratif\b", r"\badministrative\b",
        r"\bsecr[eé]taire\b", r"\baudit\b", r"\btr[eé]sorerie\b", r"\bbanque\b", r"\bassurance\b"
    ],
    "Vente, Commerce & Relation Client": [
        r"\bvente\b", r"\bcommerce\b", r"\bcommercial(e)?\b", r"\brelation client\b", r"\bbusiness developer\b",
        r"\bsales\b", r"\baccount manager\b", r"\bcharg[eé](e)? d.?affaires\b", r"\bconseiller\b", r"\bvendeur\b",
        r"\bvendeuse\b", r"\bb2b\b", r"\bb2c\b"
    ],
    "Systèmes d'Information & Conseil": [
        r"\bconsultant(e)?\b", r"\bsyst[eè]me d.information\b", r"\berp\b", r"\bsap\b", r"\burbaniste\b",
        r"\btransformation digitale\b", r"\bdigitalisation\b", r"\bcrm\b"
    ]
}


# ────────────────────────────────────────────────────────
#  TECHNOLOGY DICTIONARIES
# ────────────────────────────────────────────────────────

PROGRAMMING_LANGUAGES = {
    # Key: canonical name, Value: list of variations / regex patterns
    # Use \b word boundaries to avoid false positives in French text
    "Python": ["\\bpython\\b"],
    "JavaScript": ["\\bjavascript\\b", "\\bjs\\b"],
    "TypeScript": ["\\btypescript\\b"],
    "Java": ["\\bjava\\b(?!script)"],
    "C#": ["\\bc#", "\\bc sharp\\b", "\\bcsharp\\b"],
    "C++": ["\\bc\\+\\+", "\\bcpp\\b"],
    "C": ["\\blangage c\\b", "\\bprogrammation c\\b"],
    "PHP": ["\\bphp\\b"],
    "Ruby": ["\\bruby\\b"],
    "Go": ["\\bgolang\\b"],
    "Rust": ["\\brust\\b"],
    "Swift": ["\\bswift\\b"],
    "Kotlin": ["\\bkotlin\\b"],
    "Scala": ["\\bscala\\b"],
    "R": ["\\blangage r\\b", "\\brstudio\\b"],
    "MATLAB": ["\\bmatlab\\b"],
    "Dart": ["\\bdart\\b"],
    "Perl": ["\\bperl\\b"],
    "SQL": ["\\bsql\\b", "\\bplsql\\b", "\\bpl/sql\\b", "\\bt-sql\\b", "\\btsql\\b"],
    "Shell/Bash": ["\\bbash\\b", "\\bscripting shell\\b"],
    "PowerShell": ["\\bpowershell\\b"],
    "Objective-C": ["\\bobjective-c\\b", "\\bobjective c\\b"],
    "VBA": ["\\bvba\\b", "\\bvisual basic\\b"],
    "Groovy": ["\\bgroovy\\b"],
    "Lua": ["\\blua\\b(?!\\w)"],
    "Solidity": ["\\bsolidity\\b"],
}

FRAMEWORKS_LIBRARIES = {
    # Frontend
    "React": ["\\breact\\b", "\\breactjs\\b", "\\breact\\.js\\b"],
    "Angular": ["\\bangular\\b"],
    "Vue.js": ["\\bvue\\.?js\\b", "\\bvuejs\\b"],
    "Next.js": ["\\bnext\\.js\\b", "\\bnextjs\\b"],
    "Nuxt.js": ["\\bnuxt\\b"],
    "Svelte": ["\\bsvelte\\b"],
    "jQuery": ["\\bjquery\\b"],
    "Bootstrap": ["\\bbootstrap\\b"],
    "Tailwind CSS": ["\\btailwind\\b"],
    "Material UI": ["\\bmaterial.?ui\\b", "\\bmui\\b"],
    # Backend
    "Node.js": ["\\bnode\\.?js\\b", "\\bnodejs\\b"],
    "Express.js": ["\\bexpress\\.?js\\b", "\\bexpressjs\\b"],
    "Django": ["\\bdjango\\b"],
    "Flask": ["\\bflask\\b"],
    "FastAPI": ["\\bfastapi\\b"],
    "Spring": ["\\bspring boot\\b", "\\bspring framework\\b", "\\bspringboot\\b"],
    "Laravel": ["\\blaravel\\b"],
    "Symfony": ["\\bsymfony\\b"],
    "Ruby on Rails": ["\\brails\\b", "\\bruby on rails\\b"],
    "ASP.NET": ["\\basp\\.net\\b", "\\baspnet\\b", "\\b\\.net core\\b", "\\bdotnet\\b"],
    ".NET": ["\\b\\.net\\b(?!.*core)"],
    "NestJS": ["\\bnestjs\\b", "\\bnest\\.js\\b"],
    # Mobile
    "React Native": ["\\breact native\\b"],
    "Flutter": ["\\bflutter\\b"],
    "SwiftUI": ["\\bswiftui\\b"],
    "Xamarin": ["\\bxamarin\\b"],
    # Data / ML
    "TensorFlow": ["\\btensorflow\\b"],
    "PyTorch": ["\\bpytorch\\b"],
    "Scikit-learn": ["\\bscikit\\b", "\\bsklearn\\b"],
    "Pandas": ["\\bpandas\\b"],
    "NumPy": ["\\bnumpy\\b"],
    "Spark": ["\\bapache spark\\b", "\\bpyspark\\b"],
    "Hadoop": ["\\bhadoop\\b"],
    "Kafka": ["\\bkafka\\b"],
    # DevOps
    "Terraform": ["\\bterraform\\b"],
    "Ansible": ["\\bansible\\b"],
    "Puppet": ["\\bpuppet\\b"],
}

TOOLS_PLATFORMS = {
    # Cloud
    "AWS": ["\\baws\\b", "\\bamazon web services\\b"],
    "Azure": ["\\bazure\\b"],
    "Google Cloud": ["\\bgcp\\b", "\\bgoogle cloud\\b", "\\bbigquery\\b"],
    "OVH Cloud": ["\\bovhcloud\\b"],
    # Databases
    "PostgreSQL": ["\\bpostgresql\\b", "\\bpostgres\\b"],
    "MySQL": ["\\bmysql\\b"],
    "MongoDB": ["\\bmongodb\\b", "\\bmongo\\b"],
    "Redis": ["\\bredis\\b"],
    "Oracle DB": ["\\boracle\\s+db\\b", "\\boracle\\s+database\\b"],
    "SQL Server": ["\\bsql server\\b", "\\bmssql\\b"],
    "Elasticsearch": ["\\belasticsearch\\b"],
    "MariaDB": ["\\bmariadb\\b"],
    "DynamoDB": ["\\bdynamodb\\b"],
    # DevOps / CI-CD
    "Docker": ["\\bdocker\\b"],
    "Kubernetes": ["\\bkubernetes\\b", "\\bk8s\\b"],
    "Jenkins": ["\\bjenkins\\b"],
    "GitLab CI": ["\\bgitlab[- ]ci\\b", "\\bgitlab\\b"],
    "GitHub Actions": ["\\bgithub actions\\b"],
    "CircleCI": ["\\bcircleci\\b"],
    "ArgoCD": ["\\bargocd\\b"],
    # Version control
    "Git": ["\\bgit\\b(?!hub|lab|ops)"],
    "SVN": ["\\bsvn\\b", "\\bsubversion\\b"],
    # IDEs / Editors
    "VS Code": ["\\bvs code\\b", "\\bvscode\\b", "\\bvisual studio code\\b"],
    "IntelliJ": ["\\bintellij\\b"],
    # Project management
    "Jira": ["\\bjira\\b"],
    "Confluence": ["\\bconfluence\\b"],
    "Trello": ["\\btrello\\b"],
    # Design
    "Figma": ["\\bfigma\\b"],
    "Adobe XD": ["\\badobe xd\\b"],
    "Photoshop": ["\\bphotoshop\\b"],
    "Illustrator": ["\\billustrator\\b"],
    # Data / Analytics
    "Power BI": ["\\bpower\\s*bi\\b"],
    "Tableau (Software)": ["\\btableau\\s+(?:software|desktop|server|online|prep)\\b"],
    "Grafana": ["\\bgrafana\\b"],
    "Datadog": ["\\bdatadog\\b"],
    # API / Integration
    "REST API": ["\\brest\\s*api\\b", "\\brestful\\b", "\\bapi\\s*rest\\b"],
    "GraphQL": ["\\bgraphql\\b"],
    "Swagger": ["\\bswagger\\b", "\\bopenapi\\b"],
    "Postman": ["\\bpostman\\b"],
    # Messaging
    "RabbitMQ": ["\\brabbitmq\\b"],
    # Testing
    "Selenium": ["\\bselenium\\b"],
    "Cypress": ["\\bcypress\\b"],
    "JUnit": ["\\bjunit\\b"],
    "pytest": ["\\bpytest\\b"],
    "SonarQube": ["\\bsonarqube\\b"],
    # Networking / Security
    "Nginx": ["\\bnginx\\b"],
    "Linux": ["\\blinux\\b", "\\bubuntu\\b", "\\bdebian\\b"],
    "Windows Server": ["\\bwindows server\\b"],
    # ERP / CRM
    "SAP": ["\\bsap\\b"],
    "Salesforce": ["\\bsalesforce\\b"],
    "ServiceNow": ["\\bservicenow\\b"],
}

CERTIFICATIONS = {
    "AWS Certified": ["\\baws certified\\b", "\\baws certification\\b", "\\bcertifié aws\\b"],
    "Azure Certified": ["\\bazure certified\\b", "\\baz-900\\b", "\\baz-104\\b", "\\baz-204\\b"],
    "Google Cloud Certified": ["\\bgoogle cloud certified\\b", "\\bgcp certified\\b"],
    "Cisco (CCNA/CCNP)": ["\\bccna\\b", "\\bccnp\\b", "\\bcisco certified\\b"],
    "CompTIA": ["\\bcomptia\\b", "\\bsecurity\\+", "\\bnetwork\\+"],
    "ITIL": ["\\bitil\\b"],
    "PMP": ["\\bpmp\\b"],
    "Scrum Master": ["\\bscrum master\\b", "\\bpsm\\b", "\\bcsm\\b"],
    "TOGAF": ["\\btogaf\\b"],
    "CISSP": ["\\bcissp\\b"],
    "Kubernetes (CKA)": ["\\bcka\\b", "\\bckad\\b", "\\bkubernetes certified\\b"],
    "PRINCE2": ["\\bprince2\\b"],
    "TOEIC": ["\\btoeic\\b"],
    "TOEFL": ["\\btoefl\\b"],
    "IELTS": ["\\bielts\\b"],
    "Certification AMF": ["\\bamf\\b", "\\bcertification amf\\b"],
    "ISTQB": ["\\bistqb\\b"],
    "CACES": ["\\bcaces\\b"],
    "Microsoft Certified": ["\\bmicrosoft certified\\b", "\\bmcp\\b"],
    "Salesforce Certified": ["\\bsalesforce certified\\b", "\\bcertification salesforce\\b"],
    "HubSpot Certified": ["\\bhubspot certified\\b", "\\bcertification hubspot\\b"],
    "Google Analytics": ["\\bgoogle analytics\\b", "\\bga4\\b", "\\bcertification google analytics\\b"],
    "CPA/DCG/DSCG": ["\\bcpa\\b", "\\bdcg\\b", "\\bdscg\\b", "\\bcca\\b"],
}

METHODOLOGIES = {
    "Agile": ["\\bagile\\b", "\\bagilité\\b", "\\bméthodologie agile\\b"],
    "Scrum": ["\\bscrum\\b"],
    "Kanban": ["\\bkanban\\b"],
    "DevOps": ["\\bdevops\\b"],
    "CI/CD": ["\\bci/cd\\b", "\\bci cd\\b", "\\bintégration continue\\b", "\\bdéploiement continu\\b"],
    "TDD": ["\\btdd\\b", "\\btest driven\\b"],
    "Clean Code": ["\\bclean code\\b"],
    "Design Patterns": ["\\bdesign patterns?\\b"],
    "Microservices": ["\\bmicroservices?\\b", "\\bmicro-services?\\b"],
    "Serverless": ["\\bserverless\\b"],
    "GitOps": ["\\bgitops\\b"],
    "MLOps": ["\\bmlops\\b"],
    "DataOps": ["\\bdataops\\b"],
    "SAFe": ["\\bscaled agile\\b"],
}

# ────────────────────────────────────────────────────────
#  CDD / NON-ALTERNANCE DETECTION
# ────────────────────────────────────────────────────────

NON_ALTERNANCE_KEYWORDS = [
    "cdd de remplacement",
    "cdd saisonnier",
    "contrat saisonnier",
    "mission intérimaire",
    "mission interim",
    "intérim",
    "interim",
    "vacation",
    "cdd classique",
    "cdd de droit commun",
    "pas en alternance",
    "hors alternance",
    "livecampus",
]

ALTERNANCE_POSITIVE = [
    "alternance", "alternant", "alternante",
    "apprentissage", "apprenti", "apprentie",
    "contrat de professionnalisation",
    "contrat pro",
    "en alternance",
    "formation en alternance",
]


def is_alternance_offer(title: str, description: Optional[str] = None,
                        contract_type: Optional[str] = None) -> bool:
    """
    Determine if an offer is truly an alternance contract.
    Returns True if the offer seems to be a real alternance.
    """
    text = f"{title or ''} {description or ''} {contract_type or ''}".lower()

    # Check for positive alternance signals
    has_alternance = any(kw in text for kw in ALTERNANCE_POSITIVE)

    # Check for negative signals (non-alternance CDD)
    has_non_alternance = any(kw in text for kw in NON_ALTERNANCE_KEYWORDS)

    if has_non_alternance and not has_alternance:
        return False

    return True


# ────────────────────────────────────────────────────────
#  SKILLS EXTRACTION
# ────────────────────────────────────────────────────────

def extract_skills(title: str, description: Optional[str] = None) -> Dict[str, List[str]]:
    """
    Extract IT skills, technologies, and certifications from text.

    Returns a dict with keys:
        - languages: programming languages found
        - frameworks: frameworks and libraries
        - tools: tools and platforms
        - certifications: professional certifications
        - methodologies: development methodologies
    """
    text = f"{title or ''} {description or ''}".lower()

    result = {
        "languages": _extract_from_dict(text, PROGRAMMING_LANGUAGES),
        "frameworks": _extract_from_dict(text, FRAMEWORKS_LIBRARIES),
        "tools": _extract_from_dict(text, TOOLS_PLATFORMS),
        "certifications": _extract_from_dict(text, CERTIFICATIONS),
        "methodologies": _extract_from_dict(text, METHODOLOGIES),
    }

    return result


def extract_skills_flat(title: str, description: Optional[str] = None) -> List[str]:
    """Extract all skills as a flat, deduplicated list."""
    skills = extract_skills(title, description)
    flat = []
    for category_skills in skills.values():
        flat.extend(category_skills)
    # Deduplicate while preserving order
    seen: Set[str] = set()
    unique: List[str] = []
    for s in flat:
        if s not in seen:
            seen.add(s)
            unique.append(s)
    return unique


def _extract_from_dict(text: str, skill_dict: Dict[str, List[str]]) -> List[str]:
    """Extract skills from text using a dictionary of patterns."""
    found = []
    for canonical_name, patterns in skill_dict.items():
        for pattern in patterns:
            try:
                if re.search(pattern, text, re.IGNORECASE):
                    found.append(canonical_name)
                    break  # Found this skill, move to next
            except re.error:
                if pattern.lower() in text:
                    found.append(canonical_name)
                    break
    return found


def get_all_technology_names() -> List[str]:
    """Return a sorted list of all known technology names."""
    all_names = set()
    for d in [PROGRAMMING_LANGUAGES, FRAMEWORKS_LIBRARIES, TOOLS_PLATFORMS,
              CERTIFICATIONS, METHODOLOGIES]:
        all_names.update(d.keys())
    return sorted(all_names)


def categorize_offer(title: str, description: Optional[str] = None) -> str:
    """Guess the business category based on title and description."""
    text = f"{title or ''} {description or ''}".lower()

    # Give double weight to title
    search_text = f"{(title or '').lower()} {(title or '').lower()} {text}"

    max_score = 0
    best_category = "Autre Développement & IT"

    for category, patterns in CATEGORIES.items():
        score = 0
        for pattern in patterns:
            # count occurrences
            matches = len(re.findall(pattern, search_text))
            score += matches * 2 if (title and category.lower() in title.lower()) else matches

        if score > max_score:
            max_score = score
            best_category = category

    return best_category


__all__ = [
    "extract_skills",
    "extract_skills_flat",
    "get_all_technology_names",
    "is_alternance_offer",
    "categorize_offer",
]
