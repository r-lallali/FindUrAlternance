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
    "Agriculture et élevage": [
        r"\bagriculture\b", r"\b[eé]levage\b", r"\bagricole\b", r"\bagronome\b", r"\bviticulture\b",
        r"\bmaraich[eé]?r\b", r"\bharticulteur\b", r"\bforestier\b", r"\bsylviculture\b",
        r"\bv[eé]t[eé]rinaire\b", r"\banimal\b", r"\bzootechnie\b",
    ],
    "Audiovisuel et spectacle": [
        r"\baudiovisuel\b", r"\bspectacle\b", r"\bcinéma\b", r"\bcinema\b", r"\bradio\b", r"\bt[eé]l[eé]vision\b",
        r"\bproduction audiovisuelle\b", r"\br[eé]alisateur\b", r"\bcam[eé]raman\b", r"\bsound design\b",
        r"\bmonteur vid[eé]o\b", r"\banimateur\b", r"\bartisteq\b",
    ],
    "Automobile": [
        r"\bautomobile\b", r"\bm[eé]canique\b", r"\bcarrosserie\b", r"\bconcessionnaire\b",
        r"\btech?nicien automobile\b", r"\br[eé]paration automobile\b", r"\bpeinture auto\b",
        r"\bvehicule\b", r"\bv[eé]hicule\b", r"\belectronique automobile\b",
    ],
    "Bâtiment et travaux publics (BTP)": [
        r"\bbtp\b", r"\bb[âa]timent\b", r"\btravaux publics\b", r"\bconstruction\b", r"\bgénie civil\b",
        r"\barchitecte\b", r"\barchitecture\b", r"\btopographe\b", r"\bchantier\b",
        r"\bplombier\b", r"\b[eé]lectricien\b", r"\bma[çc]on\b", r"\bcouvreur\b",
        r"\bcharpentier\b", r"\bmenuisier\b", r"\b[eé]lectricit[eé]\b", r"\bthermal\b",
    ],
    "Commerce et distribution": [
        r"\bvente\b", r"\bcommercial(e)?\b", r"\bdistribution\b", r"\bgrande surface\b",
        r"\bgrande distribution\b", r"\bresponsable de rayon\b", r"\bchef de rayon\b",
        r"\bcaissier\b", r"\bvendeur\b", r"\bachat\b", r"\bapprovisionn\b",
        r"\bm[eé]tier du commerce\b", r"\brelation client\b", r"\bsales\b",
        r"\baccount manager\b", r"\bbusiness developer\b", r"\bcharg[eé](e)? d.?affaires\b",
    ],
    "Communication et marketing": [
        r"\bmarketing\b", r"\bcommunication\b", r"\bseo\b", r"\bsea\b", r"\bcommunity manager\b",
        r"\btraffic manager\b", r"\be-commerce\b", r"\bacquisition\b", r"\bcontent manager\b",
        r"\bgraphiste\b", r"\bdirecteur artistique\b", r"\b[eé]v[eé]nementiel\b", r"\brelations? presse\b",
        r"\binfluenceur\b", r"\bdigital marketing\b", r"\bbrand\b", r"\bstrat[eé]gie\b",
    ],
    "Culture et patrimoine": [
        r"\bpatrimoine\b", r"\bmus[eé]e\b", r"\bculture\b", r"\bculturel\b", r"\bbibliothequ\b",
        r"\barchives\b", r"\bconservateur\b", r"\barch[eé]ologie\b", r"\bbeaux.arts\b",
        r"\bm[eé]diation culturelle\b",
    ],
    "Défense et sécurité": [
        r"\bd[eé]fense\b", r"\bs[eé]curit[eé]\b", r"\barm[eé]e\b", r"\bpolicier\b",
        r"\bgendarmerie\b", r"\bsurveillance\b", r"\bg[eé]n[eé]ral de s[eé]curit[eé]\b",
        r"\bcybers[eé]curit[eé]\b", r"\bcybersecurity\b", r"\bsecops\b", r"\bsoc\b",
        r"\bpent?est\b", r"\brisque\b", r"\bagent de s[eé]curit[eé]\b",
    ],
    "Droit et justice": [
        r"\bdroit\b", r"\bjuridique\b", r"\bjustice\b", r"\bavocat\b", r"\bnotaire\b",
        r"\bparalegal\b", r"\bjuriste\b", r"\bcontrat\b", r"\blegale?\b", r"\bcompliance\b",
        r"\blitiges?\b", r"\btribunal\b",
    ],
    "Enseignement et formation": [
        r"\benseignement\b", r"\bformation\b", r"\bpedagogie\b", r"\bp[eé]dagogi\b",
        r"\bprofesseur\b", r"\bformateur\b", r"\bcoach\b", r"\banimateur p[eé]dagogique\b",
        r"\b[eé]ducation\b", r"\btutor\b", r"\blycee\b", r"\buniversit[eé]\b",
    ],
    "Environnement": [
        r"\benvironnement\b", r"\b[eé]cologie\b", r"\bd[eé]veloppement durable\b", r"\bqhse\b",
        r"\bqse\b", r"\brse\b", r"\b[eé]nergie renouvelable\b", r"\bsolaire\b", r"\b[eé]olien\b",
        r"\bgestion des d[eé]chets\b", r"\bbiodiversit[eé]\b", r"\bhydrogene\b",
    ],
    "Finance, banque et assurance": [
        r"\bfinance\b", r"\bbanque\b", r"\bassurance\b", r"\bcontr[ôo]leur de gestion\b",
        r"\baudit\b", r"\btr[eé]sorerie\b", r"\banalyste financier\b", r"\binvestissement\b",
        r"\bcash management\b", r"\bcomptabilit[eé]\b", r"\bcomptable\b", r"\bfiscalit[eé]\b",
        r"\bfinancier\b", r"\bcredit\b", r"\bactif\b", r"\bpaie\b", r"\bpayone\b",
    ],
    "Gestion administrative et ressources humaines": [
        r"\bressources humaines\b", r"\brh\b(?! alternance)", r"\brecrutement\b", r"\brecruteur\b",
        r"\badministratif\b", r"\badministration\b", r"\bsecr[eé]taire\b", r"\bassistant(e) de direction\b",
        r"\bassistant(e) de gestion\b", r"\btalent acquisition\b", r"\bgestion des talents\b",
        r"\bdrh\b", r"\bcharg[eé](e)? de recrutement\b", r"\bpaie\b",
    ],
    "Hôtellerie et restauration": [
        r"\bh[oô]tellerie\b", r"\brestauration\b", r"\bcuisinier\b", r"\bchef de cuisine\b",
        r"\bserveur\b", r"\bh[oô]tel\b", r"\br[eé]ception\b", r"\bfood\b", r"\bbarmaid?\b",
        r"\bsommelière?\b", r"\bpatisserie\b", r"\bboulangerie\b", r"\btraiteur\b",
    ],
    "Immobilier": [
        r"\bimmobilier\b", r"\bagent immobilier\b", r"\bgestion locative\b", r"\bpropri[eé]t[eé]\b",
        r"\bpromoteur\b", r"\btransaction immobili[eè]re\b", r"\bbailleur\b", r"\bloyer\b",
    ],
    "Industrie - Chimie": [
        r"\bchimie\b", r"\bchimiste\b", r"\blaboratoire\b", r"\banalyse chimique\b",
        r"\bpharmacie\b", r"\bcosm[eé]tique\b", r"\bprocéd[eé]s industriels\b",
        r"\bpétrochimie\b", r"\bplastique\b", r"\bcaoutchouc\b",
    ],
    "Industries": [
        r"\bindustriel\b", r"\bindustrie\b", r"\bproduction\b", r"\bopérateur\b",
        r"\bcontr[ôo]le qualit[eé]\b", r"\bqualit[eé]\b", r"\blean\b", r"\bsix sigma\b",
        r"\bam[eé]lioration continue\b", r"\bm[eé]thodes\b", r"\bing[eé]nieur(e)? d.[eé]tudes\b",
        r"\bm[eé]canique industrielle\b", r"\baeronautique\b", r"\baéronautique\b",
        r"\belectronique\b", r"\b[eé]lectronique\b", r"\bautomatisme\b",
    ],
    "Informatique, internet et télécommunication": [
        r"\bd[eé]veloppeur\b", r"\bd[eé]veloppeuse\b", r"\bdeveloper\b", r"\bfront.?end\b",
        r"\bback.?end\b", r"\bfull.?stack\b", r"\bsoftware engineer\b", r"\bwebmaster\b",
        r"\btech lead\b", r"\bprogrammeur\b", r"\binfrastructure\b", r"\br[eé]seau(x)?\b",
        r"\bcloud\b", r"\bdevops\b", r"\bsysadmin\b", r"\bdata\b", r"\bmachine learning\b",
        r"\bintelligence artificielle\b", r"\bdata scientist\b", r"\bbusiness intelligence\b",
        r"\bchef(fe)? de projet\b", r"\bproject manager\b", r"\bconsultant(e)? (it|informatique)\b",
        r"\btransformation digitale\b", r"\bsi\b", r"\bux\b", r"\bui\b", r"\bdesigner\b",
        r"\bproduct owner\b", r"\bproduct manager\b", r"\btelecommunication\b", r"\biot\b",
        r"\binformatique\b", r"\bnumerique\b", r"\bnumérique\b", r"\bdigital\b",
        r"\bcybersécurité\b", r"\bcybers[eé]curit[eé]\b",
    ],
    "Logistique et transport": [
        r"\blogistique\b", r"\btransport\b", r"\bsupply chain\b", r"\bcha[iî]ne logistique\b",
        r"\bentrepos?\b", r"\bmagasinier\b", r"\blivreur\b", r"\bchauffeur\b",
        r"\bfret\b", r"\bdouane\b", r"\baffrètement\b", r"\bcariste\b",
    ],
    "Maintenance et entretien": [
        r"\bmaintenance\b", r"\bentretien\b", r"\btechnicien(ne)? de maintenance\b",
        r"\bd[eé]pannage\b", r"\br[eé]paration\b", r"\belectricien\b", r"\b[eé]lectricien\b",
        r"\bCVC\b", r"\bfroid\b", r"\bclimatisation\b", r"\binstrumentation\b",
    ],
    "Mode": [
        r"\bmode\b", r"\bvestimentaire\b", r"\btextile\b", r"\bcouture\b", r"\bmod[eé]lisme\b",
        r"\bstyliste\b", r"\bluxe\b", r"\bmaroquinerie\b", r"\bjoaillerie\b", r"\bbijouterie\b",
    ],
    "Recherche": [
        r"\brecherche\b", r"\br&d\b", r"\brecherche et d[eé]veloppement\b", r"\bchercheur\b",
        r"\binnovation\b", r"\bscientifique\b", r"\bthèse\b", r"\bphd\b", r"\blaboratoire\b",
        r"\bbiotechnologie\b", r"\bbiologie\b",
    ],
    "Santé": [
        r"\bsant[eé]\b", r"\bm[eé]decin\b", r"\bm[eé]dical\b", r"\binfirmier\b",
        r"\bpharmacien\b", r"\bkinesith[eé]rapeute\b", r"\bpsychologue\b", r"\borthophoniste\b",
        r"\bnursing\b", r"\bh[oô]pital\b", r"\bclinique\b", r"\bsoin\b",
    ],
    "Service à la collectivité et service public": [
        r"\bservice public\b", r"\bcollectivit[eé]\b", r"\bmairie\b", r"\bcommune\b",
        r"\bcommunaut[eé] de communes\b", r"\brad[eé]p\b", r"\bfonction publique\b",
        r"\bbureautique\b", r"\baccueil\b", r"\banimation socioculturelle\b",
    ],
    "Service à la personne": [
        r"\bservice [aà] la personne\b", r"\baide [aà] domicile\b", r"\bgardeq?\b",
        r"\bassistante? maternelle\b", r"\bauxiliaire de vie\b", r"\bpetite enfance\b",
        r"\b[eé]ducateur\b", r"\banimateur\b", r"\bsocial\b", r"\bmsap\b",
    ],
    "Tourisme": [
        r"\btourisme\b", r"\bgu[ia]de touristique\b", r"\bagence de voyage\b", r"\bh[oô]tel\b",
        r"\breservation\b", r"\banimateur touristique\b", r"\bcruisi[eè]re\b",
    ],
}



# ────────────────────────────────────────────────────────
#  TECHNOLOGY DICTIONARIES
# ────────────────────────────────────────────────────────

PROGRAMMING_LANGUAGES = {
    # Key: canonical name, Value: list of variations / regex patterns
    # Use \b word boundaries to avoid false positives in French text
    "Python": ["\\bpython\\b"],
    "JavaScript": ["\\bjavascript\\b", "\\bjs\\b", "\\bjava[\\s-]*script\\b"],
    "TypeScript": ["\\btypescript\\b"],
    "Java": ["\\bjava\\b(?![\\s\\-/]*script)"],
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
    "vacataire",
    "cdd classique",
    "cdd de droit commun",
    "stage",
    "stagiaire",
    "internship",
    "pas en alternance",
    "hors alternance",
    "livecampus",
    "poste à pourvoir immédiatement",
    "poste a pourvoir immediatement",
    "reprise d'ancienneté",
    "reprise d'anciennete",
    "ancienneté reprise",
    "directeur adjoint",
    "directrice adjointe",
    "chef de service",
    "responsable adjoint",
    "responsable de magasin",
    "poste en cdi",
    "poste en cdd",
]

# Keywords that suggest an offer is for a graduated profile (CDI/CDD) rather than an alternant
GRADUATED_INDICATORS = [
    "titulaire d'un", "titulaire d'une", "diplômé d'un", "diplômé d'une",
    "diplôme d'un", "diplôme d'une", "possédez un bac+", "détenez un bac+",
    "connaissance approfondie", "confirmé", "sénior", "senior",
    "expérience de minimum 2 ans", "expérience de minimum 3 ans",
    "expérience d'au moins 2 ans", "expérience d'au moins 3 ans",
    "expérimenté", "expert", "responsable de dossier",
    "de formation en comptabilité", "en cabinet d'expertise-comptable",
    "minimum 2 ans", "minimum 3 ans", "minimum 5 ans",
    "diplôme d'état requis", "diplôme d'état exigé", "diplôme d'état obligatoire",
    "de requis", "de obligatoire", "de exigé", "diplôme requis", "diplome requis",
    "titre requis", "carte professionnelle requis", "carte pro requis",
    "expérience exigée", "experience exigee", "expérience confirmée",
    "expérience de 2 ans", "expérience de 3 ans", "expérience de 5 ans",
    "expérience de 10 ans", "expérience de 15 ans",
    "reprise de l'ancienneté", "rémunération selon profil",
    "rémunération selon expérience", "reprise d'ancienneté",
]

ALTERNANCE_POSITIVE = [
    "alternance", "alternant", "alternante",
    "en alternance",
    "formation en alternance",
]


def is_alternance_offer(title: str, description: Optional[str] = None,
                        contract_type: Optional[str] = None) -> bool:
    """
    Determine if an offer is truly an alternance contract.
    Returns True if the offer seems to be a real alternance.
    """
    title_val = (title or "").lower()
    desc_val = (description or "").lower()
    contract_val = (contract_type or "").lower()
    text = f"{title_val} {desc_val} {contract_val}"

    # Check for positive alternance signals
    has_alternance_in_title = any(kw in title_val for kw in ALTERNANCE_POSITIVE)
    has_alternance = any(kw in text for kw in ALTERNANCE_POSITIVE)

    # Specific check for excluded legal forms (pro and apprenticeship)
    # The user specifically requested to exclude BOTH pro and apprenticeship
    is_legal_form = any(kw in text for kw in ["contrat de professionnalisation", "contrat pro", "professionnalisation", "apprentissage", "apprenti"])
    if is_legal_form and not has_alternance_in_title and "alternance" not in contract_val and not has_alternance:
        return False

    # Check for negative signals (non-alternance CDD/CDI patterns)
    has_non_alternance = any(kw in text for kw in NON_ALTERNANCE_KEYWORDS)

    # 1. Reject if non-alternance keywords found and no strong alternance signal in title
    if has_non_alternance:
        # Special case: if it says "stage/alternance" or similar, we might keep it IF alternance is in title
        if any(kw in text for kw in ["stage", "stagiaire", "internship"]):
            # If "alternance" or "apprentissage" is in the TITLE, we might accept a "stage/alternance" mention
            if not has_alternance_in_title:
                return False
        elif not has_alternance_in_title:
            return False
        
    # 2. Strict exclusion for medical/regulated professions if no 'alternant' in title
    if not has_alternance_in_title:
        # Check for regulated professions that are often misclassified
        regulated = ["infirmier", "infirmière", "aide-soignant", "aide soignant", "médecin", "docteur", "pharmacien", "chirurgien", "dentiste", "kinésithérapeute", "sage-femme"]
        if any(prof in title_val for prof in regulated):
            # If regulated profession and mentions "diplôme d'état" or "de requis"
            # We are more aggressive even if "étudiant" is mentioned if it looks like mentoring
            if any(ind in desc_val for ind in ["diplôme d'état", "de requis", "diplôme requis"]):
                # If "encadrer les étudiants" or similar is the only mention of students, it's still a professional job
                learning = ["apprenti", "alternance", "apprentissage", "contrat pro"]
                if not any(l in desc_val for l in learning):
                    # Even if "étudiant" is there, if it's "encadrer" or "tuteur", it's a pro job
                    if "étudiant" in desc_val and any(exp in desc_val for exp in ["encadrer", "encadrement", "tutorat", "former les"]):
                        return False
                    # If no other learning keywords, then it's likely a pro job
                    if not any(l in desc_val for l in ["alternance", "apprenti"]):
                        return False

    # 3. Strong negative signal: 'reprise d'ancienneté' or professional experience requirement
    if not has_alternance_in_title:
        if any(kw in text for kw in ["reprise d'ancienneté", "reprise d'anciennete", "reprise de l'ancienneté"]):
            return False

    # 4. Reject if it looks like a CDI/CDD for graduates
    if not has_alternance_in_title:
        # Pattern for "X ans" - more aggressive, 1 year might be okay for some, but 2+ is very suspicious
        exp_match = re.search(r'(\d+)\s*(?:ans|années?|ans d\'expérience)\b', desc_val)
        if exp_match:
            years = int(exp_match.group(1))
            if years >= 2:
                # If it mentions experience or similar context and NO current student keywords
                if any(kw in desc_val for kw in ["expérience", "experience", "en cabinet", "pratique professionnel", "justifiez d'une", "maîtrise des"]):
                    # If "alternance" or "apprentissage" is NOT in the text at all, reject
                    if not any(kw in text for kw in ["stage", "apprentissage", "alternant", "apprenti"]):
                        return False
        
        # Count graduated indicators
        indicators_found = 0
        for ind in GRADUATED_INDICATORS:
            if ind in desc_val:
                indicators_found += 1
        
        # If we have multiple graduate indicators, it's likely not an alternance
        if indicators_found >= 2:
            return False
            
        # Check for standalone "CDI" if no alternance keywords are present
        if "cdi" in text and not any(kw in text for kw in ["alternance", "apprentissage", "contrat pro"]):
            return False

    # Final guard: no alternance signal at all + explicitly non-alternance contract → reject
    if not has_alternance:
        _NON_ALT_CONTRACTS = ("cdi", "cdd", "intérim", "interim", "indépendant", "freelance", "stage")
        if contract_val and any(c in contract_val for c in _NON_ALT_CONTRACTS):
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

    languages = _extract_from_dict(text, PROGRAMMING_LANGUAGES)
    frameworks = _extract_from_dict(text, FRAMEWORKS_LIBRARIES)
    tools = _extract_from_dict(text, TOOLS_PLATFORMS)
    certifications = _extract_from_dict(text, CERTIFICATIONS)
    methodologies = _extract_from_dict(text, METHODOLOGIES)

    # Post-processing: Handle Java vs JavaScript false positives
    # If both Java and (JS or TS) are found, check if it's really a Java job
    if "Java" in languages and ("JavaScript" in languages or "TypeScript" in languages):
        # List of indicators that it's likely a real Java job
        java_indicators = ["Spring", "Hibernate", "JUnit", "Maven", "Gradle", "IntelliJ", "J2EE", "JEE", "Quarkus"]
        has_java_indicator = any(ind in frameworks or ind in tools for ind in java_indicators)
        
        # Check if "Java" (standalone) appears in the original text (title or description)
        # We look for "java" NOT followed by "script" (with optional space/dash)
        has_standalone_java = re.search(r'\bjava\b(?![ \-/]*script)', text, re.IGNORECASE) is not None
        
        # If it doesn't have Java-specific frameworks/tools AND doesn't even have a standalone "Java" 
        # (meaning all "Java" matches were actually parts of "Java Script"), then remove it.
        if not has_java_indicator and not has_standalone_java:
            languages.remove("Java")

    return {
        "languages": languages,
        "frameworks": frameworks,
        "tools": tools,
        "certifications": certifications,
        "methodologies": methodologies,
    }


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


def categorize_offer(title: str, description: Optional[str] = None) -> Optional[str]:
    """Guess the business category based on title and description. Returns None if uncertain."""
    title_lower = (title or "").lower()
    desc_lower = (description or "").lower()
    # Title weighted 3x
    search_text = f"{title_lower} {title_lower} {title_lower} {desc_lower}"

    max_score = 0
    best_category = None

    for category, patterns in CATEGORIES.items():
        score = 0
        for pattern in patterns:
            matches = len(re.findall(pattern, search_text))
            score += matches

        if score > max_score:
            max_score = score
            best_category = category

    # Require minimum score to avoid random assignments
    return best_category if max_score >= 2 else None


__all__ = [
    "extract_skills",
    "extract_skills_flat",
    "get_all_technology_names",
    "is_alternance_offer",
    "categorize_offer",
]
