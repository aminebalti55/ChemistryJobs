import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime, timedelta
import time
import re
import logging
from datetime import datetime, timedelta
import backoff  # Add this import

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

DB_FILE = "jobs.db"

# Expanded and refined keywords for chemistry jobs
EXCLUDED_KEYWORDS = {
    # Other fields to exclude
    "développeur", "developer", "programmer", "software engineer",
    "web developer", "full stack", "frontend", "backend",
    "technicien maintenance", "helpdesk", "support technique",
    
    # Business/Finance/Admin roles
    "comptable", "accountant", "finance", "audit", "commercial", 
    "marketing", "sales", "gestionnaire", "consultant fonctionnel",
    "administrative", "assistant", "secrétaire", "receptionist",
    "ressources humaines", "hr", "human resources",
    
    # Senior/Lead positions (keeping some flexibility for mid-level positions)
    "senior", "sénior", "pfe", "10 ans", "8 ans", "7 ans",
    "chef département", "directeur technique", "directeur usine",
    "head of department", "technical director", "plant director"
}

KEYWORDS = {
    'core': [
        # Primary chemistry roles (expanded)
        "chimiste", "chemist", "ingénieur chimiste", "chemical engineer",
        "analyste chimique", "chemical analyst", "laboratoire", "laboratory",
        "technicien laboratoire", "lab technician", "ingénieur procédés",
        "process engineer", "ingénieur qualité", "quality engineer",
        "ingénieur production", "production engineer",
        "scientifique", "scientist", "chercheur", "researcher",
        "analyste", "analyst", "technicien", "technician",
        "spécialiste", "specialist", "expert technique", "technical expert",
        "formulateur", "formulator", "développeur produit", "product developer"
    ],
    'specializations': [
        # Analytical techniques (expanded)
        "chimie analytique", "analytical chemistry",
        "HPLC", "chromatographie", "chromatography",
        "spectroscopie", "spectroscopy", "IR", "RMN", "NMR",
        "MEB", "SEM", "EDX", "rayons X", "X-ray",
        "ATG", "ATD", "DSC", "GC", "TLC", "LC-MS", "GC-MS",
        "microscopie", "microscopy", "UV-Vis", "ICP", "ICP-MS",
        "DRX", "XRD", "XRF", "fluorescence X",
        "analyse élémentaire", "elemental analysis",
        "AA", "absorption atomique", "atomic absorption",
        "électrochimie", "electrochemistry",
        
        # Chemistry fields (expanded)
        "chimie organique", "organic chemistry",
        "chimie inorganique", "inorganic chemistry",
        "chimie physique", "physical chemistry",
        "biochimie", "biochemistry", "biotechnologie", "biotechnology",
        "polymères", "polymers", "plasturgie", "plastics",
        "chimie des matériaux", "materials chemistry",
        "génie chimique", "chemical engineering",
        "chimie industrielle", "industrial chemistry",
        "chimie analytique", "analytical chemistry",
        "chimie verte", "green chemistry",
        "nanotechnologie", "nanotechnology",
        
        # Techniques and processes
        "validation", "contrôle qualité", "quality control",
        "assurance qualité", "quality assurance",
        "ISO 17025", "ISO 9001", "ISO 14001", "GMP", "BPF",
        "accréditation", "certification", "normalisation",
        "étalonnage", "calibration", "métrologie", "metrology",
        "BPL", "GLP", "pharmacopée", "pharmacopoeia",
        
        # Environmental and safety
        "traitement des eaux", "water treatment",
        "analyse environnementale", "environmental analysis",
        "HSE", "hygiène", "sécurité", "environnement",
        "développement durable", "sustainable development",
        "gestion des déchets", "waste management",
        "protection environnement", "environmental protection",
        "QHSE", "QSE", "HSE", "EHS"
    ],
    'job_titles': [
        # Specific job titles (expanded)
        "ingénieur chimiste", "chemical engineer",
        "analyste laboratoire", "laboratory analyst",
        "technicien laboratoire", "lab technician",
        "responsable laboratoire", "lab manager",
        "ingénieur procédés", "process engineer",
        "ingénieur qualité", "quality engineer",
        "chimiste R&D", "R&D chemist",
        "ingénieur validation", "validation engineer",
        "ingénieur production", "production engineer",
        "analyste contrôle qualité", "quality control analyst",
        "formulateur", "formulation scientist",
        "ingénieur développement", "development engineer",
        "ingénieur application", "application engineer",
        "ingénieur projet", "project engineer",
        "technicien analyse", "analytical technician",
        "technicien recherche", "research technician",
        "assistant laboratoire", "lab assistant",
        "chargé de recherche", "research associate",
        "responsable qualité", "quality manager",
        "coordinateur laboratoire", "lab coordinator",
        "specialist assurance qualité", "quality assurance specialist"
    ],
    'domains': [
        # Industry domains (expanded)
        "industrie pharmaceutique", "pharmaceutical",
        "industrie chimique", "chemical industry",
        "agroalimentaire", "food industry",
        "cosmétique", "cosmetics", "cosmétologie", "cosmetology",
        "pétrochimie", "petrochemical",
        "matériaux", "materials",
        "peintures", "coatings",
        "plastiques", "plastics",
        "textile", "textile",
        "environnement", "environmental",
        "energie", "energy",
        "métallurgie", "metallurgy",
        "cimenterie", "cement",
        "céramique", "ceramics",
        "détergents", "detergents",
        "adhésifs", "adhesives",
        "emballage", "packaging",
        "traitement surface", "surface treatment",
        "traitement eau", "water treatment",
        
        # Function areas
        "métrologie", "metrology",
        "instrumentation", "instrumentation",
        "R&D", "recherche", "research",
        "production", "manufacturing",
        "qualité", "quality",
        "analyse", "analysis",
        "laboratoire", "laboratory",
        "validation", "validation",
        "contrôle", "control",
        "développement", "development",
        "formulation", "formulation",
        "caractérisation", "characterization",
        "synthèse", "synthesis",
        "pilote", "pilot", "scale-up",
        "optimisation", "optimization"
    ]
}

BASE_URLS = {
    "optioncarriere": "https://www.optioncarriere.tn/emploi?s={query}&l=Tunisie",
    "keejob": "https://www.keejob.com/offres-emploi/?keywords={query}",
    "tunisietravail": "https://www.tunisietravail.net/search/{query}"
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1"
}

def create_session():
    session = requests.Session()
    retry_strategy = requests.adapters.Retry(
        total=5,  # Increase total retries
        backoff_factor=2,  # Increase backoff time between retries
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "POST"]
    )
    adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

# Add exponential backoff decorator for request functions
@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, requests.exceptions.Timeout),
    max_tries=5
)
def make_request(url, session=None, timeout=(30, 30)):
    """
    Make HTTP request with retries and backoff
    """
    if session is None:
        session = create_session()
    try:
        response = session.get(url, headers=HEADERS, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed for {url}: {str(e)}")
        raise
# In database initialization
def initialize_db():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Create jobs table with application tracking
        c.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            title TEXT,
            link TEXT UNIQUE,
            publish_date TEXT,
            location TEXT,
            experience TEXT,
            description TEXT,
            status TEXT,
            added_date TIMESTAMP,
            is_new BOOLEAN,
            is_old BOOLEAN,
            is_clicked BOOLEAN DEFAULT 0,
            application_attempts INTEGER DEFAULT 0,
            last_application_date TIMESTAMP,
            application_success BOOLEAN
        )
        ''')
        
        # Create application history table
        c.execute('''
        CREATE TABLE IF NOT EXISTS application_history (
            id INTEGER PRIMARY KEY,
            job_id INTEGER,
            application_date TIMESTAMP,
            success BOOLEAN,
            site_type TEXT,
            error_message TEXT,
            FOREIGN KEY (job_id) REFERENCES jobs (id)
        )
        ''')
        
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database initialization error: {e}")
    finally:
        conn.close()

def get_applied_job_links():
    """Get links of all successfully applied jobs"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''
            SELECT link FROM jobs 
            WHERE status = 'applied' 
            AND application_success = 1
        ''')
        applied_links = set(row[0] for row in c.fetchall())
        conn.close()
        return applied_links
    except sqlite3.Error as e:
        logging.error(f"Error fetching applied links: {e}")
        return set()

def record_application_attempt(job_id: int, success: bool, site_type: str, error_message: str = None):
    """Record an application attempt in the history"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Update jobs table
        c.execute('''
            UPDATE jobs 
            SET application_attempts = application_attempts + 1,
                last_application_date = ?,
                application_success = ?,
                status = ?
            WHERE id = ?
        ''', (datetime.now(), success, 'applied' if success else 'failed', job_id))
        
        # Add to history
        c.execute('''
            INSERT INTO application_history 
            (job_id, application_date, success, site_type, error_message)
            VALUES (?, ?, ?, ?, ?)
        ''', (job_id, datetime.now(), success, site_type, error_message))
        
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Error recording application attempt: {e}")
    finally:
        conn.close()

def get_application_stats():
    """Get detailed application statistics"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Get overall stats
        c.execute('''
            SELECT 
                COUNT(*) as total_applications,
                SUM(CASE WHEN application_success = 1 THEN 1 ELSE 0 END) as successful_applications,
                SUM(CASE WHEN application_success = 0 THEN 1 ELSE 0 END) as failed_applications,
                SUM(application_attempts) as total_attempts
            FROM jobs 
            WHERE status IN ('applied', 'failed')
        ''')
        
        stats = c.fetchone()
        
        # Get stats by site
        c.execute('''
            SELECT 
                site_type,
                COUNT(*) as attempts,
                SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successes
            FROM application_history
            GROUP BY site_type
        ''')
        
        site_stats = c.fetchall()
        
        conn.close()
        
        return {
            'overall': {
                'total_applications': stats[0],
                'successful_applications': stats[1],
                'failed_applications': stats[2],
                'total_attempts': stats[3]
            },
            'by_site': {
                row[0]: {'attempts': row[1], 'successes': row[2]}
                for row in site_stats
            }
        }
    except sqlite3.Error as e:
        logging.error(f"Error getting application stats: {e}")
        return None

def advanced_job_scoring(title, description):
    """
    Enhanced scoring system for job relevance
    """
    score = 0
    title_lower = title.lower()
    desc_lower = description.lower()
    
    # Check for excluded keywords first
    for excluded in EXCLUDED_KEYWORDS:
        if excluded in title_lower:
            # Special case for software architect
            if excluded == "architecte" and "logiciel" in title_lower:
                continue
            return 0

    # Score based on core keywords (highest weight)
    for keyword in KEYWORDS['core']:
        if keyword in title_lower:
            score += 5
        if keyword in desc_lower:
            score += 3

    # Score based on your specific technologies
    for keyword in KEYWORDS['specializations']:
        if keyword in title_lower:
            score += 4
        if keyword in desc_lower:
            score += 2

    # Score based on exact job titles
    for keyword in KEYWORDS['job_titles']:
        if keyword in title_lower:
            score += 6
        if keyword in desc_lower:
            score += 3

    # Score based on relevant domains
    for keyword in KEYWORDS['domains']:
        if keyword in title_lower:
            score += 3
        if keyword in desc_lower:
            score += 1

    # Bonus points for key combinations
    if ("symfony" in title_lower and "php" in title_lower) or \
       ("react" in title_lower and "javascript" in title_lower) or \
       ("full stack" in title_lower or "fullstack" in title_lower):
        score += 5

    # Bonus for junior positions
    if "junior" in title_lower or "débutant" in title_lower:
        score += 3

    return score

def should_exclude_job(title, description):
    """
    Check if a job should be excluded based on title and description
    """
    title_lower = title.lower()
    desc_lower = description.lower()
    
    # Check for excluded keywords
    for keyword in EXCLUDED_KEYWORDS:
        if keyword.lower() in title_lower:
            logging.info(f"Excluding job due to keyword '{keyword}' in title: {title}")
            return True
    
    # Check for experience requirements in description
    experience_patterns = [
        r'(\d+)[\s-]*ans? d\'expérience',
        r'expérience .*?(\d+)[\s-]*ans?',
        r'(\d+)[\s-]*years? experience',
        r'experience .*?(\d+)[\s-]*years?'
    ]
    
    for pattern in experience_patterns:
        match = re.search(pattern, desc_lower)
        if match:
            years = int(match.group(1))
            if years > 5:  # Exclude jobs requiring more than 5 years experience
                logging.info(f"Excluding job due to high experience requirement ({years} years): {title}")
                return True
    
    return False


def filter_jobs(jobs):
    """
    Filter jobs using the enhanced scoring system
    """
    scored_jobs = []
    
    for job in jobs:
        title, link, publish_date, location, experience, description = job
        
        # Skip jobs with excluded terms in title
        if should_exclude_job(title, description):
            continue
            
        score = advanced_job_scoring(title, description)
        
        # Only include jobs with a minimum relevance score
        if score > 8:  # Adjust threshold as needed
            scored_jobs.append((job, score))
            logging.info(f"Matched Job - Title: {title}, Score: {score}")

    # Sort by score in descending order
    scored_jobs.sort(key=lambda x: x[1], reverse=True)
    return [job for job, score in scored_jobs]

def parse_relative_date(date_str):
    """
    Enhanced date parsing with support for multiple formats and relative times
    """
    date_str = date_str.strip().lower()
    now = datetime.now()

    # Direct month mapping for various formats
    month_mapping = {
        'jan': 1, 'fev': 2, 'mar': 3, 'avr': 4, 'mai': 5, 'juin': 6,
        'juil': 7, 'août': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
        'janvier': 1, 'février': 2, 'mars': 3, 'avril': 4, 'mai': 5, 'juin': 6,
        'juillet': 7, 'aout': 8, 'septembre': 9, 'octobre': 10, 'novembre': 11, 'décembre': 12
    }

    try:
        # Handle "Il y a X minutes/heures/jours/mois"
        if "il y a" in date_str or "depuis" in date_str:
            number = int(re.search(r"(\d+)", date_str).group(1))
            
            if "minute" in date_str:
                return (now - timedelta(minutes=number)).date()
            elif "heure" in date_str:
                return (now - timedelta(hours=number)).date()
            elif "jour" in date_str:
                return (now - timedelta(days=number)).date()
            elif "mois" in date_str:
                return (now - timedelta(days=number * 30)).date()
            elif "semaine" in date_str:
                return (now - timedelta(weeks=number)).date()
        
        # Handle standard date format "dd/mm/yyyy"
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            pass

        # Handle "Month, Year" format
        for month_name in month_mapping.keys():
            if month_name in date_str:
                year_match = re.search(r'\d{4}', date_str)
                if year_match:
                    year = int(year_match.group())
                    month = month_mapping[month_name]
                    return datetime(year, month, 1).date()

        # Handle relative dates without "Il y a" (e.g., "2 jours")
        number_match = re.search(r"(\d+)", date_str)
        if number_match:
            number = int(number_match.group(1))
            if "jour" in date_str:
                return (now - timedelta(days=number)).date()
            elif "mois" in date_str:
                return (now - timedelta(days=number * 30)).date()
            elif "semaine" in date_str:
                return (now - timedelta(weeks=number)).date()

        # If no pattern matches, log it and return current date
        logging.warning(f"No pattern matched for date string: {date_str}")
        return now.date()

    except Exception as e:
        logging.warning(f"Date parsing failed for {date_str}: {e}")
        return now.date()

def fetch_job_details(url, site):
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch job details from {url}: {e}")
        return "N/A", "N/A", "N/A", "N/A"

    description, publish_date, location, experience = "N/A", "N/A", "N/A", "N/A"

    try:
        if site == "keejob":
            # Get description from the main content area
            desc_section = soup.find("div", class_="block_a", style=lambda x: x and "padding: 15px 30px 5px" in x)
            if desc_section:
                # Remove social buttons and meta sections
                for meta in desc_section.find_all("div", class_="meta_a"):
                    meta.decompose()
                description = desc_section.get_text(strip=True)

            # Get metadata from details section
            details_div = soup.find("div", class_="text", style="margin-bottom:20px;")
            if details_div:
                meta_sections = details_div.find_all("div", class_="meta")
                details = {}
                for meta in meta_sections:
                    label = meta.find("b")
                    if label:
                        key = label.text.strip(":").strip().lower()
                        value = meta.get_text(strip=True).replace(label.text, "").strip()
                        details[key] = value

                # Extract location
                location = details.get("lieu de travail", "N/A")
                
                # Build experience string
                exp_parts = []
                if "type de poste" in details:
                    exp_parts.append(details["type de poste"])
                if "expérience" in details:
                    exp_parts.append(details["expérience"])
                if "étude" in details:
                    exp_parts.append(f"Niveau: {details['étude']}")
                if "rémunération proposée" in details:
                    exp_parts.append(f"Salaire: {details['rémunération proposée']}")
                
                experience = " | ".join(exp_parts) if exp_parts else "N/A"

                # Add publish date
                if "publiée le" in details:
                    publish_date = details["publiée le"]

                # Add company info to description if available
                company = details.get("entreprise", "").strip()
                if company and company != "N/A":
                    description = f"Entreprise: {company} | {description}"
                    
    except Exception as e:
        logging.error(f"Error parsing job details for site {site}: {e}")

    if site == "optioncarriere":
            header = soup.find("header")
            if header:
                # Get description
                content_section = soup.find("section", class_="content")
                if content_section:
                    description = content_section.get_text(strip=True)
                else:
                    description = "N/A"

                # Get company name
                company_section = header.find("p", class_="company")
                if company_section:
                    company_name = company_section.get_text(strip=True)
                    description = f"Company: {company_name} | {description}"

                # Get location and contract details
                details_list = header.find("ul", class_="details")
                if details_list:
                    location = "N/A"
                    contract_info = []
                    
                    for detail in details_list.find_all("li"):
                        # Location
                        if detail.find("svg", {"class": "icon"}):
                            location_span = detail.find("span")
                            if location_span:
                                location = location_span.get_text(strip=True)
                        # Contract type and work time
                        else:
                            contract_text = detail.get_text(strip=True)
                            if contract_text:
                                contract_info.append(contract_text)
                    
                    if contract_info:
                        experience = " | ".join(contract_info)
                    else:
                        experience = "N/A"
                else:
                    location = "N/A"
                    experience = "N/A"

                # Get publish date
                publish_date = datetime.now().date()
                tags = header.find("ul", class_="tags")
                if tags:
                    date_badge = tags.find("span", class_="badge", string=lambda x: "jours" in str(x))
                    if date_badge:
                        date_text = date_badge.get_text(strip=True)
                        if "jours" in date_text:
                            try:
                                days = int(re.search(r"(\d+)", date_text).group(1))
                                publish_date = datetime.now().date() - timedelta(days=days)
                            except (ValueError, AttributeError) as e:
                                logging.error(f"Error parsing date from '{date_text}': {e}")

    elif site == "tunisietravail":
        try:
            # Get description from PostContent
            desc_section = soup.find("div", class_="PostContent")
            if desc_section:
                # Remove script tags and ads
                for script in desc_section.find_all(["script", "ins"]):
                    script.decompose()
                description = desc_section.get_text(strip=True)
            else:
                description = "N/A"

            # Get location and company info
            info_section = soup.find("div", class_="PostContent")
            if info_section:
                # Extract location
                location_match = re.search(r"Ville\s*›\s*([^›\n]+)", info_section.text)
                location = location_match.group(1).strip() if location_match else "N/A"
                
                # Extract company
                company_match = re.search(r"Entreprise\s*›\s*([^›\n]+)", info_section.text)
                company = company_match.group(1).strip() if company_match else "N/A"
                
                # Extract experience requirements from description
                exp_match = re.search(r"expérience\s*[d\']*au moins\s*(\d+)\s*ans?", description, re.IGNORECASE)
                experience = f"{exp_match.group(1)} ans d'expérience" if exp_match else "N/A"
                
                # Add company to description if found
                if company != "N/A":
                    description = f"Entreprise: {company} | {description}"
            else:
                location = "N/A"
                experience = "N/A"

            # Extract contract type if mentioned
            if "CDI" in description:
                experience = f"CDI | {experience}"
            elif "CDD" in description:
                experience = f"CDD | {experience}"

            return description, None, location, experience

        except Exception as e:
            logging.error(f"Error parsing tunisietravail job details: {str(e)}")
            return "N/A", None, "N/A", "N/A"

    return description, publish_date, location, experience


def get_existing_job_links():
    """Get all existing job links from the database"""
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('SELECT link FROM jobs')
        existing_links = set(row[0] for row in c.fetchall())
        conn.close()
        return existing_links
    except sqlite3.Error as e:
        logging.error(f"Error fetching existing links: {e}")
        return set()


def fetch_jobs_from_optioncarriere(keyword, existing_links):
    """Enhanced optioncarriere job fetching with duplicate prevention"""
    url = BASE_URLS["optioncarriere"].format(query=keyword)
    logging.info(f"Fetching jobs from optioncarriere for keyword: {keyword}")
    
    try:
        session = create_session()
        response = make_request(url, session)
        soup = BeautifulSoup(response.text, "html.parser")
        jobs = []

        job_listings = soup.find_all("article", class_="job")
        new_listings = 0
        skipped_listings = 0
        
        for job in job_listings:
            try:
                # Extract job title and link
                title_element = job.find("h2").find("a")
                if not title_element:
                    continue
                
                title = title_element.get_text(strip=True)
                link = f"https://www.optioncarriere.tn{title_element['href']}"

                # Skip if job already exists in database
                if link in existing_links:
                    skipped_listings += 1
                    logging.debug(f"Skipping existing job: {title}")
                    continue

                # Skip if job doesn't match criteria
                if should_exclude_job(title, ""):
                    logging.info(f"Skipping excluded job: {title}")
                    continue

                company_element = job.find("p", class_="company")
                company = company_element.get_text(strip=True) if company_element else "N/A"

                location_element = job.find("ul", class_="location").find("li")
                location = location_element.get_text(strip=True) if location_element else "N/A"

                date_element = job.find("footer").find("span", class_="badge")
                if date_element:
                    date_text = date_element.get_text(strip=True)
                    if "jours" in date_text:
                        days = int(re.search(r"(\d+)", date_text).group(1))
                        if days > 15:
                            continue
                    publish_date = parse_relative_date(date_text)
                else:
                    publish_date = datetime.now().date()

                # Only fetch details for new jobs
                description, _, _, experience = fetch_job_details(link, "optioncarriere")
                
                if company != "N/A":
                    description = f"Company: {company} | {description}"

                desc_element = job.find("div", class_="desc")
                if desc_element:
                    preview = desc_element.get_text(strip=True)
                    description = f"{preview} | {description}" if description != "N/A" else preview

                contract_type = job.find("li", string=lambda text: text and ("CDI" in text or "CDD" in text))
                if contract_type:
                    contract_text = contract_type.get_text(strip=True)
                    experience = f"{contract_text} | {experience}" if experience != "N/A" else contract_text

                jobs.append((title, link, publish_date, location, experience, description))
                new_listings += 1

            except Exception as e:
                logging.error(f"Error parsing job from optioncarriere: {str(e)}")
                continue

        logging.info(f"Optioncarriere summary for {keyword}: {new_listings} new jobs, {skipped_listings} existing jobs skipped")
        return jobs

    except Exception as e:
        logging.error(f"Failed to fetch jobs from optioncarriere for '{keyword}': {str(e)}")
        return []

def fetch_jobs_from_tunisietravail(keyword, existing_links):
    """Enhanced tunisietravail job fetching with duplicate prevention"""
    base_url = "https://www.tunisietravail.net/category/offres-d-emploi-et-recrutement/it/"
    
    it_categories = [
        "developpeur/",
        "developpeur-net-c-vb-java-jee/",
        "developpeur-web/",
        "ingenieur/"
    ]
    
    jobs = []
    new_listings = 0
    skipped_listings = 0
    logging.info(f"Fetching IT jobs from tunisietravail")
    
    try:
        session = create_session()
        
        for category in it_categories:
            category_url = base_url + category
            try:
                response = make_request(category_url, session)
                soup = BeautifulSoup(response.text, "html.parser")
                
                job_listings = soup.find_all("div", class_="Post")
                logging.info(f"Found {len(job_listings)} potential jobs in category {category}")
                
                for job in job_listings:
                    try:
                        title_tag = job.find("a", class_="h1titleall")
                        if not title_tag:
                            continue
                        
                        title = title_tag.text.strip()
                        link = title_tag["href"]

                        # Skip if job already exists
                        if link in existing_links:
                            skipped_listings += 1
                            logging.debug(f"Skipping existing job: {title}")
                            continue
                        
                        # Skip if job title doesn't match keywords
                        if not any(kw.lower() in title.lower() for kw in KEYWORDS['core'] + KEYWORDS['job_titles']):
                            continue
                        
                        date_section = job.find("p", class_="PostDateIndex")
                        publish_date = datetime.now().date()
                        if date_section:
                            month_tag = date_section.find("strong", class_="month")
                            if month_tag:
                                month_text = month_tag.text.strip()
                                try:
                                    publish_date = datetime.strptime(month_text, "%b, %Y").date()
                                except ValueError:
                                    publish_date = parse_relative_date(month_text)
                        
                        desc_preview = job.find("div", style=lambda x: x and "line-height:18px" in x)
                        preview_text = desc_preview.text.strip() if desc_preview else ""
                        
                        description, _, location, experience = fetch_job_details(link, "tunisietravail")
                        full_description = f"{preview_text} | {description}" if description != "N/A" else preview_text
                        
                        jobs.append((title, link, publish_date.strftime("%Y-%m-%d"), location, experience, full_description))
                        new_listings += 1
                        
                    except Exception as e:
                        logging.error(f"Error parsing job listing: {str(e)}")
                        continue
                        
            except Exception as e:
                logging.error(f"Error fetching category {category}: {str(e)}")
                continue

        logging.info(f"Tunisietravail summary: {new_listings} new jobs, {skipped_listings} existing jobs skipped")
        return jobs
        
    except Exception as e:
        logging.error(f"Failed to fetch jobs from tunisietravail: {str(e)}")
        return []

def fetch_jobs_from_keejob(keyword, existing_links):
    """Enhanced keejob job fetching with duplicate prevention"""
    url = BASE_URLS["keejob"].format(query=keyword)
    logging.info(f"Fetching jobs from keejob for keyword: {keyword}")
    
    try:
        session = create_session()
        response = make_request(url, session)
        soup = BeautifulSoup(response.text, "html.parser")
        jobs = []
        new_listings = 0
        skipped_listings = 0

        job_listings = soup.find_all("div", class_="block_white_a")
        logging.info(f"Found {len(job_listings)} potential jobs for '{keyword}' on keejob")

        for job in job_listings:
            try:
                title_tag = job.find("a", style="color: #005593;")
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                link = f"https://www.keejob.com{title_tag['href']}"

                # Skip if job already exists
                if link in existing_links:
                    skipped_listings += 1
                    logging.debug(f"Skipping existing job: {title}")
                    continue
                
                # Skip if job title doesn't match keywords
                if not any(kw.lower() in title.lower() for kw in KEYWORDS['core'] + KEYWORDS['job_titles']):
                    continue

                content_div = job.find("div", class_="content")
                company = "N/A"
                if content_div:
                    company_text = content_div.find("div", class_="span12").get_text(strip=True)
                    if company_text:
                        company = company_text.split('|')[0].strip()

                location_tag = job.find("i", class_="fa-map-marker")
                location = location_tag.next_sibling.strip() if location_tag else "N/A"

                date_div = job.find("div", class_="meta_a")
                publish_date = datetime.now().date()
                if date_div:
                    date_text = date_div.find("i", class_="fa-clock-o").next_sibling.strip()
                    publish_date = datetime.strptime(date_text, "%d/%m/%Y").date()

                description, _, _, experience = fetch_job_details(link, "keejob")
                
                jobs.append((title, link, publish_date, location, experience, description))
                new_listings += 1

            except Exception as e:
                logging.error(f"Error parsing job from keejob: {str(e)}")
                continue

        logging.info(f"Keejob summary for {keyword}: {new_listings} new jobs, {skipped_listings} existing jobs skipped")
        return jobs

    except Exception as e:
        logging.error(f"Failed to fetch jobs from keejob for '{keyword}': {str(e)}")
        return []
    
    
def save_job_to_db(title, link, publish_date, location, experience, description, status):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Convert publish_date to datetime if it's a string
        if isinstance(publish_date, str):
            publish_date = parse_relative_date(publish_date)
        
        # Calculate job age
        days_since_published = (datetime.now().date() - publish_date).days
        is_new = days_since_published < 3
        is_old = days_since_published > 15
        
        c.execute('''
        INSERT OR REPLACE INTO jobs (
            title, link, publish_date, location, experience, 
            description, status, added_date, is_new, is_old
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            title, link, publish_date.strftime("%Y-%m-%d"), location, 
            experience, description, status, datetime.now(), 
            is_new, is_old
        ))
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database save error: {e}")
    finally:
        conn.close()

def update_jobs_with_logging():
    try:
        # Existing update_jobs logic
        update_jobs()
        
        # Log successful update
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute('''
        CREATE TABLE IF NOT EXISTS update_log (
            last_update DATETIME
        )
        ''')
        c.execute('INSERT INTO update_log (last_update) VALUES (?)', 
                  (datetime.now().strftime('%Y-%m-%d %H:%M:%S'),))
        conn.commit()
        conn.close()
        
        logging.info("Job update completed successfully")
    except Exception as e:
        logging.error(f"Job update failed: {e}")
        raise
    
    
def update_jobs():
    """Enhanced job update process with duplicate prevention"""
    logging.info("Starting job update process...")
    
    # Get existing jobs from database first
    existing_links = get_existing_job_links()
    logging.info(f"Found {len(existing_links)} existing jobs in database")
    
    all_keywords = set(
        KEYWORDS['core'] + 
        KEYWORDS['specializations'] + 
        KEYWORDS['job_titles'] + 
        KEYWORDS['domains']
    )

    total_jobs_added = 0
    total_jobs_skipped = 0
    total_jobs_processed = 0
    failed_keywords = []

    for keyword in all_keywords:
        try:
            logging.info(f"Processing keyword: {keyword}")
            
            # Pass existing_links to each fetch function
            jobs_optioncarriere = fetch_jobs_from_optioncarriere(keyword, existing_links)
            jobs_tunisietravail = fetch_jobs_from_tunisietravail(keyword, existing_links)
            jobs_keejob = fetch_jobs_from_keejob(keyword, existing_links)

            all_jobs = jobs_optioncarriere + jobs_tunisietravail + jobs_keejob
            total_jobs_processed += len(all_jobs)
            
            filtered_jobs = filter_jobs(all_jobs)
            
            for job in filtered_jobs:
                if job[1] not in existing_links:  # Check link isn't in existing set
                    save_job_to_db(*job, "new")
                    total_jobs_added += 1
                    existing_links.add(job[1])  # Add to existing set to prevent duplicates
                else:
                    total_jobs_skipped += 1

        except Exception as e:
            logging.error(f"Error processing keyword {keyword}: {e}")
            failed_keywords.append(keyword)

    logging.info("Job update summary:")
    logging.info(f"Total jobs processed: {total_jobs_processed}")
    logging.info(f"New jobs added: {total_jobs_added}")
    logging.info(f"Existing jobs skipped: {total_jobs_skipped}")
    if failed_keywords:
        logging.warning(f"Failed keywords: {', '.join(failed_keywords)}")

    return total_jobs_added


if __name__ == "__main__":
    initialize_db()
    update_jobs_with_logging()


    while True:
        
        update_jobs()
        logging.info("Job list updated. Sleeping for 24 hours...")
        time.sleep(86400)