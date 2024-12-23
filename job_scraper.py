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
KEYWORDS = {
    "core": [
        "chimiste", "chemist", "chimie", "chemistry"
    ],
    "specializations": [
        "chimie analytique", "analytical chemistry", 
        "chimie organique", "organic chemistry",
        "chimie des matériaux", "materials chemistry",
        "chimie alimentaire", "food chemistry",
        "chimie pharmaceutique", "pharmaceutical chemistry",
        "chimie environnementale", "environmental chemistry", "instrumentation", "validation de processus", 
        "contrôle qualité", "quality control",
        "analyse environnementale", "environmental analysis",
        "biochimie", "biochemistry","traitement des eaux", "water treatment",
        "purification d'eau", "water purification",
        "gestion des eaux", "water management",
        "analyse des eaux", "water analysis",  "hse", "qhse", "health safety environment",
        "qualité hygiène sécurité environnement"
    ],
    "job_titles": [
        "chimiste", "chemist",
        "technicien chimie", "chemistry technician",
        "analyste chimique", "chemical analyst",
        "ingénieur chimie", "chemical engineer",
        "chercheur chimie", "chemistry researcher", "contrôle qualité", "quality control specialist" ,"expert traitement des eaux", "water treatment specialist",
        "ingénieur eau", "water engineer","hse","qhse", "responsable hse", "hse manager",
        "chargé qhse", "qhse officer"

    ],
    "domains": [
        "laboratoire", "laboratory",
        "recherche chimique", "chemical research",
        "contrôle qualité chimique", "chemical quality control",
        "analyse chimique", "chemical analysis",
        "spectroscopie", "spectroscopy",
        "chromatographie", "chromatography",
        "microscope électronique", "electron microscope",
        "analyse des eaux", "water analysis",
        "émissions industrielles", "industrial emissions",
        "analyse médicale", "medical analysis",
        "hse", "health safety environment","gestion environnementale", "environmental management",
        "recyclage des eaux", "water recycling", "qualité hygiène sécurité", "quality hygiene safety",
        "management environnemental", "environmental management" 
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
            is_clicked BOOLEAN DEFAULT 0  
        )
        ''')
        conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database initialization error: {e}")
    finally:
        conn.close()

def advanced_job_scoring(title, description):
    """
    Enhanced job scoring mechanism with weighted keyword matching
    """
    score = 0
    total_keywords = (
        KEYWORDS['core'] + 
        KEYWORDS['specializations'] + 
        KEYWORDS['job_titles'] + 
        KEYWORDS['domains']
    )

    # Case-insensitive matching with additional scoring
    for category, weight in [
        (KEYWORDS['core'], 3),
        (KEYWORDS['specializations'], 5),
        (KEYWORDS['job_titles'], 4),
        (KEYWORDS['domains'], 2)
    ]:
        for keyword in category:
            # Count occurrences with case-insensitive matching
            title_matches = len(re.findall(rf'\b{re.escape(keyword)}\b', title, re.IGNORECASE))
            desc_matches = len(re.findall(rf'\b{re.escape(keyword)}\b', description, re.IGNORECASE))
            
            # Weighted scoring
            score += (title_matches * weight * 2) + (desc_matches * weight)

    return score

def filter_jobs(jobs):
    """
    Advanced job filtering with detailed scoring and logging
    """
    scored_jobs = []
    for job in jobs:
        title, link, publish_date, location, experience, description = job
        score = advanced_job_scoring(title, description)
        
        if score > 5:  # Adjust threshold as needed
            scored_jobs.append((job, score))
            logging.info(f"Matched Job - Title: {title}, Score: {score}")

    # Sort by score in descending order
    scored_jobs.sort(key=lambda x: x[1], reverse=True)
    return [job for job, score in scored_jobs]

def parse_relative_date(date_str):
    """
    Enhanced date parsing with multiple format support
    """
    date_str = date_str.strip()
    now = datetime.now()

    # Direct month parsing for tunisie travail format
    month_mapping = {
        'Jan': 1, 'Fev': 2, 'Mar': 3, 'Avr': 4, 'Mai': 5, 'Juin': 6,
        'Juil': 7, 'Août': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
    }

    try:
        # Try parsing standard formats
        try:
            return datetime.strptime(date_str, "%d/%m/%Y").date()
        except ValueError:
            pass

        # Try parsing relative time like "Il y a X jours"
        days_match = re.search(r"(\d+)", date_str)
        if days_match:
            days_ago = int(days_match.group(1))
            return (now - timedelta(days=days_ago)).date()

        # Try parsing month format like "Déc, 2024"
        month_match = re.match(r"(\w+),\s*(\d{4})", date_str)
        if month_match:
            month_abbr, year = month_match.groups()
            month_num = month_mapping.get(month_abbr[:3].capitalize(), now.month)
            return datetime(int(year), month_num, 1).date()

    except Exception as e:
        logging.warning(f"Date parsing failed for {date_str}: {e}")
        return now.date()

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
            desc_section = soup.find("div", class_="block_a")
            if desc_section:
                description = desc_section.get_text(strip=True)
            publish_date_section = soup.find("i", class_="fa-clock-o")
            if publish_date_section:
                publish_date = publish_date_section.next_sibling.strip()
            location_section = soup.find("i", class_="fa-map-marker")
            if location_section:
                location = location_section.next_sibling.strip()

        elif site == "optioncarriere":
            header = soup.find("header")
            if header:
                description_section = header.find_next("section", class_="content")
                description = description_section.get_text(strip=True) if description_section else "N/A"

                publish_date_section = header.find("span", class_="badge")
                if publish_date_section:
                    relative_time = publish_date_section.get_text(strip=True)
                    publish_date = parse_relative_date(relative_time)

                location_section = header.find("svg", {"class": "icon"})
                if location_section:
                    location = location_section.find_next("span").get_text(strip=True)

        elif site == "tunisietravail":
            desc_section = soup.find("div", class_="PostContent")
            if desc_section:
                description = desc_section.get_text(strip=True)

            publish_date_section = soup.find("p", class_="PostDate")
            if publish_date_section:
                publish_date = publish_date_section.find("strong", class_="month")
                publish_date = publish_date.get_text(strip=True) if publish_date else "N/A"

            location_section = soup.find("p", class_="PostInfo")
            if location_section:
                location_links = location_section.find_all("a")
                possible_locations = [
                    link.get_text(strip=True)
                    for link in location_links
                    if "category" not in link.get('href', '')
                ]
                location = possible_locations[0] if possible_locations else "N/A"
    except Exception as e:
        logging.error(f"Error parsing job details from {site} ({url}): {e}")

    return description, publish_date, location, experience

def fetch_jobs_from_optioncarriere(keyword):
    """Enhanced optioncarriere job fetching with better error handling"""
    url = BASE_URLS["optioncarriere"].format(query=keyword)
    logging.info(f"Fetching jobs from optioncarriere for keyword: {keyword}")
    
    try:
        session = create_session()
        response = make_request(url, session)
        soup = BeautifulSoup(response.text, "html.parser")
        jobs = []

        job_listings = soup.find_all("article", class_="job")
        logging.info(f"Found {len(job_listings)} potential jobs for '{keyword}' on optioncarriere")

        for job in job_listings:
            try:
                title_tag = job.find("h2").find("a")
                if not title_tag:
                    continue
                
                title = title_tag.text.strip()
                link = f"https://www.optioncarriere.tn{title_tag['href']}"
                logging.info(f"Processing job: {title}")

                location_section = job.find("ul", class_="location")
                location = location_section.find("li").text.strip() if location_section else "N/A"

                publish_date_section = job.find("span", class_="badge")
                publish_date = datetime.now().date()
                if publish_date_section:
                    publish_date = parse_relative_date(publish_date_section.text.strip())

                desc_section = job.find("div", class_="desc")
                description = desc_section.text.strip() if desc_section else "N/A"

                jobs.append((title, link, publish_date, location, "N/A", description))
                logging.info(f"Successfully processed: {title} | Location: {location}")

            except Exception as e:
                logging.error(f"Error parsing job from optioncarriere: {str(e)}")
                continue

        return jobs

    except Exception as e:
        logging.error(f"Failed to fetch jobs from optioncarriere for '{keyword}': {str(e)}")
        return []

def fetch_jobs_from_tunisietravail(keyword):
    """Enhanced tunisietravail job fetching"""
    url = BASE_URLS["tunisietravail"].format(query=keyword)
    logging.info(f"Fetching jobs from tunisietravail for keyword: {keyword}")
    
    try:
        session = create_session()
        response = make_request(url, session)
        soup = BeautifulSoup(response.text, "html.parser")
        jobs = []

        job_listings = soup.find_all("div", class_="Post")
        logging.info(f"Found {len(job_listings)} potential jobs for '{keyword}' on tunisietravail")

        for job in job_listings:
            try:
                title_tag = job.find("a", class_="h1titleall")
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                link = title_tag["href"]
                logging.info(f"Processing job: {title}")

                date_section = job.find("p", class_="PostDateIndexRed")
                publish_date = datetime.now().date()
                if date_section:
                    month_tag = date_section.find("strong", class_="month")
                    if month_tag:
                        publish_date = parse_relative_date(month_tag.text.strip())

                location_section = job.find("p", class_="PostInfo")
                location = "N/A"
                if location_section:
                    location_links = [link for link in location_section.find_all("a") 
                                    if "category" not in link.get('href', '')]
                    if location_links:
                        location = location_links[0].text.strip()

                description, _, _, experience = fetch_job_details(link, "tunisietravail")
                jobs.append((title, link, publish_date.strftime("%Y-%m-%d"), location, experience, description))
                logging.info(f"Successfully processed: {title} | Location: {location}")

            except Exception as e:
                logging.error(f"Error parsing job from tunisietravail: {str(e)}")
                continue

        return jobs

    except Exception as e:
        logging.error(f"Failed to fetch jobs from tunisietravail for '{keyword}': {str(e)}")
        return []

def fetch_jobs_from_keejob(keyword):
    """Enhanced keejob job fetching"""
    url = BASE_URLS["keejob"].format(query=keyword)
    logging.info(f"Fetching jobs from keejob for keyword: {keyword}")
    
    try:
        session = create_session()
        response = make_request(url, session)
        soup = BeautifulSoup(response.text, "html.parser")
        jobs = []

        job_listings = soup.find_all("div", class_="block_white_a")
        logging.info(f"Found {len(job_listings)} potential jobs for '{keyword}' on keejob")

        for job in job_listings:
            try:
                title_tag = job.find("a", style=True)
                if not title_tag:
                    continue

                title = title_tag.text.strip()
                link = f"https://www.keejob.com{title_tag['href']}"
                logging.info(f"Processing job: {title}")

                location_tag = job.find("i", class_="fa-map-marker")
                location = location_tag.next_sibling.strip() if location_tag else "N/A"

                date_tag = job.find("i", class_="fa-clock-o")
                publish_date = datetime.now().date()
                if date_tag and date_tag.next_sibling:
                    publish_date = parse_relative_date(date_tag.next_sibling.strip())

                description, _, _, experience = fetch_job_details(link, "keejob")
                jobs.append((title, link, publish_date, location, experience, description))
                logging.info(f"Successfully processed: {title} | Location: {location}")

            except Exception as e:
                logging.error(f"Error parsing job from keejob: {str(e)}")
                continue

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
    """Enhanced job update process with detailed logging"""
    logging.info("Starting job update process...")
    all_keywords = set(
        KEYWORDS['core'] + 
        KEYWORDS['specializations'] + 
        KEYWORDS['job_titles'] + 
        KEYWORDS['domains']
    )

    total_jobs_added = 0
    total_jobs_processed = 0
    failed_keywords = []

    for keyword in all_keywords:
        try:
            logging.info(f"Processing keyword: {keyword}")
            jobs_optioncarriere = fetch_jobs_from_optioncarriere(keyword)
            jobs_tunisietravail = fetch_jobs_from_tunisietravail(keyword)
            jobs_keejob = fetch_jobs_from_keejob(keyword)

            all_jobs = jobs_optioncarriere + jobs_tunisietravail + jobs_keejob
            total_jobs_processed += len(all_jobs)
            
            filtered_jobs = filter_jobs(all_jobs)
            logging.info(f"Found {len(filtered_jobs)} relevant jobs for keyword '{keyword}'")

            for job in filtered_jobs:
                save_job_to_db(*job, "new")
                total_jobs_added += 1

        except Exception as e:
            logging.error(f"Error processing keyword {keyword}: {e}")
            failed_keywords.append(keyword)

    logging.info("Job update summary:")
    logging.info(f"Total jobs processed: {total_jobs_processed}")
    logging.info(f"Total jobs added: {total_jobs_added}")
    if failed_keywords:
        logging.warning(f"Failed keywords: {', '.join(failed_keywords)}")

if __name__ == "__main__":
    initialize_db()
    update_jobs_with_logging()


    while True:
        
        update_jobs()
        logging.info("Job list updated. Sleeping for 24 hours...")
        time.sleep(86400)