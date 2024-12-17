import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime, timedelta
import time
import re
import logging
from datetime import datetime, timedelta

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

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

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
    url = BASE_URLS["optioncarriere"].format(query=keyword)
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    jobs = []

    for job in soup.find_all("article", class_="job"):
        try:
            title_tag = job.find("h2").find("a")
            if not title_tag:
                continue
            title = title_tag.text.strip()
            link = f"https://www.optioncarriere.tn{title_tag['href']}"

            location_section = job.find("ul", class_="location")
            location = "N/A"
            if location_section:
                location_candidates = [li.text.strip() for li in location_section.find_all("li")]
                location = next((loc for loc in location_candidates if loc), "N/A")

            publish_date_section = job.find("span", class_="badge")
            publish_date = "N/A"
            if publish_date_section:
                relative_time = publish_date_section.text.strip()
                publish_date = parse_relative_date(relative_time)

            desc_section = job.find("div", class_="desc")
            description = desc_section.text.strip() if desc_section else "N/A"

            jobs.append((title, link, publish_date, location, "N/A", description))
        except Exception as e:
            logging.error(f"Error parsing job card from optioncarriere: {e}")

    return jobs

def fetch_jobs_from_tunisietravail(keyword):
    url = BASE_URLS["tunisietravail"].format(query=keyword)
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    jobs = []
    one_month_ago = (datetime.now() - timedelta(days=30)).date()

    for job in soup.find_all("div", class_="Post"):
        try:
            date_section = job.find("p", class_="PostDateIndexRed")
            if date_section:
                month_tag = date_section.find("strong", class_="month")
                if month_tag:
                    publish_date = parse_relative_date(month_tag.text.strip())
                    
                    # Strict date filtering
                    if publish_date < one_month_ago:
                        continue

                    title_tag = job.find("a", class_="h1titleall")
                    if not title_tag:
                        continue
                    
                    title = title_tag.text.strip()
                    link = title_tag["href"]

                    location_section = job.find("p", class_="PostInfo")
                    location = "N/A"
                    if location_section:
                        location_links = location_section.find_all("a")
                        possible_locations = [
                            link.get_text(strip=True)
                            for link in location_links
                            if "category" not in link.get('href', '')
                        ]
                        location = possible_locations[0] if possible_locations else "N/A"

                    description, _, _, experience = fetch_job_details(link, "tunisietravail")
                    jobs.append((title, link, publish_date.strftime("%Y-%m-%d"), location, experience, description))
        except Exception as e:
            logging.error(f"Error parsing job from tunisietravail: {e}")

    return jobs

def fetch_jobs_from_keejob(keyword):
    url = BASE_URLS["keejob"].format(query=keyword)
    response = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(response.text, "html.parser")
    jobs = []

    for job in soup.find_all("div", class_="block_white_a"):
        try:
            title_tag = job.find("a", style=True)
            if not title_tag:
                continue
            title = title_tag.text.strip()
            link = f"https://www.keejob.com{title_tag['href']}"

            location_tag = job.find("i", class_="fa-map-marker")
            location = "N/A"
            if location_tag and location_tag.next_sibling:
                location = location_tag.next_sibling.strip()

            date_tag = job.find("i", class_="fa-clock-o")
            publish_date = "N/A"
            if date_tag and date_tag.next_sibling:
                publish_date = parse_relative_date(date_tag.next_sibling.strip())

            description, _, _, experience = fetch_job_details(link, "keejob")
            jobs.append((title, link, publish_date, location, experience, description))
        except Exception as e:
            logging.error(f"Error parsing job from keejob: {e}")

    return jobs

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
        INSERT OR IGNORE INTO jobs (
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
    """
    Enhanced job update process with more robust error handling
    """
    all_keywords = set(
        KEYWORDS['core'] + 
        KEYWORDS['specializations'] + 
        KEYWORDS['job_titles'] + 
        KEYWORDS['domains']
    )

    for keyword in all_keywords:
        try:
            logging.info(f"Processing keyword: {keyword}")
            jobs_optioncarriere = fetch_jobs_from_optioncarriere(keyword)
            jobs_tunisietravail = fetch_jobs_from_tunisietravail(keyword)
            jobs_keejob = fetch_jobs_from_keejob(keyword)

            all_jobs = jobs_optioncarriere + jobs_tunisietravail + jobs_keejob
            filtered_jobs = filter_jobs(all_jobs)

            for job in filtered_jobs:
                save_job_to_db(*job, "new")

        except Exception as e:
            logging.error(f"Error processing keyword {keyword}: {e}")

if __name__ == "__main__":
    initialize_db()
    update_jobs_with_logging()


    while True:
        
        update_jobs()
        logging.info("Job list updated. Sleeping for 24 hours...")
        time.sleep(86400)