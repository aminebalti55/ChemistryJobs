from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
from datetime import datetime
import time
import threading
from typing import Optional, List, Dict, Any, Tuple
from job_scraper import (
    initialize_db, update_jobs, KEYWORDS, 
    get_application_stats, get_applied_job_links,
    record_application_attempt
)
import uvicorn
from fastapi.responses import JSONResponse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import os
from twocaptcha import TwoCaptcha
import logging
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
import undetected_chromedriver as uc
import cloudscraper
import requests
# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s',
    handlers=[
        logging.FileHandler('job_automation.log'),
        logging.StreamHandler()
    ]
)

# Constants
DB_FILE = "jobs.db"
initialize_db()

# Pydantic Models
class Job(BaseModel):
    title: str
    link: str
    publish_date: str
    location: str
    experience: str
    description: Optional[str]
    status: str
    is_clicked: bool = False
    application_attempts: Optional[int]
    last_application_date: Optional[str]
    application_success: Optional[bool]

class ApplicationStats(BaseModel):
    total_applications: int
    successful_applications: int
    failed_applications: int
    total_attempts: int
    by_site: Dict[str, Dict[str, int]]

class AutomationStatus(BaseModel):
    status: str
    stats: Optional[Dict[str, Any]]
    applied_count: int
    running_since: Optional[str]

class ApplicationAttempt(BaseModel):
    job_id: int
    success: bool
    site_type: str
    error_message: Optional[str]

class JobAutomator:
    def __init__(self, db_path: str):
        # Use undetected-chromedriver instead of regular ChromeDriver
        # Initialize Chrome options
        options = uc.ChromeOptions()
        
        # Add arguments directly
        options.add_argument('--start-maximized')
        options.add_argument('--enable-automation')
        options.add_argument('--disable-blink-features=AutomationControlled')
        
        # Initialize the driver with the options
        self.driver = uc.Chrome(options=options)
        
                
        # Apply stealth settings
        stealth(self.driver,
            languages=["fr-FR", "fr"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        
        # Initialize cloudscraper for API requests
        self.scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'mobile': False
            }
        )
        
        # Rest of your initialization code
        self.db_path = db_path
        self.profile = {
            "nom": "Balti",
            "prenom": "Med Amine",
            "cin": "11410142",
            "telephone1": "+21692358690",
            "email": "mohamedamine.balti@esprit.tn",
            "password": "98625232",
            "cv_path": r"C:\Users\JIMMY\Downloads\Resume\Amine balti resume.pdf",
            "languages": ["Arabe", "Français", "Anglais"]
        }
        self.applied_jobs = get_applied_job_links()
        self.stats = {
            'optioncarriere': {'attempts': 0, 'successes': 0},
            'tanitjobs': {'attempts': 0, 'successes': 0},
            'tunisietravail': {'attempts': 0, 'successes': 0},
            'keejob': {'attempts': 0, 'successes': 0}
        }
        self.start_time = datetime.now()

    def get_unapplied_jobs(self) -> List[Tuple[int, str, str]]:
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        try:
            c.execute("""
                SELECT id, link, title 
                FROM jobs 
                WHERE status = 'new' 
                AND (link LIKE '%tunisietravail.net%' 
                    OR link LIKE '%keejob.com%'
                    OR link LIKE '%optioncarriere.tn%'
                    OR link LIKE '%tanitjobs.com%')
                AND (application_success IS NULL OR application_success = 0)
                AND (application_attempts < 3)  
                AND is_clicked = 0
                AND link NOT IN (
                    SELECT link FROM jobs 
                    WHERE status = 'applied' 
                    AND application_success = 1
                )
            """)
            jobs = c.fetchall()
            return jobs
        finally:
            conn.close()

    def get_site_type(self, url: str) -> str:
            if "tunisietravail.net" in url:
                return "tunisietravail"
            elif "keejob.com" in url:
                return "keejob"
            elif "optioncarriere.tn" in url:
                return "optioncarriere"
            elif "tanitjobs.com" in url:
                return "tanitjobs"
            return "unknown"
        
        
    def update_application_stats(self, site_type: str, success: bool):
        self.stats[site_type]['attempts'] += 1
        if success:
            self.stats[site_type]['successes'] += 1



    def apply_to_job(self, job_url: str) -> bool:
        try:
            if "tunisietravail.net" in job_url:
                return self.apply_to_tunisie_travail(job_url)
            elif "keejob.com" in job_url:
                return self.apply_to_keejob(job_url)
            elif "optioncarriere.tn" in job_url:
                return self.apply_to_option_carriere(job_url)
            elif "tanitjobs.com" in job_url:
                return self.apply_to_tanitjobs(job_url)
            else:
                print(f"Unsupported job site: {job_url}")
                return False
        except Exception as e:
            print(f"Error applying to job {job_url}: {str(e)}")
            return False

    def apply_to_option_carriere(self, job_url: str) -> bool:
        try:
            logging.info(f"Starting application process for OptionCarriere job: {job_url}")
            self.driver.get(job_url)
            time.sleep(3)

            # Look for the Postuler button
            apply_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR, 
                    "a.btn.btn-r.btn-primary.btn-apply"
                ))
            )
            application_url = apply_button.get_attribute('href')
            logging.info(f"Found application URL: {application_url}")
            
            # Get the application URL
            self.driver.get(application_url)
            time.sleep(2)

            # Check if we're on the alert setup page
            if "alert" in self.driver.current_url:
                try:
                    direct_access_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((
                            By.CSS_SELECTOR, 
                            "a.btn.btn-l.btn-r.btn-full.skip"
                        ))
                    )
                    direct_access_url = direct_access_button.get_attribute('href')
                    if direct_access_url:
                        # Clean and fix the URL
                        if direct_access_url.startswith('/'):
                            direct_access_url = f"https://www.optioncarriere.tn{direct_access_url}"
                        elif "https://www.optioncarriere.tnhttps" in direct_access_url:
                            direct_access_url = direct_access_url.replace(
                                "https://www.optioncarriere.tnhttps//",
                                "https://"
                            )
                        
                        logging.info(f"Redirecting to cleaned URL: {direct_access_url}")
                        self.driver.get(direct_access_url)
                        time.sleep(2)
                except Exception as e:
                    logging.error(f"Error handling alert page: {str(e)}")
                    return False

            # Check if we need to login to OptionCarriere
            try:
                email_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "email"))
                )
                email_input.send_keys(self.profile["email"])
                logging.info("Filled email field")
                
                continue_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        "button.btn.btn-primary.btn-r.btn-next.btn-full"
                    ))
                )
                continue_button.click()
                time.sleep(2)
                logging.info("Clicked continue after email")

                password_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.ID, "password"))
                )
                password_input.send_keys(self.profile["password"])
                logging.info("Filled password field")
                
                login_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        "button.btn.btn-primary.btn-r.btn-next.btn-full"
                    ))
                )
                login_button.click()
                time.sleep(3)
                logging.info("Completed login process")

            except Exception as login_error:
                logging.error(f"Login error: {str(login_error)}")
                return False

            # After login, look for the continue button and check for TanitJobs redirect
            try:
                continue_to_apply_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR, 
                        "a.btn.btn-primary.btn-r.btn-next.btn-full"
                    ))
                )
                
                next_url = continue_to_apply_button.get_attribute('href')
                logging.info(f"Found next URL: {next_url}")
                
                if "tanitjobs.com" in next_url:
                    logging.info("Detected TanitJobs redirect, handling transition...")
                    try:
                        # Store cookies before redirect
                        option_carriere_cookies = self.driver.get_cookies()
                        
                        # Navigate to TanitJobs
                        return self.apply_to_tanitjobs(next_url, from_option_carriere=True)
                    except Exception as redirect_error:
                        logging.error(f"Error during TanitJobs redirect: {str(redirect_error)}")
                        return False
                
                return True

            except Exception as navigation_error:
                logging.error(f"Navigation error: {str(navigation_error)}")
                return False

        except Exception as e:
            logging.error(f"Error applying to OptionCarriere job {job_url}: {str(e)}")
        return False

    def apply_to_tanitjobs(self, job_url: str, from_option_carriere: bool = False) -> bool:
        try:
            logging.info(f"Starting TanitJobs application process for URL: {job_url}")
            
            # Navigate to the job URL
            self.driver.get(job_url)
            time.sleep(5)  # Wait for Cloudflare check
            
            # Handle Cloudflare if needed
            if "checking your browser" in self.driver.page_source.lower():
                logging.info("Detected Cloudflare check, waiting for bypass...")
                time.sleep(10)
            
            # Verify we're on the correct page
            if not any(keyword in self.driver.current_url for keyword in ["tanitjobs.com/job/", "tanitjobs.com/apply-now"]):
                logging.error("Failed to reach TanitJobs page")
                return False
                
            # Look for and click the apply button
            try:
                apply_button = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((
                        By.CSS_SELECTOR,
                        "button.btn.btn-apply.btn-primary.btn-lg.btn-block"
                    ))
                )
                apply_button.click()
                logging.info("Clicked apply button")
                time.sleep(2)
            except Exception as apply_error:
                logging.error(f"Error clicking apply button: {str(apply_error)}")
                return False

            # Handle login if needed
            if "login-form" in self.driver.page_source:
                logging.info("Login form detected, proceeding with login...")
                try:
                    # Fill login form
                    email_input = WebDriverWait(self.driver, 10).until(
                        EC.presence_of_element_located((By.NAME, "username"))
                    )
                    email_input.clear()
                    email_input.send_keys(self.profile["email"])
                    
                    password_input = self.driver.find_element(By.NAME, "password")
                    password_input.clear()
                    password_input.send_keys(self.profile["password"])
                    
                    login_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.ID, "bouton-con"))
                    )
                    login_button.click()
                    time.sleep(3)
                    logging.info("Completed login process")

                    # Click apply button again after login
                    apply_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((
                            By.CSS_SELECTOR,
                            "button.btn.btn-apply.btn-primary.btn-lg.btn-block"
                        ))
                    )
                    apply_button.click()
                    time.sleep(2)
                    logging.info("Clicked apply button after login")
                    
                except Exception as login_error:
                    logging.error(f"Login error: {str(login_error)}")
                    return False

            # Handle the application modal
            try:
                logging.info("Waiting for application modal...")
                WebDriverWait(self.driver, 10).until(
                    EC.visibility_of_element_located((By.ID, "apply-modal"))
                )

                # Try to select existing resume first
                try:
                    resume_select = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.NAME, "id_resume"))
                    )
                    select = Select(resume_select)
                    select.select_by_value("1110190")
                    logging.info("Selected existing resume")
                except Exception as resume_error:
                    logging.warning(f"Could not select existing resume: {str(resume_error)}")
                    # Try file upload instead
                    try:
                        file_input = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.NAME, "file_tmp"))
                        )
                        file_input.send_keys(self.profile["cv_path"])
                        logging.info("Uploaded resume file")
                    except Exception as upload_error:
                        logging.error(f"Failed to upload resume: {str(upload_error)}")
                        return False

                # Fill other required fields if empty
                try:
                    name_input = self.driver.find_element(By.NAME, "name")
                    if not name_input.get_attribute('value'):
                        name_input.send_keys(f"{self.profile['prenom']} {self.profile['nom']}")

                    email_input = self.driver.find_element(By.NAME, "email")
                    if not email_input.get_attribute('value'):
                        email_input.send_keys(self.profile["email"])
                    
                    logging.info("Filled required fields")
                except Exception as field_error:
                    logging.error(f"Error filling fields: {str(field_error)}")

                # Submit application
                try:
                    submit_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((
                            By.CSS_SELECTOR,
                            "input.btn__submit-modal.btn.btn__orange.btn__bold"
                        ))
                    )
                    submit_button.click()
                    logging.info("Clicked submit button")
                    time.sleep(5)
                except Exception as submit_error:
                    logging.error(f"Error submitting application: {str(submit_error)}")
                    return False

                # Check for success
                success_indicators = [
                    "succès", "success", "merci", "candidature envoyée",
                    "votre candidature a été envoyée"
                ]
                page_source = self.driver.page_source.lower()
                success = any(indicator in page_source for indicator in success_indicators)
                
                if success:
                    logging.info(f"Successfully applied to job at {job_url}")
                else:
                    logging.warning(f"No success message found after applying to {job_url}")
                
                return success

            except Exception as modal_error:
                logging.error(f"Error in application modal: {str(modal_error)}")
                return False

        except Exception as e:
            logging.error(f"Error applying to TanitJobs position {job_url}: {str(e)}")
        return False

    def get_cloudflare_tokens(self):
        """Get Cloudflare tokens using cloudscraper"""
        try:
            tokens = {}
            scraper = cloudscraper.create_scraper()
            response = scraper.get('https://www.tanitjobs.com')
            cookies = scraper.cookies.get_dict()
            tokens.update(cookies)
            return tokens
        except Exception as e:
            logging.error(f"Error getting Cloudflare tokens: {str(e)}")
            return None
    
    def apply_to_tunisie_travail(self, job_url: str) -> bool:
        try:
        # Visit job listing page
            self.driver.get(job_url)
            time.sleep(3)

            # Find and click the application button
            apply_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//div[contains(@style, 'background: #CC0000')]//a[contains(@href, 'candidate/?post_id=')]"
                ))
            )
            application_url = apply_button.get_attribute('href')
            self.driver.get(application_url)
            time.sleep(3)

            # Fill personal info with explicit waits
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "nom"))
            ).send_keys(self.profile["nom"])
            
            self.driver.find_element(By.ID, "prenom").send_keys(self.profile["prenom"])
            self.driver.find_element(By.ID, "cin").send_keys(self.profile["cin"])
            self.driver.find_element(By.ID, "telephone1").send_keys(self.profile["telephone1"])
            self.driver.find_element(By.ID, "email").send_keys(self.profile["email"])

            # Location - Tunisia and Ariana
            country_select = Select(self.driver.find_element(By.ID, "country_selector"))
            country_select.select_by_value("1")  # Value for Tunisia
            time.sleep(2)  # Wait for region dropdown to populate
            
            region_select = Select(self.driver.find_element(By.ID, "region_selector"))
            region_select.select_by_visible_text("Ariana")

            # Select "Diplome: Oui" and choose "Diplome Ingenieur"
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "diplome_oui"))
            ).click()
            time.sleep(1)
            
            diploma_select = Select(WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "u_diplome_detail"))
            ))
            diploma_select.select_by_value("Diplôme Ingénieur")

            # Experience: Select "Non"
            WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "experience_non"))
            ).click()

            # Languages: Select Arabic, French, and English
            languages = ["Arabe", "Français", "Anglais"]
            for lang in languages:
                checkbox = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH, 
                        f"//input[@type='checkbox'][@name='langues'][@value='{lang}']"
                    ))
                )
                if not checkbox.is_selected():
                    checkbox.click()

            # Upload CV
            file_input = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.NAME, "fichier[]"))
            )
            file_input.send_keys(self.profile["cv_path"])

            # Accept terms
            terms_checkbox = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "useforms"))
            )
            terms_checkbox.click()

            # Handle CAPTCHA
            print(f"Waiting for CAPTCHA interaction on {job_url}")
            
            # Wait for reCAPTCHA iframe and switch to it
            recaptcha_iframe = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR, 
                    "iframe[title='reCAPTCHA']"
                ))
            )
            self.driver.switch_to.frame(recaptcha_iframe)
            
            # Wait for and click the CAPTCHA checkbox
            captcha_checkbox = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR, 
                    ".recaptcha-checkbox-border"
                ))
            )
            captcha_checkbox.click()
            
            # Switch back to main content
            self.driver.switch_to.default_content()
            
            # Wait for CAPTCHA validation (g-recaptcha-response should be populated)
            WebDriverWait(self.driver, 120).until(
                lambda driver: driver.execute_script(
                    "return document.getElementsByName('g-recaptcha-response')[0].value"
                )
            )

            # Submit form
            submit_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.ID, "submitBtn"))
            )
            submit_button.click()

            # Wait for success/error message
            time.sleep(5)
            
            # Check for success indicators in the response
            success_indicators = ["succès", "success", "merci", "reçu"]
            error_indicators = ["erreur", "error", "invalide", "échec"]
            
            page_source = self.driver.page_source.lower()
            
            if any(indicator in page_source for indicator in success_indicators):
                print(f"Successfully applied to {job_url}")
                return True
            elif any(indicator in page_source for indicator in error_indicators):
                print(f"Failed to apply to {job_url} - Form submission error")
                return False
            else:
                print(f"Unclear submission status for {job_url}")
                return False

        except Exception as e:
            print(f"Error applying to TunisieTravail job {job_url}: {str(e)}")
            return False

    def verify_captcha_completion(self) -> bool:
    
        try:
            response = self.driver.execute_script(
                "return document.getElementsByName('g-recaptcha-response')[0].value"
            )
            return bool(response)
        except Exception as e:
            print(f"Error verifying CAPTCHA completion: {str(e)}")
            return False


    def apply_to_keejob(self, job_url: str) -> bool:
        try:
            # Navigate to job listing
            self.driver.get(job_url)
            time.sleep(3)

            # Find and click the application button
            postuler_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input#postuler_button.btn[value='Postuler']"))
            )
            postuler_button.click()
            time.sleep(2)

            # Check if we need to login
            if "login" in self.driver.current_url or "connexion" in self.driver.current_url:
                print("Logging into Keejob...")
                
                # Fill in login credentials
                username_input = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input#username.span11"))
                )
                username_input.clear()
                username_input.send_keys(self.profile["email"])
                
                password_input = self.driver.find_element(By.CSS_SELECTOR, "input#password.span11")
                password_input.clear()
                password_input.send_keys(self.profile["password"])
                
                # Click login button
                login_button = self.driver.find_element(By.CSS_SELECTOR, "button.btn")
                login_button.click()
                time.sleep(3)

                # After login, we might need to navigate back to job URL and click postuler again
                if "postuler" not in self.driver.current_url:
                    self.driver.get(job_url)
                    postuler_button = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "input#postuler_button.btn[value='Postuler']"))
                    )
                    postuler_button.click()
                    time.sleep(2)

            # Now we should be on the application form
            
            # Select Tunisia as country (value 184)
            country_select = Select(WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "select.span8[ng-model='data.country']"))
            ))
            country_select.select_by_value("184")  # Tunisia
            time.sleep(1)

            # Select Ariana as region (value 0)
            region_select = Select(self.driver.find_element(By.CSS_SELECTOR, "select.span8[ng-model='data.region']"))
            region_select.select_by_value("0")  # Ariana
            time.sleep(1)

            # Verify Tunisia is selected for phone country code
            phone_country_select = Select(self.driver.find_element(By.CSS_SELECTOR, "select[ng-model='phoneNumbers[0].country']"))
            if phone_country_select.first_selected_option.get_attribute("value") != "184":
                phone_country_select.select_by_value("184")  # Tunisia

            # Find and select the Aminebaltiresume checkbox
            resume_checkbox = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//input[@type='checkbox'][@name='attached_docs'][@value='1154523']"))
            )
            if not resume_checkbox.is_selected():
                resume_checkbox.click()

            # Click the final submit button
            submit_button = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "input#apply_button.btn.span12[value='Valider']"))
            )
            submit_button.click()
            time.sleep(5)

            # Verify submission by looking for success indicators
            success_text = ["succès", "success", "Merci", "candidature"]
            page_text = self.driver.page_source.lower()
            return any(text in page_text for text in success_text)
    
        except Exception as e:
            print(f"Error applying to Keejob position {job_url}: {str(e)}")
            return False

    def mark_job_applied(self, job_id: int, job_link: str, success: bool, site_type: str, error_message: str = None):
        record_application_attempt(job_id, success, site_type, error_message)
        if success:
            self.applied_jobs.add(job_link)
        self.update_application_stats(site_type, success)

    def run_automation(self):
        jobs = self.get_unapplied_jobs()
        total_jobs = len(jobs)
        logging.info(f"Found {total_jobs} unapplied jobs")
        
        successful_applications = 0
        failed_applications = 0
        
        for job_id, job_url, job_title in jobs:
            try:
                if job_url in self.applied_jobs:
                    logging.info(f"Skipping already applied job: {job_title}")
                    continue
                    
                logging.info(f"Attempting to apply to: {job_title}")
                site_type = self.get_site_type(job_url)
                success = self.apply_to_job(job_url)
                
                if success:
                    successful_applications += 1
                    logging.info(f"Successfully applied to: {job_title}")
                else:
                    failed_applications += 1
                    logging.warning(f"Failed to apply to: {job_title}")
                
                self.mark_job_applied(job_id, job_url, success, site_type)
                time.sleep(2)
                
            except Exception as e:
                error_msg = str(e)
                failed_applications += 1
                logging.error(f"Error processing job {job_title}: {error_msg}")
                self.mark_job_applied(job_id, job_url, False, self.get_site_type(job_url), error_msg)
                continue

        # Log summary
        logging.info("\nAutomation Run Summary:")
        logging.info(f"Total jobs processed: {total_jobs}")
        logging.info(f"Successful applications: {successful_applications}")
        logging.info(f"Failed applications: {failed_applications}")
        
        # Log stats by site
        for site, stats in self.stats.items():
            logging.info(f"{site} stats - Attempts: {stats['attempts']}, Successes: {stats['successes']}")

    def get_run_time(self) -> str:
        return str(datetime.now() - self.start_time)

    def get_stats(self) -> Dict[str, Any]:
        stats = get_application_stats()
        stats['site_stats'] = self.stats
        stats['running_since'] = self.start_time.strftime("%Y-%m-%d %H:%M:%S")
        return stats

    def close(self):
        try:
            self.driver.quit()
        except Exception as e:
            logging.error(f"Error closing WebDriver: {str(e)}")

# FastAPI App Configuration
# Global variables
automator = None
automation_thread = None
automation_start_time = None

def get_db_connection():
    try:
        return sqlite3.connect(DB_FILE)
    except sqlite3.Error as e:
        logging.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail="Database connection failed")



def lifespan(app: FastAPI):
    print("Starting application...")
    initialize_db()
    print("Database initialized.")
    
    def start_job_updater():
        print("Starting job updater...")
        def update_loop():
            while True:
                update_jobs()
                print("Jobs updated. Sleeping for 24 hours...")
                time.sleep(86400)
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()

    start_job_updater()
    yield
    
    global automator
    if automator:
        automator.close()
    print("Shutting down application.")

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup_event():
    initialize_db()
    logging.info("Application started and database initialized")
    
@app.on_event("shutdown")
async def shutdown_event():
    global automator
    if automator:
        automator.close()
    logging.info("Application shutting down")
    
    
@app.api_route("/", methods=["GET", "HEAD"])
def read_root():
    return JSONResponse({"message": "Welcome to the Job Scraper API!"})

@app.post("/mark-job-clicked")
def mark_job_clicked(job_data: dict):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
        UPDATE jobs 
        SET is_clicked = 1 
        WHERE link = ?
        ''', (job_data['link'],))
        conn.commit()
        conn.close()
        return {"status": "success"}
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/jobs", response_model=List[Job])
def get_jobs(keyword: str = None, status: str = None):
    try:
        conn = get_db_connection()
        c = conn.cursor()

        query = """
            SELECT title, link, publish_date, location, experience, 
                   description, status, is_clicked, application_attempts,
                   last_application_date, application_success
            FROM jobs
            WHERE 1=1
        """
        params = []

        if keyword:
            query += " AND (title LIKE ? OR description LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])

        if status:
            query += " AND status = ?"
            params.append(status)

        c.execute(query, params)
        jobs = c.fetchall()
        conn.close()

        return [
            {
                "title": row[0],
                "link": row[1],
                "publish_date": row[2],
                "location": row[3],
                "experience": row[4],
                "description": row[5],
                "status": row[6],
                "is_clicked": bool(row[7]),
                "application_attempts": row[8],
                "last_application_date": row[9],
                "application_success": bool(row[10]) if row[10] is not None else None
            }
            for row in jobs
        ]
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/keywords")
def get_keywords():
    return {"keywords": KEYWORDS}


@app.get("/application-stats", response_model=ApplicationStats)
async def get_application_stats_endpoint():
    stats = get_application_stats()
    if not stats:
        raise HTTPException(status_code=500, detail="Failed to retrieve application statistics")
    return stats

@app.get("/update-jobs")
def trigger_job_update():
    try:
        result = update_jobs()
        return {
            "status": "success",
            "message": "Jobs updated successfully",
            "new_jobs_added": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Job update failed: {str(e)}")

def run_automation_loop():
    global automator
    while True:
        try:
            automator.run_automation()
            time.sleep(3600)  # Wait for 1 hour between runs
        except Exception as e:
            print(f"Automation error: {str(e)}")
            time.sleep(300)  # Wait 5 minutes on error

@app.post("/start-automation")
async def start_automation():
    global automator, automation_thread, automation_start_time
    
    if automation_thread and automation_thread.is_alive():
        raise HTTPException(status_code=400, detail="Automation already running")
    
    try:
        automator = JobAutomator(DB_FILE)
        automation_start_time = datetime.now()
        automation_thread = threading.Thread(
            target=run_automation_loop,
            daemon=True
        )
        automation_thread.start()
        logging.info("Automation started successfully")
        return {
            "status": "success",
            "message": "Automation started successfully",
            "start_time": automation_start_time.strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        logging.error(f"Failed to start automation: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/automation-status", response_model=AutomationStatus)
async def get_automation_status():
    global automator, automation_thread, automation_start_time
    stats = None
    if automator:
        stats = automator.get_stats()
    return {
        "status": "running" if automation_thread and automation_thread.is_alive() else "stopped",
        "stats": stats,
        "applied_count": get_applied_count(),
        "running_since": automation_start_time.strftime("%Y-%m-%d %H:%M:%S") if automation_start_time else None
    }

@app.post("/stop-automation")
async def stop_automation():
    global automator, automation_thread, automation_start_time
    
    if automator:
        automator.close()
        automator = None
    automation_thread = None
    automation_start_time = None
    
    logging.info("Automation stopped")
    return {"status": "success", "message": "Automation stopped"}

def get_applied_count():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute("""
        SELECT 
        COUNT(*) as total_applications
        FROM jobs 
        WHERE status = 'applied'
        """)
        row = c.fetchone()
        conn.close()
        return row[0] if row else 0
    except sqlite3.Error:
        return 0
    
def run_automation_loop():
    global automator
    while True:
        try:
            automator.run_automation()
            stats = automator.get_stats()
            logging.info(f"Automation cycle completed. Current stats: {stats}")
            
            # Sleep longer if we've applied to many jobs
            if stats and stats['overall']['total_applications'] > 0:
                sleep_time = 3600 * 4  # 4 hours if we've applied to jobs
            else:
                sleep_time = 3600  # 1 hour if no applications
                
            time.sleep(sleep_time)
            
        except Exception as e:
            logging.error(f"Automation error: {str(e)}")
            time.sleep(300)  # 5 minutes on error


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)