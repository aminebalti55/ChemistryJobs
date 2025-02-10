from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
import sqlite3
import time
import re
from typing import Dict, Optional
import threading
import os
import uvicorn

DB_FILE = "jobs.db"

class JobAutomator:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.driver = webdriver.Chrome()
        self.profile = {
            "nom": "Balti",
            "prenom": "Med Amine",
            "cin": "YOUR_CIN_HERE",  # Add your CIN
            "telephone1": "+21692358690",
            "email": "mohamedamine.balti@esprit.tn",
            "cv_path": r"C:\Users\JIMMY\Downloads\Resume\Amine balti resume.pdf",
            "experience": "3 Ans et Plus",
            "languages": ["Arabe", "Français", "Anglais"]
        }

    def get_unapplied_jobs(self):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute("""
            SELECT link FROM jobs 
            WHERE status = 'new' 
            AND link LIKE '%tunisietravail.net%'
            AND is_clicked = 0
        """)
        jobs = c.fetchall()
        conn.close()
        return [job[0] for job in jobs]

    def fill_education(self):
        self.driver.find_element(By.ID, "diplome_oui").click()
        time.sleep(1)
        diploma_select = Select(self.driver.find_element(By.ID, "u_diplome_detail"))
        diploma_select.select_by_value("Diplôme Ingénieur")  # Based on your education

    def fill_experience(self):
        self.driver.find_element(By.ID, "experience_oui").click()
        time.sleep(1)
        experience_select = Select(self.driver.find_element(By.ID, "duree_experience"))
        experience_select.select_by_value("3 Ans et Plus")  # Based on your experience

    def apply_to_job(self, job_url: str) -> bool:
        try:
            job_id = re.search(r'post_id=(\d+)', job_url).group(1)
            application_url = f"https://tunisietravail.net/candidate/?post_id={job_id}"
            self.driver.get(application_url)
            
            # Fill personal info
            self.driver.find_element(By.ID, "nom").send_keys(self.profile["nom"])
            self.driver.find_element(By.ID, "prenom").send_keys(self.profile["prenom"])
            self.driver.find_element(By.ID, "cin").send_keys(self.profile["cin"])
            self.driver.find_element(By.ID, "telephone1").send_keys(self.profile["telephone1"])
            self.driver.find_element(By.ID, "email").send_keys(self.profile["email"])

            # Location
            country_select = Select(self.driver.find_element(By.ID, "country_selector"))
            country_select.select_by_value("1")  # Tunisia
            time.sleep(2)
            region_select = Select(self.driver.find_element(By.ID, "region_selector"))
            region_select.select_by_visible_text("Ariana")

            # Education and Experience
            self.fill_education()
            self.fill_experience()

            # Languages
            for lang in self.profile["languages"]:
                checkbox = self.driver.find_element(
                    By.XPATH, f"//input[@type='checkbox'][@value='{lang}']"
                )
                if not checkbox.is_selected():
                    checkbox.click()

            # Upload CV
            file_input = self.driver.find_element(By.NAME, "fichier[]")
            file_input.send_keys(self.profile["cv_path"])

            # Accept terms
            self.driver.find_element(By.ID, "useforms").click()

            # Wait for CAPTCHA
            print(f"Please solve CAPTCHA for job {job_id}")
            WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.CLASS_NAME, "g-recaptcha-response"))
            )

            # Submit
            submit_button = self.driver.find_element(By.ID, "submitBtn")
            submit_button.click()
            time.sleep(5)

            return True

        except Exception as e:
            print(f"Error applying to job {job_url}: {str(e)}")
            return False

    def mark_job_applied(self, job_link: str, success: bool):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        status = 'applied' if success else 'failed'
        c.execute("""
            UPDATE jobs 
            SET status = ?, is_clicked = 1
            WHERE link = ?
        """, (status, job_link))
        conn.commit()
        conn.close()

    def run_automation(self):
        jobs = self.get_unapplied_jobs()
        print(f"Found {len(jobs)} unapplied jobs")

        for job_url in jobs:
            success = self.apply_to_job(job_url)
            self.mark_job_applied(job_url, success)
            time.sleep(2)

    def close(self):
        self.driver.quit()

# FastAPI endpoints
app = FastAPI()
automator = None
automation_thread = None

def run_automation_loop():
    global automator
    while True:
        try:
            automator.run_automation()
            time.sleep(3600)
        except Exception as e:
            print(f"Automation error: {str(e)}")
            time.sleep(300)

@app.post("/start-automation")
async def start_automation():
    global automator, automation_thread
    
    if automation_thread and automation_thread.is_alive():
        raise HTTPException(status_code=400, detail="Automation already running")
    
    try:
        automator = JobAutomator(DB_FILE)
        automation_thread = threading.Thread(
            target=run_automation_loop,
            daemon=True
        )
        automation_thread.start()
        return {"status": "Automation started successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/automation-status")
async def get_automation_status():
    global automation_thread
    return {
        "status": "running" if automation_thread and automation_thread.is_alive() else "stopped",
        "applied_count": get_applied_count()
    }

@app.post("/stop-automation")
async def stop_automation():
    global automator, automation_thread
    if automator:
        automator.close()
        automator = None
    automation_thread = None
    return {"status": "Automation stopped"}

def get_applied_count():
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM jobs WHERE status = 'applied'")
        count = c.fetchone()[0]
        conn.close()
        return count
    except sqlite3.Error:
        return 0

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)