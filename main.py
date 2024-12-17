from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
from datetime import datetime
import time
import threading
from typing import Optional
from job_scraper import initialize_db, update_jobs, KEYWORDS
import uvicorn
from fastapi.responses import JSONResponse

try:
    initialize_db()
    print("Database initialized successfully")
except Exception as e:
    print(f"Failed to initialize database: {e}")
    
import os  

DB_FILE = "jobs.db"  
initialize_db()

# Create the FastAPI app
def lifespan(app: FastAPI):
    print("Starting application...")  # Debug log
    initialize_db()
    print("Database initialized.")  # Debug log
    
    def start_job_updater():
        print("Starting job updater...")  # Debug log
        def update_loop():
            while True:
                update_jobs()
                print("Jobs updated. Sleeping for 24 hours...")  # Debug log
                time.sleep(86400)
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()

    start_job_updater()
    yield
    print("Shutting down application.")  


app = FastAPI(lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Job(BaseModel):
    title: str
    link: str
    publish_date: str
    location: str
    experience: str
    description: Optional[str]  
    status: str

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sqlite3
from datetime import datetime
import time
import threading
from typing import Optional
from job_scraper import initialize_db, update_jobs, KEYWORDS

# Initialize the database
initialize_db()
DB_FILE = "jobs.db"

# Create the FastAPI app
def lifespan(app: FastAPI):
    print("Starting application...")  # Debug log
    initialize_db()
    print("Database initialized.")  # Debug log
    
    def start_job_updater():
        print("Starting job updater...")  # Debug log
        def update_loop():
            while True:
                update_jobs()
                print("Jobs updated. Sleeping for 24 hours...")  # Debug log
                time.sleep(86400)
        thread = threading.Thread(target=update_loop, daemon=True)
        thread.start()

    start_job_updater()
    yield
    print("Shutting down application.")  # Debug log


app = FastAPI(lifespan=lifespan)

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
     allow_origins=[
        "http://localhost:3000", 
        "https://nchallababytel9a5edma.netlify.app"  # Add your actual Netlify site URL,
        "https://nchallahbabytel9a5edma.netlify.app"
    ],
    
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models for API Input/Output
class Job(BaseModel):
    title: str
    link: str
    publish_date: str
    location: str
    experience: str
    description: Optional[str]  # Allow None values
    status: str
    is_clicked: bool = False  # Add this line

@app.api_route("/", methods=["GET", "HEAD"])
def read_root():
    return JSONResponse({"message": "Welcome to the Job Scraper API!"})


@app.post("/mark-job-clicked")
def mark_job_clicked(job_data: dict):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        
        # Mark the job as clicked
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

# Modify get_jobs to include is_clicked in the response
@app.get("/jobs", response_model=list[Job])
def get_jobs(keyword: str = None):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()

        query = "SELECT title, link, publish_date, location, experience, description, status, is_clicked FROM jobs"
        params = ()
        if keyword:
            query += " WHERE title LIKE ? OR description LIKE ?"
            params = (f"%{keyword}%", f"%{keyword}%")
        
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
                "is_clicked": bool(row[7])  # Convert to boolean
            }
            for row in jobs
        ]
    except sqlite3.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")


# Endpoint: List supported keywords
@app.get("/keywords")
def get_keywords():
    """
    Return the list of keywords used for filtering jobs.
    """
    return {"keywords": KEYWORDS}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
