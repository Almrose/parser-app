from time import sleep
from resume_daily import ResumeParser
from config.celery_app import app
from pathlib import Path
from worker.token_checker import Token
import os

ROOT_DIR = Path(__file__).resolve(strict=True).parent.parent.parent
# test_project/
APPS_DIR = ROOT_DIR / "parser-app"


@app.task(name='Token Check')
def token_check():
    token = Token()
    updated = token.get_token()
    return updated


@app.task(name='Resume Cards Parsing')
def parse_last_resumes_cards():
    parser = ResumeParser()
    parsed = parser.get_last_resumes_cards()
    return parsed


@app.task(name='Unparsed Resume Parsing')
def parse_unparsed_resumes():
    parser = ResumeParser()
    parsed = parser.get_unparsed_resumes()
    return parsed


@app.task(name='Upload To Remote Table')
def parse_unparsed_resumes():
    parser = ResumeParser()
    parsed = parser.upload_to_main_db()
    return parsed


# @app.task
# def print_test():
#     with open("/app/config/token", "w") as f:
#         f.write("fdasfadsfads")
