from dotenv import load_dotenv
import os

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')

load_dotenv(dotenv_path)
LINKEDIN_USER_NAME = os.getenv('LINKEDIN_USER_NAME')
LINKEDIN_PWD = os.getenv('LINKEDIN_PWD')
DATABASE_URL = os.getenv('DATABASE_URL')