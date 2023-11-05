from dotenv import load_dotenv
import os

load_dotenv('.env')

print(os.environ.get("PKB"))