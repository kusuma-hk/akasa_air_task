import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

def get_engine():
    db_type = os.getenv("DB_TYPE", "sqlite")
    db_user = os.getenv("DB_USER", "")
    db_pass = os.getenv("DB_PASS", "")
    db_host = os.getenv("DB_HOST", "")
    db_port = os.getenv("DB_PORT", "")
    db_name = os.getenv("DB_NAME", "akasa_air.db")

    if db_type == "mysql":
        return create_engine(
            f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        )
    else:
        # fallback to SQLite (no credentials needed)
        return create_engine(f"sqlite:///{db_name}")
