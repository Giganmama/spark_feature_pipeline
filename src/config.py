import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "feature_store"),
    "user": os.getenv("DB_USER", "sber_de"),
    "password": os.getenv("DB_PASSWORD", "secure_password")
}

JDBC_URL = f"jdbc:postgresql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
JDBC_PROPS = {
    "user": DB_CONFIG["user"],
    "password": DB_CONFIG["password"],
    "driver": "org.postgresql.Driver"
}
