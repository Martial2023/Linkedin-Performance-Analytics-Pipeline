import pandas as pd
from sqlalchemy import create_engine
from loguru import logger
from config import DATABASE_URL
import sys


logger.remove()
logger.add("logger/dataload", rotation="500kb", level="WARNING")
logger.add(sys.stderr, level="INFO")

TABLE_NAME = "linkedin_posts"
def load_in_postgre(df):
    for col in ['comments', 'shares', 'likes', 'followers']:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["text"] = df["text"].fillna("N/A")

    engine = create_engine(DATABASE_URL)
    try:
        logger.info("Connexion à la data base")
        engine = create_engine(DATABASE_URL)
        logger.info(f"Sauvegarde des info dans la table '{TABLE_NAME}'")
        df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
        logger.info("Sauvegarde terminé avec succèss")
    except Exception as e:
        logger.error(f"Erreur lors du sauvegarde des info dans la db: {e}")