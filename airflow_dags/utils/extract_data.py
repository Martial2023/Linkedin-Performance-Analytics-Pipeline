import polars as pl
from sqlalchemy import create_engine
from loguru import logger
import sys
from config import DATABASE_URL
import os



logger.remove()
logger.add("logger/extrac_data", rotation="500kb", level="WARNING")
logger.add(sys.stderr, level="INFO")
TABLE_NAME = 'linkedin_posts'




def extraction(filename="data/linkedin_posts.parquet"):
    try:
        logger.info("Connection à la base de données distant")
        engine = create_engine(DATABASE_URL)
        logger.info(f"Extraction des données depuis la table: {TABLE_NAME}")
        query = f"SELECT * FROM {TABLE_NAME}"
        df = pl.read_database(query=query, connection=engine)
        df.write_parquet(filename)
        logger.info(f"Extraction éffecutée avec sucssès. Données sauvegardées à '{filename}'")
    except Exception as e:
        logger.error(f"Erreur durant l'extraction des datas: {e}")