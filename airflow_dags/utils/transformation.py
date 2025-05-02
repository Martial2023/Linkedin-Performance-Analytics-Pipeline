import polars as pl
import re
from loguru import logger
import sys

#Initialisation du logger
logger.remove()
logger.add("logger/linkedin", rotation="500kb", level="WARNING")
logger.add(sys.stderr, level="INFO")



def clean_feature(df):
    try:
        df = df.with_columns(
            pl.when(pl.col("theme").is_in(["DataScience", "DataScienceInnovation"]))
            .then(pl.lit("IA"))
            .when(pl.col("theme").is_in(["Innovation", "Devoloppement"]))
            .then(pl.lit("Technology"))
            .otherwise(pl.col("theme"))
            .alias("theme")
        )

        # Considérons aléatoirement 220 posts IA et laissons les autres pour ne pas tirer les stats dans le sens de 'IA'
        logger.info("# Conserons aléatoirement 220 posts IA et laissons les autres pour ne pas tirer les stats dans le sens de 'IA'")
        df_ia = df.filter(pl.col("theme") == "IA")
        num_to_keep = min(220, df_ia.height)
        df_ia_sampled = df_ia.sample(n=num_to_keep, with_replacement=False)
        df_other = df.filter(pl.col("theme") != "IA")

        # 4. Fusionner les posts restants et ceux sélectionnés aléatoirement pour "IA"
        df = pl.concat([df_ia_sampled, df_other])


        df = df.filter(
            pl.col("theme") != "hackathonsport"
        )
        
        df = df.sort("id")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de 'clean_feature()': {e}")
    
    return df


def extract_hashtags(text):
    return " ".join(re.findall(r"#\w+", text))

def nbr_hashtags(text):
    return sum([1 for mot in " ".join(re.findall(r"#\w+", text)) if "#" in mot])

def feature_calculate(df):
    try:
        #a. Calculer l'engagement
        logger.info("a. Calculer l'engagement")
        df = df.with_columns(
            (pl.col("likes") + pl.col("comments") + pl.col("shares")).alias("engagement_total"),
        )
        
        #b. Longueur du texte
        logger.info("b. Longueur du texte")
        df = df.with_columns(
            pl.col("text").str.split(" ").list.len().alias("text_length")
        )
        
        #c. Analyse temporelle
        logger.info("c. Analyse temporelle")
        df = df.with_columns(
            pl.col("date").dt.weekday().alias("day_of_week"),
            pl.col("date").dt.hour().alias("hour")
        )
        
        #d. Sentiments annalysis
        
        #e. Hashtags
        logger.info("e. Hashtags")
        df = df.with_columns(
            pl.col("text").map_elements(extract_hashtags, return_dtype=pl.String).alias("hashtags"),
            pl.col("text").map_elements(nbr_hashtags, return_dtype=pl.Int64).alias("nbr_hashtags")
        )
    
        #f. Post viraux
        logger.info("f. Post viraux")
        viral_threshold = df["shares"].quantile(0.9)
        df = df.with_columns(
            (pl.col("shares") >= viral_threshold).alias("is_viral")
        )
        
        #g. Fort/Faible engagement
        logger.info("g. Fort/Faible engagement")
        engagement_threshold = df["engagement_total"].quantile(0.90)
        df = df.with_columns(
            pl.when(
                pl.col("engagement_total") >= engagement_threshold
            ).then(pl.lit("Fort")
            ).otherwise(pl.lit("Faible")
            ).alias("engagement_category")
        )
        
        df = df.sort("id")
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de 'feature_calculate()': {e}")
    
    return df



def transformation(df, transformation_output="data/transformed_date.parquet"):
    try:
        df = clean_feature(df)
        df = feature_calculate(df)
        df.write_parquet(transformation_output)
        logger.info("Datas transformées avec succès")
    except Exception as e:
        logger.error(f"Erreur durant la transformation des données: {e}")