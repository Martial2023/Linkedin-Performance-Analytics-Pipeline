import re
import polars as pl
from loguru import logger
import sys
import os

logger.remove()
logger.add("logger/compute_kpis", rotation="500kb", level="WARNING")
logger.add(sys.stderr, level="INFO")


kpi_folder = "Kpis"
os.makedirs(kpi_folder, exist_ok=True)

def categorize_time(hour):
    if 6 <= hour <= 8:
        return "Tôt le matin"
    elif 9 <= hour <= 11:
        return "Milieu de matinée"
    elif 12 <= hour <= 16:
        return "Après-midi"
    elif 17 <= hour <= 19:
        return "Fin de journée"
    else:
        return "Soir"

def encode_engagement(df):
    """
    Cette fonction permet de faire de l'encodage (Fort/Faible) du dataframe en se basant sur le quantile 0.9
    """
    engagement_threshold = df["engagement_total"].quantile(0.90)
    return df.with_columns(
        pl.when(
            pl.col("engagement_total") >= engagement_threshold
        ).then(pl.lit("Fort")
        ).otherwise(pl.lit("Faible")
        ).alias("engagement_category")
    )
    

def engagement_fort_faible_proportion(df_engagement):
    """
    Cette fonction permet d'examiner la porportion des posts Forts ou Faible (posts à fort engagement)
    """
    stats = df_engagement.group_by("engagement_category").agg(
        mean_text_length=pl.col("text_length").mean(),
        median_text_length=pl.col("text_length").median(),
        std_text_length=pl.col("text_length").std(),
        count=pl.col("text_length").count()
    )
    stats[["engagement_category", "mean_text_length"]].write_parquet(f"{kpi_folder}/engagement_category_proportion.parquet")


def proportion_viral_noviral_proportion(df):
    """
    Cette fonction permet d'examiner la porportion des posts viraux et non-viraux
    """
    viral_stats = df.group_by("is_viral").agg(
        mean_text_length=pl.col("text_length").mean(),
        median_text_length=pl.col("text_length").median(),
        std_text_length=pl.col("text_length").std(),
        count=pl.col("text_length").count(),
        mean_followers=pl.col("followers").mean()
    )
    viral_stats[["is_viral", "mean_text_length"]].write_parquet(f"{kpi_folder}/viral_categoy_proportion.parquet")


def hashtag_impact_compute(df):
    """
    Cette fonction permet de mesurer les l'impact du nombre de hashtags sur les l'engagement total et le nombre
    de partages des postes
    """
    hashtag_impact = df.group_by("nbr_hashtags").agg(
        mean_engagement_total=pl.col("engagement_total").mean(),
        median_engagement_total=pl.col("engagement_total").median(),
        mean_shares=pl.col("shares").mean(),
        median_shares=pl.col("shares").median()
    ).sort("nbr_hashtags")
    hashtag_impact.head(10).write_parquet(f"{kpi_folder}/top_hashtag_impact.parquet")


def top_hashtags(df_type, filename):
    """
    Cette fonction détermine les top 15 des hashtags les plus utilisés dans les postes à fort engagement
    ou viraux
    """
    hashtags_list = df_type["hashtags"].map_elements(
        lambda x: [tag.lower() for tag in re.findall(r'#\w+', x)] if x else [],
        return_dtype=pl.List(pl.Utf8)
    )
    hashtags_list = [element for sublist in hashtags_list.to_list() for element in (sublist if isinstance(sublist, list) else [sublist])]
    hashtags_count = pl.DataFrame({
        "hashtag": hashtags_list
    }).group_by("hashtag").len().sort("len", descending=True)
    hashtags_count.head(15).write_parquet(f"{kpi_folder}/{filename}.parquet")


def theme_posts_repartition(df_type, filename):
    """
    Cette fonction permet de trouver la répartition des thèmes dans les posts les plus engagés ou viraux
    """
    viral_post_theme = df_type.group_by("theme").agg(
        count=pl.col("theme").count()
    ).sort("count", descending=True)
    viral_post_theme.write_parquet(f"{kpi_folder}/{filename}.parquet")
    
 
def engagement_partages_per_day(df_type, type_propertie):
    """
    Cette fonction permet de retrouver les répartion des engagement et des partages dans les jours des la semaines et
    les moments de la journée
    """
    engagement_timing = df_type.group_by(["day_of_week", "time_of_day"]).agg(
        count=pl.col("id").count(),
        mean_engagement_total=pl.col("engagement_total").mean(),
        mean_shares=pl.col("shares").mean(),
        themes=pl.col("theme").unique().str.join(", "),
        hashtags=pl.col("hashtags").str.join(", ")
    ).drop_nulls()#.sort(["mean_engagement_total"], descending=True).drop_nulls()
    engagement_timing[["day_of_week", "time_of_day", "mean_engagement_total"]].sort("day_of_week").write_parquet(f"{kpi_folder}/{type_propertie}_timing.parquet")
    engagement_timing[["day_of_week", "time_of_day", "mean_shares"]].sort("day_of_week").write_parquet(f"{kpi_folder}/{type_propertie}_shares_timing.parquet")   


def compute_kpis(transformed_data="data/transformed_date.parquet"):
    """
    Compute various Key Performance Indicators (KPIs) for social media posts based on the provided dataset.
    Args:
        transformed_data (str): Path to the transformed data file in Parquet format. 
                                Defaults to "data/transformed_date.parquet".
    Workflow:
        1. Load the transformed data from the specified Parquet file.
        2. Encode engagement levels and filter posts with high engagement.
        3. Identify viral posts and compute the following KPIs:
            - Number of posts, high-engagement posts, and viral posts.
            - Statistics for text length by engagement category.
            - Statistics for text length for viral posts.
            - Impact of the number of hashtags on total engagement and shares.
            - Top hashtags used in viral and high-engagement posts.
            - Theme distribution in viral and high-engagement posts.
        4. Perform temporal analysis:
            - Categorize posts by time of day.
            - Analyze engagement and shares by day of the week and time of day.
    Outputs:
        - Saves various KPI results to Parquet files in the specified output folder.
        - Logs progress and any errors encountered during computation.
    Raises:
        Exception: Logs and raises any errors encountered during the computation process.
    """
    try:
        logger.info("Lecture des données temporairement sauvegardées à data/transformed_date.parquet")
        df = pl.read_parquet(transformed_data)
        
        # Déterminer et Encodage fort faible engagement
        logger.info("Déterminer et Encodage fort faible engagement")
        df_engagement = encode_engagement(df)
        # Filtrer et garder uniquement les posts à fort engagement "engagement_category"=="Fort"
        fort_engagement_df = df_engagement.filter(
            pl.col("engagement_category")=="Fort"
        )
        # Posts viraux
        viral_posts = df.filter(pl.col("is_viral") == True)
        
        # Nombre de postes à fort engagement et viraux
        pl.DataFrame({
            "nbre_postes": df.height,
            "nbre_fort": fort_engagement_df.height,
            "nbre_viraux": viral_posts.height
        }).write_parquet(f"{kpi_folder}/nbre_posts.parquet")

        # Statistiques pour text_length par catégorie
        logger.info("Statistiques pour text_length par catégorie")
        engagement_fort_faible_proportion(df_engagement)
        
        # Statistiques pour text_length pour les posts viraux
        logger.info("Statistiques pour text_length pour les posts viraux")
        proportion_viral_noviral_proportion(df)
        
        # Impact du nombre de hastags sur l'engagement total et le nombre de partages
        logger.info("Impact du nombre de hastags sur l'engagement total et le nombre de partages")
        hashtag_impact_compute(df)
        
        # Hashtag stratégiques: Les top hashtags les plus utilisés
        # Dans les postes les plus viraux
        logger.info("Hashtag stratégiques: Les top hashtags les plus utilisés dans les postes les plus viraux")
        top_hashtags(viral_posts, "top_viral_hashtags")
        # Posts engagés
        logger.info("Hashtag stratégiques: Les top hashtags les plus utilisés dans les postes les plus engagés")
        top_hashtags(fort_engagement_df, "top_engagement_hashtags")
        
        # Répartion des thèmes dans les postes viraux
        logger.info("Répartion des thèmes dans les postes viraux")
        theme_posts_repartition(viral_posts, "viral_post_theme_repartition")
        
        # Répartion des thèmes dans les postes à fort engagement
        logger.info("Répartion des thèmes dans les postes à fort engagement")
        theme_posts_repartition(fort_engagement_df, "engagement_post_theme")
        
        
        # Analyse temporelle
        logger.info("Analyse temporelle")
        viral_posts = viral_posts.with_columns(
            pl.col("hour").map_elements(categorize_time, return_dtype=pl.Utf8).alias("time_of_day")
        )

        df_engagement = fort_engagement_df.with_columns(
            pl.col("hour").map_elements(categorize_time, return_dtype=pl.Utf8).alias("time_of_day")
        )
        # trouver les répartion des engagement et des partages dans les jours des la semaines et
        # les moments de la journée
        logger.info("trouver les répartion des engagement et des partages dans les jours des la semaines et les moments de la journée")
        engagement_partages_per_day(viral_posts, "viral")
        engagement_partages_per_day(df_engagement, "engagement")
    except Exception as e:
        logger.error(f"Erreur lors du calcul des Kpi: {e}")