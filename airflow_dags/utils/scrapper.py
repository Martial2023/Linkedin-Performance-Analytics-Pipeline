from .scrapper_utils import *
from config import LINKEDIN_USER_NAME, LINKEDIN_PWD


target_total_posts = 100 #Nbre de postes maximum attendus
path = "data"
sub_path = "subtheme"

themes = [
    "IA",
    "DataScience",
    "Innovation",
    "finance",
    "projet",
    "Technology",
    "hackathon",
    "sport",
    "Leadership",
    "HumanResources",
    "DigitalTransformation",
    "tutoriel",
    "education"
]

def scrapping():
    try:
        driver = setup_driver()
        login_to_linkedin(driver, LINKEDIN_USER_NAME, LINKEDIN_PWD)
        df, filename = start_scrapping(driver, themes, target_total_posts, path, sub_path)
        
        followers_dict = {}
        for i, link in enumerate(df["author_link"].unique()):
            time.sleep(5)
            if pd.notna(link) and link != "N/A":
                followers = get_subscribers(driver, link)
                followers_dict[link] = followers

            else:
                followers_dict[link] = 0
            
            if i % 5 == 0:
                time.sleep(10)
            else:
                time.sleep(random.uniform(3, 6))
            
        # Ajouter la colonne followers
        df["followers"] = df["author_link"].map(followers_dict).fillna(0).astype(int)
        return df
    except Exception as e:
        logger.error(f"Erreur lors du scrapping des publications: {e}")