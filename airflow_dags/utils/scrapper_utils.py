import sys
import os
import re
from loguru import logger
import time
import random
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from datetime import datetime, timedelta


#Initialisation du logger
logger.remove()
logger.add("logger/linkedin", rotation="500kb", level="WARNING")
logger.add(sys.stderr, level="INFO")


def setup_driver():
    logger.info("Configuration du driver Selenium")
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )
    chrome_options.add_argument("--headless") # Décommenter pour afficher la fenêtre du navigaateur (chrome)
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=chrome_options
    )
    return driver



def login_to_linkedin(driver, username, password):
    logger.info("Tentative de connexion à LinkedIn")
    try:
        driver.get("https://www.linkedin.com/login")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "username"))
        )
        time.sleep(random.uniform(2, 5))

        email_field = driver.find_element(By.ID, "username")
        email_field.send_keys(username)
        logger.info("Email saisi")

        password_field = driver.find_element(By.ID, "password")
        password_field.send_keys(password)
        logger.info("Mot de passe saisi")

        submit_button = driver.find_element(By.XPATH, "//button[@type='submit']")
        submit_button.click()
        logger.info("Bouton de connexion cliqué")

        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.ID, "global-nav"))
        )
        logger.info("Connexion réussie")
        time.sleep(random.uniform(3, 6))
    except TimeoutException:
        logger.error("Timeout lors de la connexion à LinkedIn. Possible CAPTCHA ou erreur réseau.")
        raise
    except Exception as e:
        logger.error(f"Erreur lors de la connexion : {str(e)}")
        raise
    
    
def parse_relative_date(relative_date, scrape_time):
    """Convertir une date relative en date estimée."""
    if "N/A" in relative_date:
        return "N/A"
    try:
        if "min" in relative_date.lower():
            minutes = int(relative_date.split()[0])
            return (scrape_time - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")
        if "h" in relative_date.lower():
            hours = int(relative_date.split()[0])
            return (scrape_time - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
        elif "d" in relative_date.lower():
            days = int(relative_date.split()[0])
            return (scrape_time - timedelta(days=days)).strftime("%Y-%m-%d 12:00:00")
        elif "w" in relative_date.lower():
            weeks = int(relative_date.split()[0])
            return (scrape_time - timedelta(weeks=weeks)).strftime("%Y-%m-%d 12:00:00")
        elif "mois" in relative_date.lower():
            mois = int(relative_date.split()[0])
            return (scrape_time - timedelta(days=30 * mois)).strftime("%Y-%m-%d 12:00:00")
        else:
            return relative_date
    except Exception as e:
        logger.warning(f"Erreur lors du parsing de la date {relative_date} : {str(e)}")
        return relative_date
    

def scrape_posts(driver, search_url, theme="feed", max_scrolls=5, posts_per_theme=100):
    logger.info(f"Scraping des posts pour l'URL : {search_url}")
    post_data = []
    scrape_time = datetime.now()
    try:
        driver.get(search_url)
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "feed-shared-update-v2"))
        )
        time.sleep(random.uniform(2, 5))

        # Faire défiler pour charger plus de posts
        for _ in range(max_scrolls):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            try:
                more_button = driver.find_element(By.CLASS_NAME, 'scaffold-finite-scroll__load-button')
                more_button.click()
            except:
                pass
            time.sleep(random.uniform(3, 6))
            

        # Parser la page pour collecter les URLs
        soup = BeautifulSoup(driver.page_source, "html.parser")
        posts = soup.find_all("div", class_="feed-shared-update-v2")
        
        # Scraper chaque post individuel
        for i, post in enumerate(posts[:posts_per_theme]):
            try:
                # Extraire les données
                description_elem = post.find("span", class_="break-words")
                description = description_elem.text.strip() if description_elem else "N/A"

                # Extraire la date
                date_elem = post.find("span", class_="update-components-actor__sub-description")
                date_text = date_elem.text.strip() if date_elem else "N/A"
                date_text = date_text.split('•')[0]
                date = parse_relative_date(date_text, scrape_time)

                likes_elem = post.find("span", class_="social-details-social-counts__reactions-count")
                likes = likes_elem.text.strip() if likes_elem else "0"
                
                comments = "0"
                shares = "0"
                shares_comments_elems = post.select("button.social-details-social-counts__btn > span")
                for elem in shares_comments_elems:
                    text = elem.text.strip().lower()
                    if "comment" in text:
                        comments = text.split()[0].strip() or "0"
                    elif "republication" in text or "share" in text:
                        shares = text.split()[0].strip() or "0"
                
                author_elem = post.find("a", class_="update-components-actor__meta-link")
                if author_elem:
                    author_link = author_elem["href"]
                    sub_span = author_elem.find("span", dir="ltr")
                    if sub_span:
                        name_span = sub_span.find("span", {"aria-hidden": "true"})
                        author = name_span.text.strip() if name_span else sub_span.text.strip()
                    else:
                        author = author_elem.text.strip()
                else:
                    author = "N/A"
                logger.debug(f"Auteur extrait : {author}: {author_link}")

                post_data.append({
                    "author": author,
                    "author_link": author_link,
                    "text": description,
                    "date": date,
                    "likes": likes,
                    "comments": comments,
                    "shares": shares,
                    "theme": theme,
                })
                logger.info(f"Post scrapé : - Auteur : {author}")
                time.sleep(random.uniform(1, 2))
            except Exception as e:
                logger.warning(f"Erreur lors du scraping du post : {str(e)}")
                continue

        return post_data
    except TimeoutException:
        logger.error("Timeout lors du chargement des posts.")
        return post_data
    except Exception as e:
        logger.error(f"Erreur lors du scraping des posts : {str(e)}")
        return post_data


def start_scrapping(driver, themes, target_total_posts, path, sub_path):
    posts_per_theme = target_total_posts // len(themes)
    all_posts = []
    os.makedirs(path, exist_ok=True)
    os.makedirs(sub_path, exist_ok=True)

    try:
        for theme in themes:
            logger.info(f"Début du scraping pour la thématique : {theme}")
            search_url = f"https://www.linkedin.com/search/results/content/?keywords={theme.replace(' ', '%20')}"
            posts = scrape_posts(driver, search_url, theme=theme, max_scrolls=5, posts_per_theme=posts_per_theme)
            sub_df = pd.DataFrame(posts)
            sub_df.to_csv(f"{sub_path}/{theme}_{datetime.now().strftime("%H_%M_%d_%m_%Y")}.csv")
            all_posts.extend(posts)
            logger.info(f"Posts scrapés pour {theme} : {len(posts)}")
            time.sleep(random.uniform(5, 10))

        if all_posts:
            df = pd.DataFrame(all_posts)
            filename = f"{path}/linkedin_posts_{datetime.now().strftime("%H_%M_%d_%m_%Y")}.csv"
            df.to_csv(filename, index=False, encoding="utf-8")
            logger.info(f"Données sauvegardées dans {filename} ({len(all_posts)} posts)")
        else:
            logger.warning("Aucune donnée scrapée")
            
        return df, filename

    except Exception as e:
        logger.error(f"Erreur dans le programme principal : {str(e)}")


def get_subscribers(driver, link):
    """
        Extrait le nombre d'abonnés d'un profil LinkedIn à partir de son URL.
        
        Args:
            driver: Instance du WebDriver Selenium.
            link (str): URL du profil LinkedIn (author_link).
            
        Returns:
            int: Nombre d'abonnés (ex. : 9517) ou 0 si échec.
    """
    try:
        driver.get(link)
        
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CLASS_NAME, "pvs-entity__caption-wrapper"))
        )
        time.sleep(random.uniform(2, 4))
        
        soup = BeautifulSoup(driver.page_source, "html.parser")
        subscribers_element = soup.find('span', class_="pvs-entity__caption-wrapper")
        
        if subscribers_element:
            text = subscribers_element.text.strip()
            
            # Nettoyer et extraire le nombre
            # Remplacer l'espace insécable et normaliser
            text = text.replace('\u202f', '').replace('\u00a0', '')
            
            # Extraire le nombre avec regex
            match = re.search(r'(\d[\d\s,]*\d|\d+)(?=\s*(abonné|abonnés|follower|followers))', text)
            if match:
                number_str = match.group(1).replace(' ', '').replace(',', '')
                subscribers = int(number_str)
                logger.info(f"Nombre d'abonnés extrait : {subscribers}")
                return subscribers
            else:
                return text
        else:
            return 0
            
    except TimeoutException:
        logger.error(f"Timeout lors du chargement du profil {link}")
        return 0
    except Exception as e:
        logger.error(f"Erreur lors de la récupération des abonnés pour {link} : {str(e)}")
        return 0