import pandas as pd
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.edge.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pyodbc

# 📌 Configuration du chemin du EdgeDriver
EDGE_DRIVER_PATH = r"C:\Users\marie\edgedriver_win64\msedgedriver.exe"

# 📚 Catégories à scraper
categories_to_scrape = [
    "Travel", "Mystery", "Historical Fiction", "Sequential Art", "Classics",
    "Philosophy", "Romance", "Womens Fiction", "Fiction", "Childrens",
    "Religion", "Nonfiction", "Music", "Default", "Science Fiction", "Sports and Games"
]

# 🚗 Configuration du navigateur Edge en mode headless
options = Options()
options.add_argument("--headless")
options.add_argument("--disable-gpu")
service = Service(executable_path=EDGE_DRIVER_PATH)
driver = webdriver.Edge(service=service, options=options)

# ⭐ Fonction pour convertir la classe en note
def get_rating(class_str):
    for word, val in {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}.items():
        if word in class_str:
            return val
    return 0

# 🔎 Accès à la page principale
driver.get("https://books.toscrape.com/")
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "nav-list")))

category_elements = driver.find_elements(By.CSS_SELECTOR, ".nav-list ul li a")
category_urls = {
    el.text.strip(): el.get_attribute("href")
    for el in category_elements if el.text.strip() in categories_to_scrape
}

# 📊 Préparation des DataFrames
df_categories = pd.DataFrame(columns=["id", "title"])
df_status = pd.DataFrame(columns=["id", "status"])
df_books = pd.DataFrame(columns=["id", "id_categorie", "id_status", "ratings", "title", "price"])

book_id = 1
status_map = {}
category_id = 1

for cat_name, cat_url in category_urls.items():
    print(f"🔍 Scraping category: {cat_name}")
    try:
        driver.get(cat_url)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, "product_pod")))

        df_categories = pd.concat([
            df_categories,
            pd.DataFrame([{"id": category_id, "title": cat_name}])
        ])

        collected = 0
        while collected < 10:
            books = driver.find_elements(By.CLASS_NAME, "product_pod")

            for book in books:
                if collected >= 10:
                    break

                title = book.find_element(By.TAG_NAME, "h3").text.strip()
                price = float(book.find_element(By.CLASS_NAME, "price_color").text.strip().replace("£", ""))
                rating_class = book.find_element(By.CLASS_NAME, "star-rating").get_attribute("class")
                rating = get_rating(rating_class)
                availability = book.find_element(By.CLASS_NAME, "availability").text.strip()

                if availability not in status_map:
                    status_id = len(status_map) + 1
                    status_map[availability] = status_id
                    df_status = pd.concat([
                        df_status,
                        pd.DataFrame([{"id": status_id, "status": availability}])
                    ])

                df_books = pd.concat([
                    df_books,
                    pd.DataFrame([{
                        "id": book_id,
                        "id_categorie": category_id,
                        "id_status": status_map[availability],
                        "ratings": rating,
                        "title": title,
                        "price": price
                    }])
                ])
                book_id += 1
                collected += 1

            try:
                next_btn = driver.find_element(By.CLASS_NAME, "next")
                next_url = next_btn.find_element(By.TAG_NAME, "a").get_attribute("href")
                driver.get(next_url)
            except:
                break

        category_id += 1

    except TimeoutException:
        print(f"⏱ Timeout in category: {cat_name}")
        continue
    except Exception as e:
        print(f"❌ Error in category {cat_name}: {e}")
        continue

driver.quit()

# ✅ Affichage local (facultatif)
print("\n✅ Categories:")
print(df_categories)
print("\n✅ Status:")
print(df_status)
print("\n✅ Books:")
print(df_books)

# 💾 Connexion à SQL Server pour insérer les données
conn_str = (
    r"DRIVER={ODBC Driver 17 for SQL Server};"
    r"SERVER=MBY-VICTUS;"
    r"DATABASE=Books;"  # ✅ Nom de votre base de données
    r"Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# ➕ Insertion des catégories
for _, row in df_categories.iterrows():
    cursor.execute(
        "INSERT INTO dbo.categories (id, title) VALUES (?, ?)",
        int(row['id']), row['title']
    )

# ➕ Insertion des statuts
for _, row in df_status.iterrows():
    cursor.execute(
        "INSERT INTO dbo.status (id, status) VALUES (?, ?)",
        int(row['id']), row['status']
    )

# ➕ Insertion des livres
for _, row in df_books.iterrows():
    cursor.execute(
        """
        INSERT INTO dbo.books (id, id_categorie, id_status, ratings, title, price)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        int(row['id']),
        int(row['id_categorie']),
        int(row['id_status']),
        int(row['ratings']),
        row['title'],
        float(row['price'])
    )

conn.commit()
conn.close()

print("\n📥 Données insérées avec succès dans la base SQL Server 'Books'.")
