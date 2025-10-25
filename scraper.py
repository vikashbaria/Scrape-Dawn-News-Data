# Dawn News Continuous Scraper for morph.io
# https://morph.io — stores results in data.sqlite

import scraperwiki
import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import os
import sqlite3

CONSECUTIVE_FAIL_LIMIT = 10  # stop temporarily if 10 fails in a row
DB_FILE = "data.sqlite"

# --- Create Scraper ---
def create_scraper():
    chrome_versions = ["121.0.0.0", "122.0.0.0", "123.0.0.0", "124.0.0.0", "125.0.0.0", "141.0.0.0"]
    user_agent = f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
                 f"(KHTML, like Gecko) Chrome/{random.choice(chrome_versions)} Safari/537.36"
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.8", "en;q=0.7"]),
        "Referer": "https://www.google.com/",
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
        "Accept-Encoding": "gzip, deflate, br",
    }
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "windows", "mobile": False},
        delay=random.randint(1, 3),
    )
    scraper.headers.update(headers)
    return scraper

def clean_text(text):
    return ' '.join(text.strip().split()) if text else ""

# --- Initialize Database ---
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()
cur.execute("""
CREATE TABLE IF NOT EXISTS data (
    article_id INTEGER PRIMARY KEY,
    url TEXT,
    status TEXT,
    error_reason TEXT,
    title TEXT,
    date TEXT,
    meta TEXT,
    content TEXT,
    images TEXT
)
""")
conn.commit()

# --- Get Last ID ---
cur.execute("SELECT MAX(article_id) FROM data")
row = cur.fetchone()
last_id = row[0] if row and row[0] else 0
print(f"🔁 Last saved article ID = {last_id}")

scraper = create_scraper()
fail_streak = 0
article_id = last_id + 1

# --- Continuous Loop ---
while True:
    url = f"https://www.dawn.com/news/{article_id}"
    print(f"\nFetching {url} ...")

    article_data = {
        "article_id": article_id,
        "url": url,
        "status": "",
        "error_reason": "",
        "title": "",
        "date": "",
        "meta": "",
        "content": "",
        "images": ""
    }

    success = False

    for attempt in range(3):
        try:
            response = scraper.get(url, timeout=25)
            response.encoding = response.apparent_encoding

            if response.status_code != 200:
                article_data["status"] = f"HTTP_{response.status_code}"
                article_data["error_reason"] = f"HTTP {response.status_code}"
                print(f"❌ Attempt {attempt+1}: {url} ({response.status_code})")

                if response.status_code == 403:
                    print("🔁 403 detected — regenerating scraper...")
                    scraper = create_scraper()
                    time.sleep(random.uniform(5, 10))
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            title_tag = soup.find("h2", class_="story__title") or soup.find("h1")
            title = clean_text(title_tag.get_text()) if title_tag else None

            date_tag = soup.find("span", class_="timestamp") or soup.find("meta", {"property": "article:published_time"})
            date = date_tag.get_text(strip=True) if date_tag and date_tag.name == "span" else (date_tag["content"] if date_tag else None)

            meta_desc = soup.find("meta", {"name": "description"})
            meta = meta_desc["content"] if meta_desc else None

            content_div = (
                soup.find("div", class_="story__content")
                or soup.find("div", class_="story-content")
                or soup.find("div", id="story-content")
                or soup.find("article", class_="story")
                or soup.find("div", {"itemprop": "articleBody"})
            )

            if not content_div:
                article_data["status"] = "no_content"
                article_data["error_reason"] = "Main content missing"
                print(f"⚠️ No main content for {url}")
                continue

            for tag in content_div(["script", "iframe", "ins", "aside", "style"]):
                tag.decompose()

            paragraphs = [clean_text(p.get_text()) for p in content_div.find_all("p") if clean_text(p.get_text())]
            content = " ".join(paragraphs)
            images = [img["src"] for img in content_div.find_all("img", src=True)]

            if not content.strip():
                article_data["status"] = "empty_content"
                article_data["error_reason"] = "Parsed but no text"
                continue

            article_data.update({
                "status": "success",
                "error_reason": "",
                "title": title,
                "date": date,
                "meta": meta,
                "content": content,
                "images": ", ".join(images)
            })
            success = True
            print(f"✅ Saved: {title[:60] if title else 'No title'}")
            break

        except Exception as e:
            article_data["status"] = "exception"
            article_data["error_reason"] = str(e)
            print(f"⚠️ Error attempt {attempt+1}: {e}")
            time.sleep(random.uniform(3, 6))

    # --- Save to SQLite ---
    scraperwiki.sqlite.save(
        unique_keys=['article_id'],
        data=article_data,
        table_name="data"
    )

    conn.commit()

    if success:
        fail_streak = 0
    else:
        fail_streak += 1

    # --- Stop temporarily if too many fails ---
    if fail_streak >= CONSECUTIVE_FAIL_LIMIT:
        print(f"😴 Too many ({fail_streak}) consecutive failed articles. Sleeping for 30 minutes...")
        fail_streak = 0
        time.sleep(1800)

    article_id += 1
    time.sleep(random.uniform(2, 8))
