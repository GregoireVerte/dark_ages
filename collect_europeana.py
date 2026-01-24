import os
import requests
import csv
import time
from dotenv import load_dotenv

# klucz API
load_dotenv()
API_KEY = os.getenv("EUROPEANA_PERSONAL_API_KEY")

if not API_KEY:
    raise ValueError("Nie znaleziono EUROPEANA_PERSONAL_API_KEY w pliku .env")

BASE_URL = "https://api.europeana.eu/record/search.json"
ROWS_PER_PAGE = 100          # max 100
MAX_RECORDS_PER_KEYWORD = 50  # limit na hasło

# keywords
KEYWORDS = [
    "fall of rome",
    "germanic kingdom",
]

# pola do wyciągnięcia
FIELDS = (
    "europeana_id,"
    "title,"
    "dcCreator,"
    "dcDescription,"
    "year,"
    "dcSubject,"
    "dcPublisher"
)

def search_europeana(query, api_key, cursor="*"):
    params = {
        "query": f'"{query}"',   # automatycznie opakowuje w double quotes --> exact phrase
        "wskey": api_key,
        "rows": ROWS_PER_PAGE,
        "cursor": cursor,
        "profile": "rich",           # daje proxy_dc_* pola (lepsze opisy)
        "qf": "",                    # tu ewentualne filtry
        "fields": FIELDS,
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 429:
        print(f"Rate limit (429) dla query: {query} – czekam 60 sekund...")
        time.sleep(60)
        return None
    
    if response.status_code != 200:
        print(f"Błąd {response.status_code} dla query: {query}")
        print(response.text)
        return None
    
    return response.json()


def main():
    all_rows = []
    
    for keyword in KEYWORDS:
        print(f"\nPrzetwarzam: {keyword}")
        cursor = "*"  # start cursor pagination
        collected = 0
        
        while collected < MAX_RECORDS_PER_KEYWORD:
            data = search_europeana(keyword, API_KEY, cursor)
            if not data or "success" not in data or not data["success"]:
                print("API zwróciło błąd lub brak sukcesu.")
                break
            
            items = data.get("items", [])
            total = data.get("totalResults", 0)
            print(f"  Znaleziono {total} wyników ogółem | cursor: {cursor} | pobrano {len(items)}")
            
            if not items:
                break

            next_cursor = data.get("nextCursor")
            if not next_cursor or len(items) == 0:
                break
            
            for item in items:
                if collected >= MAX_RECORDS_PER_KEYWORD:
                    break
                
                row = {
                    "keyword": keyword,
                    "title": item.get("title", [None])[0] or "",
                    "creator": "; ".join(item.get("dcCreator", [])).strip() if item.get("dcCreator") else "",
                    "abstract": item.get("dcDescription", [None])[0] or "",
                    "year": "; ".join(item.get("year", [])).strip() if item.get("year") else "",
                    "subject": "; ".join(item.get("dcSubject", [])).strip() if item.get("dcSubject") else "",
                    "publisher": "; ".join(item.get("dcPublisher", [])).strip() if item.get("dcPublisher") else "",
                    "id": item.get("europeana_id", ""),
                }
                all_rows.append(row)
                collected += 1
            
            cursor = next_cursor
            time.sleep(1.5)  # delay, żeby nie spamować API

    # zapis do CSV
    if all_rows:
        headers = ["keyword", "title", "creator", "abstract", "year", "subject", "publisher", "id"]
        with open("europeana_articles.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\nZapisano {len(all_rows)} rekordów do europeana_articles.csv")
    else:
        print("Nie pobrano żadnych rekordów.")

if __name__ == "__main__":
    main()