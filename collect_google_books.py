import requests
import time
import csv
import json
from urllib.parse import quote_plus
import os

## konfiguracja
API_KEY = "AIzaSyCPtS8IZhFqfLY8bpJYonCYB3J8pD27X_Q"  ### https://console.cloud.google.com/apis/credentials


OUTPUT_CSV = "google_books_articles.csv"
MAX_RESULTS = 40      # max na jedno zapytanie (API pozwala max 40)
PAUSE_BETWEEN = 3.0   # sekundy między zapytaniami

## słowa kluczowe
KEYWORDS = [
    "late antiquity",
    "barbarian invasions",
]

# filtrowanie gdzie tylko książki z opisem i po angielsku
LANGUAGE_FILTER = "en"

# funkcje
def google_books_search(query, start_index=0):
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "maxResults": MAX_RESULTS,
        "startIndex": start_index,
        "printType": "books",
        "orderBy": "relevance",
    }
    if LANGUAGE_FILTER:
        params["langRestrict"] = LANGUAGE_FILTER
    if API_KEY:
        params["key"] = API_KEY

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"Błąd zapytania: {e}")
        return None

def extract_metadata(item):
    v = item.get("volumeInfo", {})
    title = v.get("title", "")
    subtitle = v.get("subtitle", "")
    full_title = f"{title} {subtitle}".strip() if subtitle else title
    
    authors = ", ".join(v.get("authors", [])) if v.get("authors") else ""
    publisher = v.get("publisher", "")
    published_date = v.get("publishedDate", "")  #### może być sam rok albo YYYY-MM-DD
    description = v.get("description", "")
    page_count = v.get("pageCount", "")
    categories = ", ".join(v.get("categories", [])) if v.get("categories") else ""
    language = v.get("language", "")
    isbn_list = v.get("industryIdentifiers", [])
    isbn = ""
    if isbn_list:
        for i in isbn_list:
            if i["type"] in ["ISBN_13", "ISBN_10"]:
                isbn = i["identifier"]
                break

    return {
        "title": full_title,
        "authors": authors,
        "publisher": publisher,
        "publishedDate": published_date,
        "description": description,
        "pageCount": page_count,
        "categories": categories,
        "language": language,
        "isbn": isbn
    }

# main loop
all_books = []
seen_books = set()

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["title", "authors", "publisher", "publishedDate", 
                  "description", "pageCount", "categories", "language", "isbn", "source_query"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for keyword in KEYWORDS:
        print(f"\nSzukam: {keyword}")
        start_index = 0
        while True:
            data = google_books_search(keyword, start_index)
            if not data or "items" not in data:
                print("Brak wyników lub błąd API")
                break

            items = data["items"]
            if not items:
                break

            for item in items:
                meta = extract_metadata(item)
                if meta["description"] and len(meta["description"]) > 100: ### powyżej 100 znaków żeby nie śmiecić
                    row = meta
                    row["source_query"] = keyword

                    # wstępne usuwanie duplikatów
                    unique_key = meta["isbn"] or (meta["title"] + "|" + meta["authors"]).lower()
                    if unique_key not in seen_books:
                        seen_books.add(unique_key)
                        writer.writerow(row)
                        all_books.append(row)
                        print(f"  + {meta['title'][:70]:70} | {meta['publishedDate']} | {meta['authors'][:50]}")

            # jeśli mniej niż maxResults to koniec wyników dla tego query
            if len(items) < MAX_RESULTS:
                break

            start_index += MAX_RESULTS
            time.sleep(PAUSE_BETWEEN)

        time.sleep(PAUSE_BETWEEN + 1)  # przerwa między słowami kluczowymi

print(f"\nZebrano {len(all_books)} książek z opisami.")
print(f"Zapisano do pliku: {OUTPUT_CSV}")