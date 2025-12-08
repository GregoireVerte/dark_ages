import requests
import pandas as pd
import time
from typing import List, Dict

## lista słów kluczowych
KEYWORDS = [
    "fall of rome",
    "early medieval history",
    "barbarian invasions",
    "justinian plague",
    "vandal africa",
]

BASE_URL = "https://openlibrary.org/search.json"
HEADERS = {
    "User-Agent": "DarkAgesHistoryMLProject (trevor84@wp.pl)"  ### wymagane dla rate limit
}
RESULTS_PER_QUERY = 50  ### max na stronę, by nie przeciążyć
OUTPUT_CSV = "open_library_articles.csv"

def search_books(query: str) -> List[Dict]:
    params = {
        "q": query,
        "limit": RESULTS_PER_QUERY
    }
    try:
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)  # timeout
        response.raise_for_status()
        data = response.json()
        docs = data.get("docs", [])
        results = []
        for doc in docs:
            if not doc.get("title"):
                continue
            #### description czasem jest dict, czasem string – trzeba obsłużyć oba przypadki
            desc = doc.get("description", "")
            if isinstance(desc, dict):
                desc = desc.get("value", "")
            results.append({
                "keyword": query,
                "title": doc.get("title", ""),
                "authors": ", ".join(doc.get("author_name", [])),
                "publish_year": doc.get("first_publish_year"),
                "description": desc
            })
        return results

    except requests.exceptions.RequestException as e:
        print(f"Błąd sieci dla '{query}': {e}")
        return []
    except Exception as e:
        print(f"Nieoczekiwany błąd dla '{query}': {e}")
        return []

def main():
    all_books = []
    for i, keyword in enumerate(KEYWORDS, 1):
        print(f"Przetwarzam {i}/{len(KEYWORDS)}: {keyword}")
        books = search_books(keyword)
        all_books.extend(books)
        time.sleep(3)  ### pauza na rate limit
    
    ## zapis do CSV
    if all_books:
        df = pd.DataFrame(all_books)
        df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
        print(f"Zapisano {len(all_books)} rekordów do {OUTPUT_CSV}")
    else:
        print("Brak danych – sprawdź API.")

if __name__ == "__main__":
    main()