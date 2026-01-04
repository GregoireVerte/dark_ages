import requests
import csv
import time
import json

# lista keywordów
keywords = [
    '"fall of rome"',
    '"barbarian migrations"'
]

base_url = "https://www.loc.gov/search/"

# pola do wyciągnięcia
desired_fields = ["title", "creator", "description", "date", "subject", "publisher", "id", "url"]

all_results = []

# limity – bezpieczeństwo + kontrola czasu
MAX_PAGES_PER_KEYWORD = 100      ## max 100 stron na jedno hasło
MAX_TOTAL_RECORDS = 10000        ## globalny limit rekordów
records_collected = 0

for kw in keywords:
    if records_collected >= MAX_TOTAL_RECORDS:
        print("Osiągnięto globalny limit rekordów. Kończę.")
        break
        
    print(f"Searching for: {kw}")
    page = 1
    while page <= MAX_PAGES_PER_KEYWORD:
        if records_collected >= MAX_TOTAL_RECORDS:
            print("Osiągnięto globalny limit rekordów w trakcie keywordu. Przerywam.")
            break
            
        params = {
            "q": kw,       ## exact phrase dzięki cudzysłowom w liście keywords
            "fo": "json",
            "c": 100,      ## max wyników na stronę
            "sp": page,
            ### bez filtrów – książki, artykuły, manuskrypty itd.
        }
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} dla strony {page}")
            break
        
        data = response.json()
        
        results = data.get("results", [])
        if not results:
            print(f"Brak dalszych wyników na stronie {page}. Kończę ten keyword.")
            break
        
        for item in results:
            if records_collected >= MAX_TOTAL_RECORDS:
                break
                
            row = {}
            for field in desired_fields:
                ### niektóre pola są listami lub w pod-słownikach
                value = item.get(field)
                if isinstance(value, list):
                    value = " | ".join([str(v) for v in value if v])
                elif isinstance(value, dict):
                    value = json.dumps(value)
                row[field] = value or ""
            
            # dodatkowy URL do rekordu
            row["item_url"] = item.get("url", "")
            
            all_results.append(row)
            records_collected += 1
        
        print(f"Strona {page} przetworzona – zebrano łącznie {records_collected} rekordów")
        
        # paginacja – sprawdzenie czy jest następna strona
        pagination = data.get("pagination", {})
        if pagination.get("next") is None:
            print("Brak dalszych stron (pagination.next = None). Kończę ten keyword.")
            break
        page += 1
        
        time.sleep(1.5)  ### rate limiting – b. ważne!!

    time.sleep(2)  ## przerwa między keywordami

# zapisz do csv
with open("congress_articles.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=desired_fields + ["item_url"])
    writer.writeheader()
    writer.writerows(all_results)

print(f"\nZebrano łącznie {len(all_results)} rekordów. Zapisano do congress_articles.csv")