import requests
import pandas as pd
import time

# keywords
keywords = [
    "late antiquity",
    "barbarian invasion"
]

# base URL dla JSON
base_url = "https://api.lib.harvard.edu/v2/items.json"
params_base = {
    "limit": 100,
}

all_records = []

for kw in keywords:
    print(f"Wyszukiwanie dla: \"{kw}\"")
    start = 0
    while True:
        params = params_base.copy()
        params["q"] = f'"{kw}"' ### fraza w cudzysłowie dla dokładnego wyszukiwania
        params["start"] = start
        
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Błąd HTTP: {response.status_code} dla {kw}")
            break
        
        try:
            data = response.json()
        except Exception as e:
            print(f"Błąd parsowania JSON: {e}")
            break

        pagination = data.get("pagination", {})
        num_found = pagination.get("numFound", 0)
        print(f"  --> Znaleziono ogółem: {num_found} rekordów")
        
        items_data = data.get("items")
        if items_data is None:
            print("  Brak klucza 'items' w odpowiedzi – prawdopodobnie 0 wyników")
            break

        # nowa struktura: często "docs" lub bezpośrednio lista MODS obiektów
        items = items_data.get("docs", items_data)  # jeśli brak "docs", bierze bezpośrednio items (array)

        
        if not items:
            print(f"Brak więcej wyników dla {kw}")
            break

        print(f"  Pobrano {len(items)} rekordów z tej strony")
        
        for raw_item in items:
            # dla bezpieczeństwa - jeśli item jest stringiem (rzadki błąd API) to pomiń
            if not isinstance(raw_item, dict):
                print(f"  Pominięto niepoprawny rekord (nie dict): {type(raw_item)}")
                continue

            item = raw_item  # teraz bezpieczny dict

            # Title
            title_info = item.get("titleInfo", {})
            if isinstance(title_info, dict):
                title = title_info.get("title", "")
            elif isinstance(title_info, list) and title_info:
                title = title_info[0].get("title", "")
            else:
                title = ""
            
            # Authors
            authors_list = []
            for name in item.get("name", []):
                if isinstance(name, dict):
                    role = name.get("role", {}).get("text", "")
                    if role == "author":
                        part = name.get("namePart", "")
                        if part:
                            authors_list.append(part)
            authors = "; ".join(authors_list)
            
            # Abstract / description (abstract lub note)
            abstract = ""
            abs_val = item.get("abstract")
            if abs_val:
                if isinstance(abs_val, list) and abs_val:
                    abstract = abs_val[0]
                else:
                    abstract = abs_val
            else:
                note_val = item.get("note")
                if note_val:
                    if isinstance(note_val, list) and note_val:
                        abstract = note_val[0]
                    else:
                        abstract = note_val
            
            # Subject
            subjects_list = []
            for s in item.get("subject", []):
                if isinstance(s, dict):
                    topic = s.get("topic", "")
                    if topic:
                        subjects_list.append(topic)
            subjects = "; ".join(subjects_list)
            
            # Publisher
            publisher = item.get("originInfo", {}).get("publisher", "")
            
            # PubDate
            pub_date = item.get("originInfo", {}).get("dateIssued", "")
            
            # ID
            record_id = item.get("id", "")
            
            all_records.append({
                "keyword": kw,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "subject": subjects,
                "publisher": publisher,
                "pubDate": pub_date,
                "id": record_id
            })
        
        start += params["limit"]
        time.sleep(3)  ### bezpieczny delay

# deduplikacja (po title + authors)
df = pd.DataFrame(all_records)
df.drop_duplicates(subset=["title", "authors"], inplace=True)

# zapis
df.to_csv("harvard_articles.csv", index=False, encoding="utf-8")
print(f"Zebrano {len(df)} unikalnych rekordów. Plik: harvard_articles.csv")