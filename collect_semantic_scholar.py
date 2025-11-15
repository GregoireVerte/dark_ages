import requests
import pandas as pd
import time

### keywords
keywords = [
    "fall of the roman empire",
    "early middle ages",
    "barbarian invasions",
    "justinian reconquest",
]

# endpoint API - wyszukiwanie po keywords
base_url = "https://api.semanticscholar.org/graph/v1/paper/search"
fields = "title,authors,year,abstract"
limit_per_query = 50

data_rows = []

for keyword in keywords:
    retries = 0
    max_retries = 3

    while retries <= max_retries:
        params = {
            "query": keyword,
            "fields": fields,
            "limit": limit_per_query,
        }
        
        try:
            response = requests.get(base_url, params=params, timeout=30)
            if response.status_code == 429:
                wait_time = 60  # poczekaj minutę po 429
                print(f"429 dla '{keyword}' – czekam {wait_time}s (retry {retries+1}/{max_retries})")
                time.sleep(wait_time)
                retries += 1
                continue  # spróbuj jeszcze raz
            else:
                response.raise_for_status()
                break  # sukces –> wychodzi z pętli retry

        except requests.exceptions.RequestException as e:
            print(f"Błąd dla '{keyword}': {e}")
            break

    else:
        print(f"Nie udało się pobrać '{keyword}' po {max_retries} próbach.")
        time.sleep(10)
        continue

    ### pobieranie danych
    results = response.json().get('data', [])
    for paper in results:
        row = {
            'keyword': keyword,
            'title': paper.get('title', ''),
            'authors': ', '.join([a.get('name', '') for a in paper.get('authors', [])]) or '',
            'year': paper.get('year', '') or '',
            'abstract': (paper.get('abstract', '') or '').replace('\n', ' ').replace('\r', ' ')
        }
        data_rows.append(row)
    
    print(f"Znaleziono {len(results)} wyników dla '{keyword}'")
    time.sleep(10)

# zapis
if data_rows:
    df = pd.DataFrame(data_rows)
    df.to_csv('semantic_scholar_articles.csv', index=False, encoding='utf-8')
    print(f"Zapisano {len(data_rows)} wierszy do semantic_scholar_articles.csv")
else:
    print("Brak danych do zapisania.")