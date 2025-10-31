import requests
import pandas as pd
import time

def get_doaj_articles(query, page_size=100):
    """
    Pobiera artykuły (metadane i abstrakty) z DOAJ API dla danego zapytania.
    """
    print(f"Rozpoczynam pobieranie dla zapytania: '{query}'...")
    
    # Podstawowy URL do API wyszukiwania artykułów
    url = "https://doaj.org/api/v2/search/articles/"
    
    # Parametry zapytania:
    # 'q' to słowo kluczowe
    # 'pageSize' to liczba wyników, którą chcemy otrzymać
    params = {"q": query, "pageSize": page_size}

    try:
        # wykonaj zapytanie GET
        response = requests.get(url, params=params)
        
        # sprawdź, czy zapytanie się powiodło (status 200 OK)
        response.raise_for_status() 
        
        # przetwórz odpowiedź JSON
        data = response.json().get("results", [])
        
        records = []
        # przejdź przez każdy zwrócony artykuł
        for article in data:
            bibjson = article.get("bibjson", {})
            
            #### wyciągnij licencję (ważne!) #### szukanie licencji w kilku miejscach dla pewności
            license_info = bibjson.get("license")
            license_title = ""
            if license_info and isinstance(license_info, list):
                license_title = license_info[0].get("title", "")
            
            # wyciągnij abstrakt (jeśli istnieje)
            abstract = bibjson.get("abstract", "")
            
            # dodaj rekord tylko jeśli ma abstrakt
            if abstract:
                records.append({
                    "title": bibjson.get("title", ""),
                    "abstract": abstract,
                    "keywords": ", ".join(bibjson.get("keywords", [])), # słowa kluczowe połączone w jeden string
                    "year": bibjson.get("year", ""),
                    "journal_title": bibjson.get("journal", {}).get("title", ""),
                    "license": license_title, # plus informację o licencji
                    "link": bibjson.get("link", [{}])[0].get("url", ""), # link do artykułu
                    "query": query # zapis z jakiego zapytania pochodzi rekord
                })
        
        print(f"Pobrano {len(records)} artykułów z abstraktami dla zapytania: '{query}'.")
        return records

    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas zapytania dla '{query}': {e}")
        return []

# Główna część skryptu
if __name__ == "__main__":
    
    # SŁOWA KLUCZOWE DO WYSZYKIWANIA DANYCH
    KEYWORDS_TO_SEARCH = [
        "early medieval history", 
        "vikings", 
        "carolingian",
        "merovingian",
        "slavs early middle ages",
        "byzantium 400-1000",
        "dark ages"
    ]
    
    # liczba artykułów do pobrania na każde słowo kluczowe
    ARTICLES_PER_KEYWORD = 50 
    
    all_articles_data = []
    
    print("Rozpoczynam zbieranie danych z DOAJ...")
    
    for keyword in KEYWORDS_TO_SEARCH:
        # pobierz dane dla słowa kluczowego
        articles = get_doaj_articles(keyword, page_size=ARTICLES_PER_KEYWORD)
        # dodaj pobrane rekordy do głównej listy
        all_articles_data.extend(articles)
        # krótka przerwa aby nie obciążać API
        time.sleep(1) 
    
    if all_articles_data:
        # konwersja listy słowników na DataFrame
        df = pd.DataFrame(all_articles_data)
        
        # zapis danych do pliku CSV
        output_filename = "doaj_articles.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\nGotowe! Zapisano łącznie {len(df)} artykułów do pliku: {output_filename}")
        print("Przykładowe dane:")
        print(df.head()) # pokaż pierwsze 5 wierszy
    else:
        print("Nie udało się pobrać żadnych artykułów.")