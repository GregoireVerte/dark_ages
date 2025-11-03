import requests
import pandas as pd
import time
import os


CORE_API_KEY = "4fzHBxIQP8e72NqKYrGhZREoc15mu6Sa" #### ENTER_API_KEY


def get_core_articles(query, page_size=25, api_key=None):
    """
    Pobiera artykuły (metadane i abstrakty) z CORE API v3 dla danego zapytania.
    """
    if not api_key or api_key == "ENTER_API_KEY":
        print(f"BŁĄD: Nie podano klucza CORE_API_KEY. Zarejestruj się na https://core.ac.uk/services/api#form")
        return []

    print(f"Rozpoczynam pobieranie dla zapytania: '{query}'...")
    
    # Adres endpointu wyszukiwania v3
    url = "https://api.core.ac.uk/v3/search/works"
    
    # Nagłówek (Header) z autoryzacją.
    headers = {
        "Authorization": f"Bearer {api_key}"
    }
    
    # Ciało (Body) zapytania POST.
    # "abstract" jako pola do przeszukania.
    search_payload = {
        "q": f"(abstract:({query}))", # Szukamy słowa kluczowego tylko w abstrakcie
        "limit": page_size,
        "scroll": False
    }

    try:
        # Wykonaj zapytanie POST, wysyłając dane jako json i nagłówki
        response = requests.post(url, json=search_payload, headers=headers)
        
        # Sprawdź, czy zapytanie się powiodło
        response.raise_for_status() 
        
        data = response.json()
        results = data.get("results", [])
        
        records = []
        for item in results:
            # Wyciągnij licencję
            license_name = item.get("licence", {}).get("name", "")
            
            # Wyciągnij abstrakt
            abstract = item.get("abstract", "")
            
            # Dodaj rekord tylko jeśli ma abstrakt
            if abstract:
                records.append({
                    "title": item.get("title", ""),
                    "abstract": abstract,
                    # CORE v3 nie zawsze zwraca łatwe 'keywords', bierzemy 'subjects'
                    "keywords": ", ".join(item.get("subjects", [])), 
                    "year": item.get("yearPublished", ""),
                    "journal_title": item.get("publisher", ""), # CORE używa 'publisher'
                    "license": license_name,
                    # Linki: próbujemy DOI, jeśli nie ma, bierzemy link do PDF
                    "link_doi": item.get("doi", ""),
                    "link_pdf": item.get("downloadUrl", ""),
                    "query": query
                })
        
        print(f"Pobrano {len(records)} artykułów z abstraktami dla zapytania: '{query}'.")
        return records

    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas zapytania dla '{query}': {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Odpowiedź serwera: {e.response.json()}") 
        return []

# Główna część skryptu
if __name__ == "__main__":
    
    KEYWORDS_TO_SEARCH = [
        "vikings", 
        "carolingian"
    ]
    
    ARTICLES_PER_KEYWORD = 50 
    
    all_articles_data = []
    
    print("Rozpoczynam zbieranie danych z CORE.ac.uk...")
    
    for keyword in KEYWORDS_TO_SEARCH:
        articles = get_core_articles(keyword, 
                                     page_size=ARTICLES_PER_KEYWORD, 
                                     api_key=CORE_API_KEY)
        
        all_articles_data.extend(articles)
        time.sleep(1)
    
    if all_articles_data:
        df = pd.DataFrame(all_articles_data)
        
        output_filename = "core_articles.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\nZapisano łącznie {len(df)} artykułów do pliku: {output_filename}")
        print("Przykładowe dane:")
        print(df.head())
    else:
        print("Nie udało się pobrać żadnych artykułów.")