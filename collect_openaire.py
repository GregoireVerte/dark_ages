import requests
import pandas as pd
import time

def get_openaire_articles(query, page_size=25):
    """
    Pobiera artykuły (metadane i abstrakty) z nowego OpenAIRE Graph API.
    """
    print(f"Rozpoczynam pobieranie dla zapytania (Graph API): '{query}'...")
    
    # nowy endpoint Graph API
    url = "https://graph.openaire.eu/v1/api/search/publications"
    
    # parametry
    params = {
        "query": query,
        "size": page_size,
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status() 
        
        data = response.json()
        
        results_list = data.get('results', [])
        
        print(f"  > Znaleziono {len(results_list)} rekordów w odpowiedzi.")
            
        records = []
        
        for item in results_list:
            try:
                # wyciąganie tytułu
                title = item.get('title', '')
                
                # wyciąganie abstraktu - jest w 'abstracts', czasem w 'description'
                abstract = ""
                abstracts_list = item.get('abstracts', []) #### to jest często lista
                if abstracts_list:
                    abstract = abstracts_list[0]
                else:
                    # próba też dla 'description' jeśli 'abstracts' jest puste
                    abstract = item.get('description', '')

                # wyciąganie roku
                year = item.get('year', '')

                # wyciąganie licencji
                license_name = item.get('license', {}).get('name', '')
                
                # dodanie rekordu tylko jeśli ma abstrakt
                if abstract:
                    records.append({
                        "title": title,
                        "abstract": abstract,
                        "keywords": query, # API nie zwraca łatwo słów kluczowych
                        "year": year, 
                        "journal_title": item.get('publisher', ''), 
                        "license": license_name, 
                        "link": item.get('id', ''), # uzycie 'id' jako unikalnego linku
                        "query": query
                    })
            except Exception as e:
                print(f"  > Błąd parsowania jednego rekordu: {e}")
                continue # przejście do następnego rekordu
        
        print(f"Pobrano {len(records)} artykułów z abstraktami dla zapytania: '{query}'.")
        return records

    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas zapytania dla '{query}': {e}")
        if hasattr(e, 'response') and e.response is not None:
            try:
                print(f"Odpowiedź serwera (JSON): {e.response.json()}")
            except requests.exceptions.JSONDecodeError:
                print(f"Odpowiedź serwera (TEXT): {e.response.text[:500]}")
        return []

#### Główna część skryptu
if __name__ == "__main__":
    
    KEYWORDS_TO_SEARCH = [
        "vikings", 
        "carolingian"
    ]
    
    ARTICLES_PER_KEYWORD = 25 
    
    all_articles_data = []
    
    print("Rozpoczynam zbieranie danych z OpenAIRE (Graph API)...")
    
    for keyword in KEYWORDS_TO_SEARCH:
        articles = get_openaire_articles(keyword, page_size=ARTICLES_PER_KEYWORD)
        all_articles_data.extend(articles)
        
        #### większa przerwa na wszelki wypadek ####
        time.sleep(3) 
    
    if all_articles_data:
        df = pd.DataFrame(all_articles_data)
        
        output_filename = "openaire_articles.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\nGotowe! Zapisano łącznie {len(df)} artykułów do pliku: {output_filename}")
        print("Przykładowe dane:")
        print(df.head())
    else:
        print("Nie udało się pobrać żadnych artykułów.")