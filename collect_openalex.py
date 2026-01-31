import requests
import pandas as pd
import time
import random

def reconstruct_abstract(inverted_index):
    """Rekonstruuje abstrakt z formatu Inverted Index OpenAlex."""
    if not inverted_index or not isinstance(inverted_index, dict):
        return ""
    
    word_index = {}
    for word, locations in inverted_index.items():
        for loc in locations:
            word_index[loc] = word
    
    return " ".join([word_index[i] for i in sorted(word_index.keys())])

def get_openalex_data(keywords, email, max_results_per_kw=100):
    base_url = "https://api.openalex.org/works"
    all_results = []
    per_page = 50  # Optymalna liczba wyników na jedną stronę API

    for kw in keywords:
        print(f"\n--- Rozpoczynam pobieranie dla: {kw} ---")
        results_for_kw = 0
        page = 1
        
        while results_for_kw < max_results_per_kw:
            print(f"  Pobieranie strony {page}...")
            
            params = {
                'search': kw,
                'mailto': email,
                'page': page,
                'per_page': per_page,
                'select': 'id,title,authorships,abstract_inverted_index,publication_year,concepts,host_venue'
            }

            try:
                response = requests.get(base_url, params=params, timeout=30)
                
                if response.status_code == 429:
                    print("  Otrzymano Error 429 (Rate Limit). Czekam dłużej...")
                    time.sleep(60)
                    continue
                
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    print("  Brak więcej wyników dla tego słowa.")
                    break

                for work in results:
                    if results_for_kw >= max_results_per_kw:
                        break
                    
                    # Autorzy
                    authors = ", ".join([auth.get('author', {}).get('display_name', '') 
                                       for auth in work.get('authorships', [])])
                    
                    # Tematy (Concepts)
                    subjects = ", ".join([c.get('display_name', '') 
                                        for c in work.get('concepts', [])])
                    
                    # Wydawca
                    publisher = work.get('host_venue', {}).get('publisher', '') or ""

                    # Budowa rekordu - jeśli czegoś brakuje, wpisujemy pusty string
                    row = {
                        'keyword': kw,
                        'title': work.get('title') or "",
                        'creator': authors,
                        'abstract': reconstruct_abstract(work.get('abstract_inverted_index')),
                        'year': work.get('publication_year') or "",
                        'subject': subjects,
                        'publisher': publisher,
                        'id': (work.get('id') or "").replace("https://openalex.org/", "")
                    }
                    all_results.append(row)
                    results_for_kw += 1

                print(f"  Pobrano łącznie: {results_for_kw}/{max_results_per_kw}")

                # Sprawdzenie czy jest sens iść do następnej strony
                if len(results) < per_page:
                    break
                
                page += 1
                
                # Losowy delay między stronami (ochrona przed 429)
                delay = random.uniform(3.5, 6.5)
                print(f"  Czekam {delay:.2f}s przed kolejną stroną...")
                time.sleep(delay)

            except Exception as e:
                print(f"  Błąd krytyczny przy {kw}: {e}")
                break

        # Dodatkowy delay po zakończeniu całego słowa kluczowego
        time.sleep(random.uniform(2.0, 4.0))

    return pd.DataFrame(all_results)

# ustawienia
# e-mail - dzięki temu trafiasz do "Polite Pool" w OpenAlex
MY_EMAIL = "trevor84@wp.pl" 

# keywords
test_keywords = [
    "fall of rome", 
    "barbarian migrations"
]

# Limit rekordów na każde słowo kluczowe
MAX_PER_KW = 100 

# Uruchomienie
df_results = get_openalex_data(test_keywords, MY_EMAIL, max_results_per_kw=MAX_PER_KW)

# Zapis do CSV
df_results.to_csv('openalex_articles.csv', index=False, encoding='utf-8')

print(f"\nZapisano {len(df_results)} rekordów do pliku 'openalex_articles.csv'.")