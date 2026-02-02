import requests
import pandas as pd
import time
import random
import os

def reconstruct_abstract(inverted_index):
    """Rekonstruuje abstrakt z formatu Inverted Index OpenAlex."""
    if not inverted_index or not isinstance(inverted_index, dict):
        return ""
    
    word_index = {}
    for word, locations in inverted_index.items():
        for loc in locations:
            word_index[loc] = word
    
    return " ".join([word_index[i] for i in sorted(word_index.keys())])

def get_openalex_data(keywords, email, max_results_per_kw=1000):
    base_url = "https://api.openalex.org/works"
    csv_filename = 'openalex_manuscripts.csv'
    
    # sprawdza czy plik istnieje żeby wiedzieć czy dopisać nagłówek
    file_exists = os.path.isfile(csv_filename)

    per_page = 50  # optymalna liczba wyników na jedną stronę API

    for kw in keywords:
        print(f"\n--- Rozpoczynam pobieranie dla: {kw} ---")
        current_kw_results = [] ## lista tymczasowa dla pojedynczego keyworda
        results_for_kw = 0
        page = 1
        
        while results_for_kw < max_results_per_kw:
            print(f"  Pobieranie strony {page}...")
            
            params = {
                'search': f'"{kw}"',
                'mailto': email,
                'page': page,
                'per_page': per_page,
                'select': 'id,title,authorships,abstract_inverted_index,publication_year,topics,primary_location'
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
                    
                    # Tematy ('topics')
                    subjects = ", ".join([t.get('display_name', '')
                                        for t in work.get('topics', [])])
                    
                    # Wydawca (schowany głębiej w primary_location -> source)
                    primary_loc = work.get('primary_location') or {}
                    source = primary_loc.get('source') or {}
                    publisher = source.get('publisher') or ""

                    # Budowa rekordu - jeśli czegoś brakuje, wpisuje pusty string
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
                    current_kw_results.append(row)
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

        ## po każdym słowie kluczowym zapis do pliku
        if current_kw_results:
            temp_df = pd.DataFrame(current_kw_results)
            # 'a' oznacza append, header tylko jeśli plik nie istniał
            temp_df.to_csv(csv_filename, mode='a', index=False, header=not file_exists, encoding='utf-8')
            file_exists = True # następne słowa kluczowe już nie dodają kolejnego nagłówka
            print(f"  ZAPISANO: {len(current_kw_results)} rekordów dla {kw}")
        
        # czyszczenie listy przed następnym słowem by zwolnić RAM
        current_kw_results = []

        # Dodatkowy delay po zakończeniu całego słowa kluczowego
        time.sleep(random.uniform(2.0, 5.0))

    return None

# ustawienia
# e-mail - dzięki temu trafiasz do "Polite Pool" w OpenAlex
MY_EMAIL = "trevor84@wp.pl"

# keywords
openalex_keywords = [
    "desiderius rex",
    "flagellum dei",
    "hunnorum",
    "hunni et alani",
    "vandalorum",
    "gens francorum",
    "gothorum",
    "langobardorum",
    "clodovicus",
	"gothi",
    "psalterium",
    "evangeliarium",
    "sacramentarium",
    "missale",
    "capitulare",
    "vita sancti",
    "gesta regum francorum",
    "karoli magni",
	"caroli magni",
    "chronica",
    "annales regni francorum",
    "historia francorum",
    "gregorius turonensis",
    "bedae historia ecclesiastica",
    "cassiodorus",
    "jordanes getica",
    "fredegar",
    "liber pontificalis",
    "codex argenteus",
    "codex aureus",
    "insular manuscripts",
    "merovingian script",
    "carolingian minuscule",
    "augustinus",
    "hieronymus",
    "isidorus hispalensis",
    "boethius",
    "prudentius",
    "alcuinus",
    "codex theodosianus",
    "lex salica",
    "lex visigothorum",
    "leges barbarorum",
    "breviarium alarici",
    "regula benedicti",
    "homiliarium",
    "martyrologium",
    "lectionarium",
    "pontificale",
    "computus",
    "aratea",
    "uncialis",
    "beneventana",
    "palimpsest",
    "apocalyps",
    "beatus",
    "codex euricianus",
    "leges langobardorum",
	"regnum langobardorum",
    "glossa",
    "dagome",
    "vitae sanctorum",              # najpopularniejsza kategoria żywotów
    "passio sancti",                # męczeństwa
    "acta martyrum",
    "vita beati",
    "epistolae",
    "sermones",
    "homiliae",
    "expositio in evangelium",
    "commentarii in",               # komentarze biblijne
    "regula sancti",
    "regula sancti benedicti",
    "ordo romanus",
    "de viris illustribus",
    "origo gentis",
    "chronicon",
    "diploma regium",
    "chartae",                      # dokumenty, przywileje
    "fragmentum",                   # fragmenty manuskryptów
    # --- Polska, Słowianie i Europa Środkowa ---
    "regnum poloniae",          # Ogólne hasło (Polonia/Poloniae)
    "polonorum",        # "Polaków" (np. Gesta principum Polonorum)
    "sclavi",           # Słowianie
    "sclavorum",        # Słowian
    "vandali",          # Często myleni/utożsamiani ze Słowianami w dawnych opisach
    "vistula",          # Wisła
    "cracovia",         # Kraków (może wystąpić w rocznikach kapituły)
    "gnesna",           # Gniezno
    "boleslaus",        # Bolesław (Chrobry, Śmiały)
    "miesco",           # Mieszko (różne pisownie: Mesco, Mieszka)
    "adalbertus",       # Św. Wojciech (kluczowy dla PL i Czech)
    "thietmar",         # Kronika Thietmara (kluczowa dla Mieszka I)
    "gallus anonymus",           # Gall Anonim (szukamy "Gallus Anonymus" lub samego imienia)
    "vincentius cracoviensis",       # Wincenty Kadłubek
    "bruno querfurtensis",           # Brunon z Kwerfurtu
    "ottonis",          # Otton (dokumenty ottońskie, relacje z PL)
	"civitas schinesghe",

    # --- Anglosasi (Mercja, Kent, Northumbria) i Insularia ---
    "gildas",           # "De Excidio et Conquestu Britanniae" (kluczowe źródło)
    "nennius",          # "Historia Brittonum"
    "aldhelm",          # Ważny autor anglosaski (Szeroko kopiowany)
    "bonifatius",       # Św. Bonifacy (Apostoł Niemiec, ale Anglosas, mnóstwo listów)
    "cuthbert",         # Św. Kutbert (Lindisfarne)
    "wilfrid",          # Św. Wilfryd
    "aelfric",          # Aelfric z Eynsham (gramatyka, homilie)
    "saxones",
    "hibernia",         # Irlandia (źródło stylu insularnego)
    "scotia",           # Szkocja/Irlandia w dawnej nomenklaturze
    "lindisfarne",      # Ewangeliarze z Lindisfarne
    "durrow",           # Księga z Durrow
    "kells",            # Księga z Kells (może być opisana jako 'Codex Cenannensis')
    "echternach",       # Klasztor Willibrorda (wpływy anglosaskie)

    # --- Karolingowie, HRE i Kronikarze (Frankowie/Niemcy) ---
    "einhard",          # Biograf Karola Wielkiego (Vita Karoli)
    "notker",           # Notker Balbulus (Sekwencje, Gesta Karoli)
    "rabanus",          # Raban Maur (Encyklopedysta, "Praeceptor Germaniae")
    "hincmar",          # Hinkmar z Reims
    "thegan",           # Żywot Ludwika Pobożnego
    "nithard",          # Historyk, wnuk Karola Wielkiego
    "widukind",         # Widukind z Korbei (Dzieje Sasów)
    "liutprand",        # Liutprand z Cremony (relacje z Bizancjum, X w.)
    "hrotsvitha",       # Hroswita z Gandersheim (pierwsza poetka/dramatopisarka)
    "adamus bremensis",           # Adam z Bremy (Gesta Hammaburgensis - północ)
    "fulda",            # Klasztor w Fuldzie (wielkie centrum skryptorium)
    "sangallensis",     # Klasztor St. Gallen (częste sygnatury: Codex Sangallensis)
    "lorsch",           # Klasztor Lorsch

    # --- Wielcy Ojcowie i Antyk (Uzupełnienie) ---
    "ambrosius mediolanensis",        # Św. Ambroży (mediolański)
    "gregorius magnus",        # Grzegorz Wielki (często "Gregorius Magnus")
    "cassianus",        # Jan Kasjan (rozmowy z ojcami pustyni)
    "orosius",          # Historia przeciw poganom (podręcznik historii w średniowieczu)
    "macrobius",        # Sen Scypiona (neoplatonizm, mapy strefowe)
    "servius honoratus",          # Komentarze do Wergiliusza
    "donatus",          # Gramatyka (Ars Donati - uczono się z tego łaciny)
    "priscian",         # Gramatyka Priscjana (kolejny podręcznik)

    # --- Liturgia i Typy Ksiąg (Specyficzne) ---
    "antiphonarium",    # Śpiewy liturgiczne (często z nutami/neumami)
    "graduale",         # Graduał
    "troparium",        # Tropary
    "sequentiarium",    # Sekwencjarz
    "hymnarium",        # Hymny
    "penitentiale",     # Księgi pokutne (bardzo ważne dla prawa kanonicznego i obyczajów)
    "bestiarium",       # Bestiariusze (opisy zwierząt, świetne obrazki)
    "herbarum",         # Zielniki (Apuleius Platonicus)

    # --- Techniczne / Materiałowe ---
    "pergamen",         # Pergamin (materiał)
    "illumination",     # Iluminacje (często w opisach angielskich)
    "neums",            # Neumy (zapis muzyczny)
]

# Limit rekordów na każde słowo kluczowe
MAX_PER_KW = 1000

# Uruchomienie
get_openalex_data(openalex_keywords, MY_EMAIL, max_results_per_kw=MAX_PER_KW)

print(f"\nProces zakończony. Sprawdź plik 'openalex_manuscripts.csv'.")