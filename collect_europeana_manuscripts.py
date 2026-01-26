import os
import requests
import csv
import time
from dotenv import load_dotenv
import random

# klucz API
load_dotenv()
API_KEY = os.getenv("EUROPEANA_PERSONAL_API_KEY")

if not API_KEY:
    raise ValueError("Nie znaleziono EUROPEANA_PERSONAL_API_KEY w pliku .env")

BASE_URL = "https://api.europeana.eu/record/search.json"
ROWS_PER_PAGE = 100          # max 100
MAX_RECORDS_PER_KEYWORD = 1500  # limit na hasło

# keywords
KEYWORDS = [
    "desiderata",
    "psalterium",
    "evangeliarium",
    "sacramentarium",
    "missale",
    "capitulare",
    "vita sancti",
    "gesta regum francorum",
    "karoli magni",
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
    "augustin",
    "hieronym",
    "isidor",
    "boethi",
    "prudenti",
    "alcuin",
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
    "glossa",
    "diploma",
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
    "polonia",          # Ogólne hasło (Polonia/Poloniae)
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
    "gallus",           # Gall Anonim (szukamy "Gallus Anonymus" lub samego imienia)
    "vincentius",       # Wincenty Kadłubek
    "brunon",           # Brunon z Kwerfurtu
    "ottonis",          # Otton (dokumenty ottońskie, relacje z PL)

    # --- Anglosasi (Mercja, Kent, Northumbria) i Insularia ---
    "gildas",           # "De Excidio et Conquestu Britanniae" (kluczowe źródło)
    "nennius",          # "Historia Brittonum"
    "aldhelm",          # Ważny autor anglosaski (Szeroko kopiowany)
    "bonifatius",       # Św. Bonifacy (Apostoł Niemiec, ale Anglosas, mnóstwo listów)
    "cuthbert",         # Św. Kutbert (Lindisfarne)
    "wilfrid",          # Św. Wilfryd
    "aelfric",          # Aelfric z Eynsham (gramatyka, homilie)
    "anglosaxon",       # Często występuje w nowoczesnych opisach metadanych
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
    "adamus",           # Adam z Bremy (Gesta Hammaburgensis - północ)
    "fulda",            # Klasztor w Fuldzie (wielkie centrum skryptorium)
    "sangallensis",     # Klasztor St. Gallen (częste sygnatury: Codex Sangallensis)
    "lorsch",           # Klasztor Lorsch

    # --- Wielcy Ojcowie i Antyk (Uzupełnienie) ---
    "ambrosius",        # Św. Ambroży (mediolański)
    "gregorius",        # Grzegorz Wielki (często "Gregorius Magnus")
    "cassianus",        # Jan Kasjan (rozmowy z ojcami pustyni)
    "orosius",          # Historia przeciw poganom (podręcznik historii w średniowieczu)
    "macrobius",        # Sen Scypiona (neoplatonizm, mapy strefowe)
    "servius",          # Komentarze do Wergiliusza
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
    "membrane",         # Membrana (inne określenie pergaminu)
    "incipit",          # "Zaczyna się" (początek tekstu)
    "explicit",         # "Kończy się" (koniec tekstu)
    "rubrica",          # Czerwone nagłówki
    "illumination",     # Iluminacje (często w opisach angielskich)
    "neums",            # Neumy (zapis muzyczny)
]

# pola do wyciągnięcia
FIELDS = (
    "europeana_id,record_id,"
    "title,proxy_dc_title,dcTitle,"
    "dcCreator,proxy_dc_creator,"
    "dcDescription,proxy_dc_description,"
    "year,edmYear,proxy_dc_date,dcterms_created,dcterms_issued,"
    "dcSubject,proxy_dc_subject,"
    "dcPublisher,proxy_dc_publisher"
)

def search_europeana(query, api_key, cursor="*"):
    params = {
        "query": query,   ### zamiast f'"{query}"'  ## tym razem bez exact phrase
        "wskey": api_key,
        "rows": ROWS_PER_PAGE,
        "cursor": cursor,
        "profile": "rich",           # daje proxy_dc_* pola (lepsze opisy)
        "qf": 'TYPE:TEXT',          # tu filtry
        "fields": FIELDS,
    }
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 429:
        print(f"Rate limit (429) dla query: {query} – czekam 60 sekund...")
        time.sleep(60)
        return None
    
    if response.status_code != 200:
        print(f"Błąd {response.status_code} dla query: {query}")
        print(response.text)
        return None
    
    return response.json()


def main():
    all_rows = []
    
    for keyword in KEYWORDS:
        print(f"\nPrzetwarzam: {keyword}")
        cursor = "*"  # start cursor pagination
        collected = 0
        
        while collected < MAX_RECORDS_PER_KEYWORD:
            data = search_europeana(keyword, API_KEY, cursor)
            if not data or "success" not in data or not data["success"]:
                print("API zwróciło błąd lub brak sukcesu.")
                break
            
            items = data.get("items", [])
            total = data.get("totalResults", 0)
            print(f"  Znaleziono {total} wyników ogółem | cursor: {cursor} | pobrano {len(items)}")
            
            if not items:
                break

            next_cursor = data.get("nextCursor")
            if not next_cursor or len(items) == 0:
                break
            
            for item in items:
                if collected >= MAX_RECORDS_PER_KEYWORD:
                    break
                
                row = {
                    "keyword": keyword,
                    "title": (
                        item.get("proxy_dc_title", [None])[0] or
                        item.get("title", [None])[0] or
                        item.get("dcTitle", [None])[0] or ""
                    ),
                    "creator": (
                        "; ".join(item.get("proxy_dc_creator", [])).strip() or
                        "; ".join(item.get("dcCreator", [])).strip() or ""
                    ),
                    "abstract": (
                        item.get("proxy_dc_description", [None])[0] or
                        item.get("dcDescription", [None])[0] or ""
                    ),
                    "year": (
                        "; ".join(item.get("edmYear", []) or item.get("year", []) or
                                  item.get("proxy_dc_date", []) or
                                  item.get("dcterms_created", []) or
                                  item.get("dcterms_issued", [])).strip() or ""
                    ),
                    "subject": (
                        "; ".join(item.get("proxy_dc_subject", []) or
                                  item.get("dcSubject", [])).strip() or ""
                    ),
                    "publisher": (
                        "; ".join(item.get("proxy_dc_publisher", []) or
                                  item.get("dcPublisher", [])).strip() or ""
                    ),
                    "id": (
                        item.get("europeana_id") or
                        item.get("record_id") or
                        ""
                    ),
                }
                all_rows.append(row)
                collected += 1
            
            cursor = next_cursor
            time.sleep(random.uniform(3.5, 9.5))  # delay, żeby nie spamować API

    # zapis do CSV
    if all_rows:
        headers = ["keyword", "title", "creator", "abstract", "year", "subject", "publisher", "id"]
        with open("europeana_articles_3turn.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\nZapisano {len(all_rows)} rekordów do europeana_articles_3turn.csv")
    else:
        print("Nie pobrano żadnych rekordów.")

if __name__ == "__main__":
    main()