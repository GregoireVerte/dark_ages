### Źródło danych: DOAJ (Directory of Open Access Journals)

import requests
import pandas as pd
import time
from urllib.parse import quote  # do kodowania query w URL

def get_doaj_articles(query, page_size=100):
    """
    Pobiera artykuły (metadane i abstrakty) z DOAJ API (v4) dla danego zapytania.
    """
    print(f"Rozpoczynam pobieranie dla zapytania: '{query}'...")
    
    # Podstawowy URL do API wyszukiwania artykułów; query w path, parametry w query string
    encoded_query = quote(query)  # koduj spacje itp., np. "early medieval history" -> "early%20medieval%20history"
    url = f"https://doaj.org/api/search/articles/{encoded_query}"
    
    # Parametry zapytania:
    params = {
            "page": 1,
            "pageSize": min(page_size, 100)  # max 100 w DOAJ
        }

    try:
        # wykonaj zapytanie GET
        response = requests.get(url, params=params)
        
        # sprawdź, czy zapytanie się powiodło (status 200 OK)
        response.raise_for_status()
        
        # przetwórz odpowiedź JSON
        data = response.json()
        results = data.get("results", [])
        
        records = []

        # przejdź przez każdy zwrócony artykuł
        for article in results:
            #### w v4 dane są w "bibjson"
            bibjson = article.get("bibjson", {})
            abstract = bibjson.get("abstract", "")

            if not abstract:
                continue
            
            #### wyciągnij licencję – lista obiektów (ważne!) #### szukanie licencji w kilku miejscach dla pewności
            license_info = bibjson.get("license", [])
            license_title = ""
            if license_info and isinstance(license_info, list) and len(license_info) > 0:
                license_title = license_info[0].get("type", "")
            
            #### Link – lista obiektów
            link_url = ""
            links = bibjson.get("link", [])
            if links and isinstance(links, list) and len(links) > 0:
                link_url = links[0].get("url", "")
            

            records.append({
                "title": bibjson.get("title", ""),
                "abstract": abstract,
                "keywords": ", ".join(bibjson.get("keywords", [])),
                "year": bibjson.get("year", ""),
                "journal_title": bibjson.get("journal", {}).get("title", ""),
                "license": license_title,
                "link": link_url,
                "query": query
            })
        
        print(f"Pobrano {len(records)} artykułów z abstraktami dla zapytania: '{query}'.")
        return records

    except requests.exceptions.RequestException as e:
        print(f"Błąd podczas zapytania dla '{query}': {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Odpowiedź serwera: {e.response.text[:500]}")
        return []

# Główna część skryptu
if __name__ == "__main__":
    
    ## SŁOWA KLUCZOWE DO WYSZUKIWANIA DANYCH
    KEYWORDS_TO_SEARCH = [
        "early medieval history", 
        "barbarian migrations", 
        "vikings", 
        "carolingian",
        "merovingian",
        "early middle ages",
        "byzantium",
        "dark ages",
        "anglo-saxon",
        "saxons",
        "angles",
        "jutes",
        "early slavs",
        "romano-britons",
        "romano-british",
        "huns",
        "goths",
        "ostrogoths",
        "visigoths",
        "franks",
        "alemanni",
        "vandal",
        "gepids",
        "lombards",
        "attila",
        "thuringians",
        "rugians",
        "sciri",
        "herules",
        "bavarians",
        "avars",
        "burgundians",
        "viking",
        "germanic paganism",
        "slavic paganism",
        "medieval celts",
        "norse paganism",
        "foederati",
        "britannia",
        "suebi",
        "alans",
        "frisians",
        "sarmatians",
        "period 400-1000",
        "late roman empire",
        "barbarian kingdoms",
        "arianism",
        "magyars",
        "khazar",
        "umayyad",
        "charlemagne",
        "clovis",
        "theodoric the great",
        "medieval constantinople",
        "gothic war",
        "barbarian invasions",
        "rome imperial borders",
        "late antiquity",
        "alaric",
        "fall of the western roman empire",
        "imperial restoration",
        "justinian",
        "odoacer",
        "christianization",
        "migration period",
        "eastern roman empire",
        "medieval rome",
        "early bulgars",
        "danelag",
        "visigothic spain",
        "arthur",
        "norman kingdom",
        "hephthalites",
        "nomadic empire",
        "kidarites",
        "alchon huns",
        "kushan",
        "rouran",
        "xianbei",
        "xiongnu",
        "yuezhi",
        "saka",
        "chionites",
        "bactria",
        "roman gaul",
        "sasanian",
        "persian empire",
        "latin empire",
        "danes",
        "beowulf",
        "roman civil wars",
        "crossing of the rhine",
        "rome imperial government",
        "barbarian rulers",
        "roman heritage",
        "roman emperors",
        "nicene christianity",
        "gothic kingdoms",
        "han china",
        "tang china",
        "roman usurpers",
        "magister militum",
        "late roman army",
        "picts",
        "dal riada",
        "gaelic tribes",
        "heptarchy",
        "bretwalda",
        "old english",
        "kingdom of east anglia",
        "germanic settlement",
        "geats",
        "venedi",
        "wends",
        "sclaveni",
        "antes",
        "norsemen",
        "balto-slavic",
        "proto-slavic",
        "slavic settlement",
        "getica",
        "jordanes",
        "sarmatae",
        "widsith",
        "samo",
        "volga bulgaria",
        "varangian",
        "old great bulgaria",
        "crusades",
        "saqaliba",
        "golden horde",
        "kara-khanid",
        "seljuk",
        "kara khitai",
        "great liao",
        "khitan",
        "kipchak",
        "qangli",
        "kangly",
        "cuman",
        "pechenegs",
        "karluks",
        "gokturks",
        "hazaras",
        "chagatai",
        "turco-mongol",
        "ilkhanate",
        "timurids",
        "jalayirid",
        "sultanate of rum",
        "turco-persian",
        "anatolian beyliks",
        "cilician armenia",
        "empire of trebizond",
        "yuan china",
        "hunnic empire",
        "vistula veneti",
        "karamanids",
        "aq qoyunlu",
        "mamluks",
        "qara qoyunlu",
        "teke",
        "aydin",
        "menteshe",
        "danishmend",
        "mengujekids",
        "saltukids",
        "kayi tribe",
        "ghaznavids",
        "great moravia",
        "kievan rus",
        "baltic slavic piracy",
        "rashidun caliphate",
        "abbasid",
        "muslim conquest of persia",
        "muslim conquest of the levant",
        "emperor heraclius",
        "muslim conquest of egypt",
        "muslim conquest of maghreb",
        "ghassanids",
        "lakhmids",
        "assyrian church",
        "nestorians",
        "paulicianism",
        "bogomilism",
        "tondrakians",
        "himyar",
        "lotharingia",
        "neustria",
        "austrasia",
        "kingdom of soissons",
        "ambrosius aurelianus",
        "gildas",
        "arthurian period",
        "de excidio et conquestu britanniae",
        "medieval brittany",
        "kingdom of galicia",
        "marcomanni",
        "marcus aurelius",
        "ricimer",
        "aetius",
        "majorian",
        "aegidius",
        "syagrius",
        "crisis of third century",
        "constantine the great",
        "catalaunian plains",
        "ripuarian franks",
        "salian franks",
        "ammianus marcellinus",
        "burgundy",
        "strategikon",
        "roxolani",
        "iazyges",
        "sabirs",
        "onoghurs",
        "utigurs",
        "kutrigurs",
        "akatziri",
        "barsils",
        "mishar tatars",
        "mordvins",
        "qaraqalpaqs",
        "nogai horde",
        "bashkirs",
        "tengrism",
        "uyghur khaganate",
        "yenisei kyrgyz",
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