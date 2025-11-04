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
            try:
                ### spróbuj odczytać błąd jako JSON (dla błędów 500)
                print(f"Odpowiedź serwera (JSON): {e.response.json()}")
            except requests.exceptions.JSONDecodeError:
                ### jeśli to nie JSON (jak błąd 429), wydrukuj zwykły tekst
                print(f"Odpowiedź serwera (TEXT): {e.response.text[:500]}") # ograniczenie do 500 znaków
        return []

# Główna część skryptu
if __name__ == "__main__":
    
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
    
    ARTICLES_PER_KEYWORD = 50
    
    all_articles_data = []
    
    print("Rozpoczynam zbieranie danych z CORE.ac.uk...")
    
    for keyword in KEYWORDS_TO_SEARCH:
        articles = get_core_articles(keyword, 
                                     page_size=ARTICLES_PER_KEYWORD, 
                                     api_key=CORE_API_KEY)
        
        all_articles_data.extend(articles)
        time.sleep(3)
    
    if all_articles_data:
        df = pd.DataFrame(all_articles_data)
        
        output_filename = "core_articles.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\nZapisano łącznie {len(df)} artykułów do pliku: {output_filename}")
        print("Przykładowe dane:")
        print(df.head())
    else:
        print("Nie udało się pobrać żadnych artykułów.")