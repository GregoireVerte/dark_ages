import requests
import time
import csv
import json
from urllib.parse import quote_plus
import os

## konfiguracja
API_KEY = "API_KEY"


OUTPUT_CSV = "google_books_articles.csv"
MAX_RESULTS = 40      # max na jedno zapytanie (API pozwala max 40)
PAUSE_BETWEEN = 3.0   # sekundy między zapytaniami

## słowa kluczowe
KEYWORDS = [
    "ayyubid",
    "fall of rome",
    "sack of rome 410",
    "sack of rome 455",
    "battle of chalons",
    "theoderic",
    "justinian reconquest",
    "plague of justinian",
    "maurice strategikon",
    "byzantine empire 5th century",
    "western roman empire collapse",
    "romanitas",
    "transformation of the roman world",
    "pirenne thesis",
    "romano-germanic kingdoms",
    "post-roman",
    "sub-roman britain",
    "anglo-saxon settlement",
    "frisian migration",
    "slavic expansion",
    "avar khaganate",
    "lombard italy",
    "cassiodorus",
    "jordanes",
    "gregory of tours",
    "isidore of seville",
    "roman law codification",
    "theodosian code",
    "salic law",
    "gothic spain",
    "alan kingdom",
    "vandal africa",
    "roman identity 5th-7th",
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

# filtrowanie gdzie tylko książki z opisem i po angielsku
LANGUAGE_FILTER = "en"

# funkcje
def google_books_search(query, start_index=0):
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "maxResults": MAX_RESULTS,
        "startIndex": start_index,
        "printType": "books",
        "orderBy": "relevance",
    }
    if LANGUAGE_FILTER:
        params["langRestrict"] = LANGUAGE_FILTER
    if API_KEY:
        params["key"] = API_KEY

    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.RequestException as e:
        print(f"Błąd zapytania: {e}")
        return None

def extract_metadata(item):
    v = item.get("volumeInfo", {})
    title = v.get("title", "")
    subtitle = v.get("subtitle", "")
    full_title = f"{title} {subtitle}".strip() if subtitle else title
    
    authors = ", ".join(v.get("authors", [])) if v.get("authors") else ""
    publisher = v.get("publisher", "")
    published_date = v.get("publishedDate", "")  #### może być sam rok albo YYYY-MM-DD
    description = v.get("description", "")
    page_count = v.get("pageCount", "")
    categories = ", ".join(v.get("categories", [])) if v.get("categories") else ""
    language = v.get("language", "")
    isbn_list = v.get("industryIdentifiers", [])
    isbn = ""
    if isbn_list:
        for i in isbn_list:
            if i["type"] in ["ISBN_13", "ISBN_10"]:
                isbn = i["identifier"]
                break

    return {
        "title": full_title,
        "authors": authors,
        "publisher": publisher,
        "publishedDate": published_date,
        "description": description,
        "pageCount": page_count,
        "categories": categories,
        "language": language,
        "isbn": isbn
    }

# main loop
all_books = []
seen_books = set()

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
    fieldnames = ["title", "authors", "publisher", "publishedDate", 
                  "description", "pageCount", "categories", "language", "isbn", "source_query"]
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for keyword in KEYWORDS:
        print(f"\nSzukam: {keyword}")
        start_index = 0
        while True:
            data = google_books_search(keyword, start_index)
            if not data or "items" not in data:
                print("Brak wyników lub błąd API")
                break

            items = data["items"]
            if not items:
                break

            for item in items:
                meta = extract_metadata(item)
                if meta["description"] and len(meta["description"]) > 100: ### powyżej 100 znaków żeby nie śmiecić
                    row = meta
                    row["source_query"] = keyword

                    # wstępne usuwanie duplikatów
                    unique_key = meta["isbn"] or (meta["title"] + "|" + meta["authors"]).lower()
                    if unique_key not in seen_books:
                        seen_books.add(unique_key)
                        writer.writerow(row)
                        all_books.append(row)
                        print(f"  + {meta['title'][:70]:70} | {meta['publishedDate']} | {meta['authors'][:50]}")

            # jeśli mniej niż maxResults to koniec wyników dla tego query
            if len(items) < MAX_RESULTS:
                break

            start_index += MAX_RESULTS
            time.sleep(PAUSE_BETWEEN)

        time.sleep(PAUSE_BETWEEN + 1)  # przerwa między słowami kluczowymi

print(f"\nZebrano {len(all_books)} książek z opisami.")
print(f"Zapisano do pliku: {OUTPUT_CSV}")