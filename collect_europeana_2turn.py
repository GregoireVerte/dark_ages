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
    "germanic tribe",
    "germanic kingdom",
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
    "anglo-saxon history",
    "saxons",
    "angles in britannia",
    "jutes",
    "early slavs",
    "romano-britons",
    "romano-british",
	"huns history",
	"history huns",
	"white huns",
	"black huns",
    "huns",
    "goths",
    "ostrogoths",
    "visigoths",
    "history franks",
	"franks history",
    "franks",
    "alemanni history",
	"history alemanni",
    "vandals history",
    "gepids",
    "lombards",
    "attila",
    "thuringians",
    "rugians",
    "sciri",
    "herules",
    "bavarians history",
    "avars history",
    "avars",
    "burgundians",
    "viking",
    "germanic paganism",
    "slavic paganism",
    "medieval celts",
    "norse paganism",
    "foederati",
    "roman britannia",
    "suebi history",
    "suebi",
    "alans history",
	"history alans",
    "alans",
    "frisians",
    "sarmatians",
    "period 400-1000",
    "late roman empire",
    "barbarian kingdoms",
    "arianism christianity",
    "magyars",
    "khazar",
	"khazars",
    "umayyad",
    "charlemagne",
    "clovis",
    "theodoric the great",
    "medieval constantinople",
    "gothic war",
    "barbarian invasion",
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
	"danelaw",
    "visigothic spain",
    "king arthur history",
    "norman kingdom",
	"hephthalite",
	"ephthalite",
	"ebodalo",
    "nomadic empire",
    "kidarites",
    "alchon huns",
    "kushan",
    "rouran",
    "xianbei",
    "xiongnu",
    "yuezhi",
    "saka people",
    "chionites",
    "bactria",
    "greco-bactria",
    "roman gaul",
    "sasanian",
    "persian empire",
    "latin empire",
    "early danes",
    "beowulf",
    "roman civil wars",
    "crossing of the rhine",
    "rome imperial government",
    "barbarian rulers",
    "roman heritage",
    "roman emperors",
    "nicene christianity",
    "gothic kingdoms",
    "han dynasty",
    "tang china",
    "roman usurpers",
    "magister militum",
    "late roman army",
    "picts",
    "dal riada",
	"dal riata",
	"dalriada",
    "gaelic tribe",
    "heptarchy",
    "bretwalda",
    "old english period",
    "kingdom of east anglia",
    "germanic settlement",
    "geats",
	"vistula venedi",
    "venedi slavs",
    "wends history",
    "sclaveni",
    "antes slavs",
	"slavic tribe",
    "norsemen",
    "balto-slavic",
    "proto-slavic",
    "slavic settlement",
    "getica",
    "jordanes",
    "sarmatae",
    "widsith",
    "king samo",
	"samo empire",
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
    "cumans history",
    "cumans",
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
    "teke beylik",
    "aydin beylik",
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
    "burgundy history",
	"history burgundy",
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
        "qf": 'TYPE:TEXT AND (YEAR:[250 TO 1400] OR YEAR:[1950 TO 2026])',          # tu filtry
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
        with open("europeana_articles_2turn.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(all_rows)
        print(f"\nZapisano {len(all_rows)} rekordów do europeana_articles_2turn.csv")
    else:
        print("Nie pobrano żadnych rekordów.")

if __name__ == "__main__":
    main()