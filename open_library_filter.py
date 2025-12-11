import pandas as pd
import gzip
import json
import time
from typing import List, Dict
from tqdm import tqdm


## lista keywords
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
    "anglo-saxon history",
    "saxons",
    "angles in britannia",
    "jutes",
    "early slavs",
    "romano-britons",
    "romano-british",
    "history huns",
    "huns history",
    "black huns",
    "white huns",
    "goths",
    "ostrogoths",
    "visigoths",
    "history franks",
    "franks history",
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
    "history avars",
    "burgundians",
    "viking",
    "germanic paganism",
    "slavic paganism",
    "medieval celts",
    "norse paganism",
    "foederati",
    "roman britannia",
    "suebi history",
    "alans history",
    "history alans",
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
    "danelaw",
    "visigothic spain",
    "king arthur history",
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
    "saka people",
    "chionites",
    "bactria",
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
    "gaelic tribes",
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


DUMP_FILE = "ol_dump_works_2025-11-30.txt"
OUTPUT_CSV = "open_library_articles.csv"
CHUNK_SIZE = 500000  ### przetwarzanie po chunkach by nie zjeść RAM


def extract_record(json_str: str) -> Dict:
    """Wyciągnij kluczowe pola z JSON."""
    try:
        data = json.loads(json_str)
        if data.get('type', {}).get('key') != '/type/work':
            return None

        title = data.get('title', '')
        #### ostry filtr –-> cała fraza musi być w tytule lub w którymś subject
        matched_keyword = None
        title_lower = title.lower()
        subjects_lower = [s.lower() for s in data.get('subjects', [])]
        full_text = title_lower + " " + " ".join(subjects_lower)

        for kw in KEYWORDS:
            phrase = kw.lower()
            if phrase in full_text:  ##### cała fraza jako ciąg znaków
                matched_keyword = kw
                break

        if not matched_keyword:
            return None

        ## description
        desc = data.get('description', '')
        if isinstance(desc, dict):
            desc = desc.get('value', '')
        desc = str(desc).replace('\n', ' ').replace('\r', ' ')[:4000]  #### obcinanie i czyszczenie

        return {
            'matched_keyword': matched_keyword,
            'title': title,
            'authors': ', '.join([a.get('name', '') if isinstance(a, dict) and 'name' in a else a.get('author', {}).get('key', '').split('/')[-1] if isinstance(a.get('author'), dict) else '' for a in data.get('authors', [])]),
            'subjects': ' | '.join(data.get('subjects', [])[:50]),  ### separator inny niż przecinek
            'description': desc,
            'publish_year': data.get('first_publish_year') or data.get('created', {}).get('value', '')[:4]
        }
    except:
        return None


def main():
    all_records = []
    total_lines = 40560519  ### liczba linii w works dump
    with tqdm(total=total_lines, desc="Przetwarzanie dumpa", unit="linii") as pbar:
        ### wczytanie TSV po chunkach (kolumny: type, key, revision, last_modified, JSON)
        for chunk in pd.read_csv(DUMP_FILE, sep='\t', header=None, names=['type', 'key', 'revision', 'last_modified', 'JSON'], chunksize=CHUNK_SIZE, low_memory=False):
            for _, row in chunk.iterrows():
                record = extract_record(row['JSON'])
                if record:
                    all_records.append(record)
                pbar.update(1)  ### aktualizuje co linię (wolniej niż co chunk)
    
    if all_records:
        df = pd.DataFrame(all_records)
        df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        print(f"Zapisano {len(all_records)} rekordów z description do {OUTPUT_CSV}")
        print(f"Przykładowe description: {df['description'].dropna().head(1).values[0] if not df['description'].dropna().empty else 'Brak'}")
    else:
        print("Brak pasujących rekordów – sprawdź keywords.")

if __name__ == "__main__":
    main()