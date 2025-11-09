import json
import pandas as pd
from pathlib import Path

def safe_get_text(record, field_name):
    """
    Funkcja do wyciągania tekstu z pola,
    które może być stringiem lub listą stringów
    (łączy wszystkie elementy listy).
    """
    data = record.get(field_name, "")
    
    if isinstance(data, list):
        # jeśli to lista łączy wszystkie jej elementy w jeden string # oddzielając je spacją
        return " ".join(str(item) for item in data)
    elif isinstance(data, str):
        # jeśli to string po prostu go zwraca
        return data
    # w przeciwnym razie zwraca pusty string
    return ""

## Ścieżka do pliku
file_path = Path("jstor_metadata.jsonl")

keywords = [
    "fall of rome",
    "romulus augustulus",
    "comitatus",
    "post-roman",
    "vandal kingdom",
    "justinian reconquest",
    "byzantine early",
    "transformation of the roman world",
    "pirenne thesis",
    "laeti",
    "limitanei",
    "sack of rome 410",
    "battle of chalons",
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

## konwersja keywords na małe litery raz na początku (dla wydajności)
keywords_lower = [kw.lower() for kw in keywords]

## tylko te rekordy wczytywane które pasują
matches = []


with open(file_path, 'r', encoding='utf-8') as f:
    for i, line in enumerate(f):
        if i % 500_000 == 0 and i > 0: # drukuj co 500k ale nie na starcie
            print(f"Przetworzono {i:,} rekordów... Znaleziono dotąd {len(matches):,} pasujących.")
            
        try:
            record = json.loads(line.strip())
        except Exception as e:
            # print(f"Błąd w linii {i}: {e}") # odkomentuj, jeśli chcesz widzieć błędy JSON
            continue
        
        title_text = safe_get_text(record, "title")
        abstract_text = safe_get_text(record, "abstract")
        subject_text = " ".join(record.get("subject", []) or [])
        
        # szukanie w title + abstract + subject
        text_to_search = f"{title_text} {abstract_text} {subject_text}".lower()
        
        # sprawdzenie które keywordy pasują
        matched_kws = [kw for kw in keywords_lower if kw in text_to_search]
        
        # jeśli lista nie jest pusta to znaczy że jest trafienie
        if matched_kws:
            
            # pole "creator" to lista, więc łączymy je
            creator_list = record.get("creator", [])
            creator_text = "; ".join(creator_list) if creator_list else ""
            
            matches.append({
                "keyword": ", ".join(matched_kws), 
                "title": title_text,
                "abstract": abstract_text,
                "creator": creator_text,
                "date": safe_get_text(record, "date"),
                "journal": safe_get_text(record, "is_part_of"),
                "doi": safe_get_text(record, "doi"),
                "language": safe_get_text(record, "language")
            })

print("\nZakończono przetwarzanie pliku.")

#### zapis do CSV
if matches:
    print(f"Rozpoczynam zapisywanie {len(matches):,} rekordów do pliku CSV...")
    df = pd.DataFrame(matches)

    df.to_csv("jstor_articles.csv", index=False, encoding="utf-8")
    print(f"\nZnaleziono i zapisano {len(df):,} rekordów → jstor_articles.csv")
    print("Przykłady:")
    print(df.head(10)[["title", "date", "keyword"]])
else:
    print("Nie znaleziono żadnych pasujących rekordów.")