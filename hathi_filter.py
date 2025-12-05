import pandas as pd
import csv
import re


keywords = [
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


INPUT_TSV = 'hathifiles20251201-26-1nw25t'
OUTPUT_CSV = 'hathi_articles.csv'
DESCRIPTION_MAX_LEN = 6000          # obcinanie długich opisów


# wczytuje bez headera po indeksach kolumn (pozycje z docs HathiFiles)
df = pd.read_csv(
    INPUT_TSV,
    sep='\t',
    header=None,                    # brak nagłówka
    usecols=[0,1,4,11,12,16,25],   # indeksy: htid=0, access=1, description=4, title=11, imprint=12, rights_date_used=16, author=25
    names=['htid', 'access', 'description', 'title', 'imprint', 'rights_date_used', 'author'],
    dtype=str,
    low_memory=False,
    encoding='utf-8'
)

# uzupełnia brakujące/puste wartości pustymi stringami (bez None/NaN)
df['title'] = df['title'].fillna('').astype(str)
df['author'] = df['author'].fillna('').astype(str)
df['imprint'] = df['imprint'].fillna('').astype(str)
df['rights_date_used'] = df['rights_date_used'].fillna('').astype(str)
df['description'] = df['description'].fillna('').astype(str)
df['htid'] = df['htid'].fillna('').astype(str)
df['access'] = df['access'].fillna('deny').astype(str)

# filtr: tylko full-view (public domain) i rekordy z tytułem
df = df[(df['access'] == 'allow') & (df['title'] != '') & (df['title'].notna())]

print(f"Wczytano {len(df)} rekordów po filtrze (tylko public domain z tytułem).")

total_records = 0

# nagłówki CSV
fieldnames = ['keyword', 'title', 'authors', 'publisher', 'published_date', 'description', 'hathi_id']

with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for kw in keywords:
        print(f"\nSzukam: '{kw}' ...")
        
        # filtrowanie po keyword w title OR author OR description OR imprint (case-insensitive)
        mask = (
            df['title'].str.contains(kw, case=False, na=False) |
            df['author'].str.contains(kw, case=False, na=False) |
            df['description'].str.contains(kw, case=False, na=False) |
            df['imprint'].str.contains(kw, case=False, na=False)
        )
        results = df[mask].copy()
        
        keyword_count = len(results)
        print(f" --> znaleziono {keyword_count} unikalnych rekordów dla '{kw}'")
        
        for _, row in results.iterrows():
            desc_raw = row['description']
            description = desc_raw[:DESCRIPTION_MAX_LEN] if len(desc_raw) > DESCRIPTION_MAX_LEN else desc_raw
            
            writer.writerow({
                'keyword': kw,
                'title': row['title'],
                'authors': row['author'],
                'publisher': row['imprint'],  ### publishing info (wydawca/miejsce/rok)
                'published_date': row['rights_date_used'],
                'description': description,
                'hathi_id': row['htid']
            })
            total_records += 1

print(f"\nZapisano {total_records} unikalnych rekordów do {OUTPUT_CSV}")