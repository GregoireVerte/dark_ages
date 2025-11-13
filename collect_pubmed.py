import requests
import time
import pandas as pd
from tqdm import tqdm
import xml.etree.ElementTree as ET
from urllib.parse import quote
import json


#### konfiguracja
API_KEY = "4f6209746fb37e733b2b0ae02e2b3b832008"  # z NCBI
EMAIL = "trevor84wppl@gmail.com"  # wymagane przez NCBI

BASE_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
SEARCH_URL = BASE_URL + "esearch.fcgi"
FETCH_URL = BASE_URL + "efetch.fcgi"


# lista słów
KEYWORDS = [
    "roman lead poisoning",
    "bioarchaeology fall of rome",
    "migration genetics late antiquity",
    "health late roman empire",
    "zooarchaeology roman period",
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


# max liczba wyników na zapytanie
MAX_PER_QUERY = 100


# opóźnienie między zapytaniami --> NCBI wymaga <1 req/s bez klucza, <3 z kluczem
DELAY = 0.34 if API_KEY else 1.1


### funkcje
def search_pubmed(term, retmax=100):
    params = {
        "db": "pubmed",
        "term": term,
        "retmax": retmax,
        "retmode": "json",
        "sort": "pub date",
        "email": EMAIL,
    }
    if API_KEY:
        params["api_key"] = API_KEY

    response = requests.get(SEARCH_URL, params=params)
    if response.status_code != 200:
        print(f"Błąd wyszukiwania: {response.status_code}, {response.text}")
        return []

    data = response.json()
    return data["esearchresult"].get("idlist", [])


def fetch_details(pmids):
    if not pmids:
        return []

    id_str = ",".join(pmids)
    params = {
        "db": "pubmed",
        "id": id_str,
        "retmode": "xml",
        "rettype": "abstract",
        "email": EMAIL,
    }
    if API_KEY:
        params["api_key"] = API_KEY

    response = requests.get(FETCH_URL, params=params)
    if response.status_code != 200:
        print(f"Błąd pobierania: {response.status_code}")
        return []

    root = ET.fromstring(response.content)
    articles = []

    for article in root.findall(".//PubmedArticle"):
        try:
            pmid = article.find(".//PMID").text
            title = article.find(".//ArticleTitle")
            title = title.text if title is not None else "No title"

            # autorzy
            authors = []
            for author in article.findall(".//Author"):
                last = author.find("LastName")
                fore = author.find("ForeName")
                if last is not None:
                    name = (fore.text + " " if fore is not None else "") + last.text
                    authors.append(name)
            authors_str = "; ".join(authors) if authors else "No authors"

            # rok
            pubdate = article.find(".//PubDate/Year") or article.find(".//ArticleDate/Year")
            year = pubdate.text if pubdate is not None else "Unknown"

            # DOI
            doi_elem = article.find(".//ELocationID[@EIdType='doi']")
            doi = doi_elem.text if doi_elem is not None else ""

            # abstrakt
            abstract_parts = article.findall(".//Abstract/AbstractText")
            abstract = " ".join([p.text for p in abstract_parts if p.text]) if abstract_parts else "No abstract"

            articles.append({
                "PMID": pmid,
                "Title": title,
                "Authors": authors_str,
                "Year": year,
                "DOI": doi,
                "Abstract": abstract,
            })
        except Exception as e:
            print(f"Błąd parsowania artykułu {pmid}: {e}")
            continue

    return articles


## main loop
all_results = []


print(f"Przetwarzanie {len(KEYWORDS)} słów kluczowych...")

for keyword in tqdm(KEYWORDS, desc="Słowa kluczowe"):
    pmids = search_pubmed(keyword, retmax=MAX_PER_QUERY)
    
    if pmids:
        details = fetch_details(pmids)
        for d in details:
            d["Query"] = keyword  # dodać info z jakiego hasła
        all_results.extend(details)
    
    time.sleep(DELAY)


## zapis do csv
df = pd.DataFrame(all_results)
df = df.drop_duplicates(subset=["PMID"])  # usuwanie duplikatów bo hasła się nakładają


output_file = "pubmed_articles.csv"
df.to_csv(output_file, index=False, encoding="utf-8")


print(f"\nZapisano {len(df)} unikalnych artykułów do {output_file}")
print(f"Przykładowe tytuły:")
for title in df["Title"].head(3):
    print(f"  • {title[:100]}{'...' if len(title)>100 else ''}")