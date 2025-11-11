import requests
import time
import xml.etree.ElementTree as ET
import csv
from urllib.parse import quote_plus
from tqdm import tqdm
import random

# keywords
keywords = [
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


# konfiguracja
BASE_URL = "http://export.arxiv.org/api/query"
MAX_RESULTS_PER_QUERY = 2000   # maksymalnie 2000 na jedno zapytanie
SLEEP_BETWEEN_QUERIES = 12.0    # 12 sekund – limity arXiv
OUTPUT_FILE = "arxiv_articles.csv"


# funkcje
def search_arxiv(keyword, start=0, retry=0):
    search_query = f'all:"{keyword}"'
    params = {
        'search_query': search_query,
        'start': start,
        'max_results': MAX_RESULTS_PER_QUERY,
        'sortBy': 'submittedDate',
        'sortOrder': 'descending'
    }
    url = f"{BASE_URL}?{requests.compat.urlencode(params, safe=':')}"
    
    for attempt in range(5):  # max 5 prób
        try:
            response = requests.get(url, timeout=60)  # dłuższy timeout
            if response.status_code == 429 or response.status_code >= 500:
                wait = (2 ** attempt) + random.random() * 5
                print(f"429/5xx dla '{keyword}' – czekam {wait:.1f}s (proba {attempt+1})")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.text
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait = (2 ** attempt) + random.random() * 10
            print(f"Timeout dla '{keyword}' – czekam {wait:.1f}s...")
            time.sleep(wait)
    print(f"Błąd dla: {keyword}")
    return None

def parse_entry(entry):
    """Wyciąga dane z jednego <entry> w XML"""
    ns = {'atom': 'http://www.w3.org/2005/Atom'}
    title = entry.find('atom:title', ns).text or ''
    title = title.replace('\n', ' ').strip()
    
    authors = [author.find('atom:name', ns).text for author in entry.findall('atom:author', ns)]
    
    summary = (entry.find('atom:summary', ns).text or '').replace('\n', ' ').replace('\r', ' ').strip()
    
    published = entry.find('atom:published', ns).text[:10]  # tylko YYYY-MM-DD
    
    arxiv_id = entry.find('atom:id', ns).text.split('/')[-1].replace('abs/', '')
    pdf_url = f"https://arxiv.org/pdf/{arxiv_id}.pdf"
    
    categories = [cat.get('term') for cat in entry.findall('atom:category', ns)]
    
    link_abs = entry.find("atom:link[@title='doi']", ns)
    doi = link_abs.get('href').split('doi.org/')[-1] if link_abs is not None else ''
    
    return {
        'keyword': keyword,
        'title': title,
        'authors': ' | '.join(authors),
        'published': published,
        'summary': summary,
        'arxiv_id': arxiv_id,
        'pdf_url': pdf_url,
        'doi': doi,
        'categories': ', '.join(categories)
    }


# main loop
all_results = []

print(f"Startuję wyszukiwanie dla {len(keywords)} słów kluczowych...")
with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['keyword', 'title', 'authors', 'published', 'summary',
                  'arxiv_id', 'pdf_url', 'doi', 'categories']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()
    
    for keyword in tqdm(keywords, desc="Postęp"):
        start = 0
        total_results = 1  # sztuczka żeby wejść do pętli
        seen_ids = set()   # unikanie duplikatów przy paginacji
        
        while start < total_results:
            xml_data = search_arxiv(keyword, start)
            if not xml_data:
                break
                
            root = ET.fromstring(xml_data)
            ns = {'atom': 'http://www.w3.org/2005/Atom'}
            
            # ile wszystkich wyników
            total_results = int(root.find('{http://a9.com/-/spec/opensearch/1.1/}totalResults').text)
            entries = root.findall('atom:entry', ns)
            
            for entry in entries:
                arxiv_id = entry.find('atom:id', ns).text.split('/')[-1].replace('abs/', '')
                if arxiv_id in seen_ids:
                    continue
                seen_ids.add(arxiv_id)
                
                result = parse_entry(entry)
                all_results.append(result)
                writer.writerow(result)
                csvfile.flush()  # zapis na bieżąco
            
            start += len(entries)
            if start < total_results:
                time.sleep(SLEEP_BETWEEN_QUERIES + random.uniform(0, 5))

        #### przerwa między słowami kluczowymi
        time.sleep(SLEEP_BETWEEN_QUERIES + random.uniform(0, 5))

print(f"\nZnaleziono {len(all_results)} artykułów.")
print(f"Wyniki zapisane do: {OUTPUT_FILE}")