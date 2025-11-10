import requests
import time
import xml.etree.ElementTree as ET
import csv
from urllib.parse import quote_plus
from tqdm import tqdm


# keywords
keywords = [
    "late antiquity", 
    "migration period",
    "dark ages",
    "anglo-saxon",
    "early middle ages",
    "viking",
]


# konfiguracja
BASE_URL = "http://export.arxiv.org/api/query"
MAX_RESULTS_PER_QUERY = 2000   # maksymalnie 2000 na jedno zapytanie
SLEEP_BETWEEN_QUERIES = 3.0    # 3 sekundy – limity arXiv
OUTPUT_FILE = "arxiv_articles.csv"


# funkcje
def search_arxiv(keyword, start=0):
    """Jedno zapytanie do API arXiv"""
    search_query = f'all:"{keyword}"'
    params = {
        'search_query': search_query,
        'start': start,
        'max_results': MAX_RESULTS_PER_QUERY,
        'sortBy': 'submittedDate',
        'sortOrder': 'descending'
    }
    url = f"{BASE_URL}?{requests.compat.urlencode(params, safe=':')}"
    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        print(f"Błąd {response.status_code} dla: {keyword}")
        return None
    return response.text

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
                time.sleep(SLEEP_BETWEEN_QUERIES)  ### grzecznie 3 s

        #### przerwa między słowami kluczowymi
        time.sleep(SLEEP_BETWEEN_QUERIES)

print(f"\nZnaleziono {len(all_results)} artykułów.")
print(f"Wyniki zapisane do: {OUTPUT_FILE}")