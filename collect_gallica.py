import time
import csv
import requests
from xml.etree import ElementTree as ET
from urllib.parse import quote
import datetime

# konfiguracja
SRU_BASE = "https://gallica.bnf.fr/SRU?operation=searchRetrieve&version=1.2"
NS = {
    "srw": "http://www.loc.gov/zing/srw/",
    "dc": "http://purl.org/dc/elements/1.1/",
    "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
    "diag": "http://www.loc.gov/zing/srw/diagnostic/"
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'pl,en-US;q=0.7,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://gallica.bnf.fr/',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

KEYWORDS = [
    ## francuskie wersje
    "carolingien",
    #"époque carolingienne",
    #"chute de Rome",
    "fin de l'Empire romain",
    ## angielskie wersje (mogą działać słabo)
    "fall of rome",
    "carolingian",
]

# ograniczenia bezpieczeństwa
MAX_RECORDS_PER_REQUEST = 80      # ile rekordów w jednym zapytaniu SRU
MAX_TOTAL_PER_KEYWORD = 2500       # max ile rekordów zbiera dla jednego słowa kluczowego
SLEEP_BETWEEN_REQUESTS = 5.0

def sru_search(keyword, start_record=1, max_records=MAX_RECORDS_PER_REQUEST):
    """
    Wykonuje jedno zapytanie SRU i zwraca (numberOfRecords, records_list, next_start)
    """
    cql_query = f'text adj "{keyword}"'  # podstawowe wyszukiwanie we wszystkich polach tekstowych

    params = {
        "query": cql_query,
        "maximumRecords": str(max_records),
        "startRecord": str(start_record),
        "recordSchema": "dc"           # Dublin Core – najprostszy i najczęściej wystarczający
    }

    try:
        time.sleep(2.5)  # dodatkowe opóźnienie przed każdym zapytaniem
        r = requests.get(SRU_BASE, params=params, headers=headers, timeout=12)
        r.raise_for_status()
        
        root = ET.fromstring(r.content)
        
        # liczba wszystkich pasujących rekordów w całym zbiorze
        total = int(root.find(".//srw:numberOfRecords", NS).text or 0)
        
        # lista rekordów z tej strony
        records = []
        for rec in root.findall(".//srw:record", NS):
            data = rec.find(".//srw:recordData", NS)
            if data is None:
                continue
                
            oai_dc = data.find("oai_dc:dc", {"oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/"})
            if oai_dc is None:
                continue
                
            def get_text(field):
                el = oai_dc.find(f"dc:{field}", NS)
                return el.text.strip() if el is not None and el.text else ""
            
            def get_multi(field):
                return "; ".join([e.text.strip() for e in oai_dc.findall(f"dc:{field}", NS) if e.text])
            
            record = {
                "title": get_text("title"),
                "creator": get_multi("creator"),
                "abstract": get_text("description"),
                "date": get_text("date"),
                "subject": get_multi("subject"),
                "publisher": get_text("publisher"),
                "id": get_text("identifier"),
            }
            records.append(record)
        
        next_start = start_record + len(records)
        return total, records, next_start
    
    except Exception as e:
        print(f"    Błąd zapytania SRU (keyword: {keyword}, start={start_record}): {e}")
        return 0, [], start_record


def main():
    results = []
    
    for keyword in KEYWORDS:
        print(f"\n--- Przeszukuję słowo: '{keyword}' ---")
        
        start = 1
        collected_for_keyword = 0
        
        while True:
            print(f"  Strona startRecord={start}... ({datetime.datetime.now().strftime('%H:%M:%S')})", end=" ", flush=True)
            
            total_possible, page_records, next_start = sru_search(keyword, start)
            
            if not page_records:
                print("brak wyników lub błąd")
                break
                
            added_this_page = 0
            for rec in page_records:
                if any(keyword.lower() in v.lower() for v in rec.values() if v):  # dodatkowy, lokalny filtr
                    rec["keyword"] = keyword
                    results.append(rec)
                    added_this_page += 1
                    collected_for_keyword += 1
            
            print(f"znaleziono {added_this_page} pasujących / pobrano {len(page_records)}")
            
            if collected_for_keyword >= MAX_TOTAL_PER_KEYWORD:
                print(f"  -> Osiągnięto limit {MAX_TOTAL_PER_KEYWORD} rekordów dla '{keyword}' – przerywam")
                break
                
            if next_start > total_possible:
                print(f"  -> Koniec wyników (znaleziono łącznie {collected_for_keyword})")
                break
                
            start = next_start
            time.sleep(SLEEP_BETWEEN_REQUESTS)
    
    # zapis do CSV
    if results:
        keys = ["keyword", "title", "creator", "abstract", "date", "subject", "publisher", "id"]
        with open("gallica_articles.csv", "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(results)
        print(f"\nZapisano {len(results)} rekordów -> gallica_articles.csv")
    else:
        print("\nNie udało się znaleźć żadnych pasujących rekordów.")


if __name__ == "__main__":
    start_time = datetime.datetime.now()
    main()
    print(f"Czas wykonania: {datetime.datetime.now() - start_time}")
