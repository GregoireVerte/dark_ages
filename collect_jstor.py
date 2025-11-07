import requests
import xml.etree.ElementTree as ET
import csv
import time

# keywords
keywords = [
    "vikings",
    "carolingian",
]

# parametry SRU
base_url = "https://dfr.jstor.org/sru/"
params_base = {
    "operation": "searchRetrieve",
    "version": "1.2",
    "maximumRecords": "100",  # max na zapytanie
    "startRecord": "1"
}

# plik wyjściowy
with open("jstor_articles.csv", "w", newline="", encoding="utf-8") as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(["keyword", "title", "abstract", "creator", "date", "journal", "doi"])

    for keyword in keywords:
        print(f"Szukam: {keyword}")
        query = f'(dc.title any "{keyword}" OR dc.description any "{keyword}" OR dc.subject any "{keyword}")'
        
        params = params_base.copy()
        params["query"] = query
        
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Błąd dla {keyword}: {response.status_code}")
            continue
            
        root = ET.fromstring(response.content)
        
        # namespace
        ns = {"srw": "http://www.loc.gov/zing/srw/", "dc": "http://purl.org/dc/elements/1.1/"}
        
        records = root.findall(".//srw:record", ns)
        print(f"Znaleziono {len(records)} rekordów dla {keyword}")
        
        for record in records:
            data = record.find("srw:recordData", ns).find("dc:record", ns)
            
            title = " ".join(t.text for t in data.findall("dc:title", ns) if t.text)
            abstract = " ".join(d.text for d in data.findall("dc:description", ns) if d.text)
            creator = "; ".join(c.text for c in data.findall("dc:creator", ns) if c.text)
            date = data.find("dc:date", ns).text if data.find("dc:date", ns) is not None else ""
            journal = data.find("dc:source", ns).text if data.find("dc:source", ns) is not None else ""
            doi = data.find("dc:identifier", ns).text if data.find("dc:identifier", ns) is not None else ""
            
            writer.writerow([keyword, title, abstract, creator, date, journal, doi])
        
        time.sleep(3)  # żeby nie obciążać serwera

print("Gotowe! Dane w jstor_articles.csv")