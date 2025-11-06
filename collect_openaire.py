import requests
import pandas as pd
import time
from datetime import datetime

def get_openaire_articles(keywords, page_size=25, max_pages=2, open_access_only=True):
    """
    Pobiera artykuły z OpenAIRE Search API (używając 'keywords' zamiast 'query').
    """
    print(f"Rozpoczynam pobieranie dla słów kluczowych (Search API): '{keywords}'...")
    
    # Poprawny endpoint
    base_url = "https://api.openaire.eu/search/publications"
    
    all_records = []
    current_page = 1
    
    for page in range(1, max_pages + 1):
        params = {
            "keywords": keywords,  ### Zmienione z 'query'
            "size": page_size,
            "page": current_page,  ### Paginacja przez page (1-based)
        }
        if open_access_only:
            params["openAccessColor"] = "gold"  # tylko otwarte publikacje, pełne Open Access (bezpieczne prawnie)
            params["format"] = "json" # odpowiedź w JSON (łatwiejsze parsowanie)
        
        print(f"  > Wysyłane parametry: {params}")
        
        try:
            response = requests.get(base_url, params=params, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
            response.raise_for_status() 
            
            data = response.json()
            # Debug: Print fragmentu odpowiedzi
            print(f"  > Przykładowy fragment JSON: {str(data)[:300].replace(chr(10), ' ').replace(chr(9), ' ')}...")
            
            # poprawne wyciąganie rekordów z JSON
            results = data.get('response', {}).get('results', {})
            records = results.get('record', []) if results is not None else []
            
            if not records:
                print(f"  > Brak rekordów.")
                break
                
            print(f"  > Znaleziono {len(records)} rekordów na stronie {page}.")
                
            for record in records:
                try:
                    result = record.get('result', [{}])[0]
                    oaf_result = result.get('metadata', {}).get('oaf:entity', {}).get('oaf:result', {})

                    # Tytuł
                    title = ''
                    title_list = oaf_result.get('title', [])
                    if title_list and isinstance(title_list, list):
                        first = title_list[0]
                        title = first if isinstance(first, str) else (first.get('$', '') if isinstance(first, dict) else '')
                    
                    # Abstrakt (description → często string lub dict)
                    abstract = ''
                    desc = oaf_result.get('description', '')
                    if isinstance(desc, str):
                        abstract = desc
                    elif isinstance(desc, dict):
                        abstract = desc.get('$', '')

                    # Fallback na abstract (rzadko)
                    if not abstract:
                        abs_list = oaf_result.get('abstract', [])
                        if abs_list and isinstance(abs_list, list):
                            first = abs_list[0]
                            abstract = first if isinstance(first, str) else (first.get('$', '') if isinstance(first, dict) else '')
                    
                    # Rok
                    pub_date = oaf_result.get('dateofacceptance', '') or oaf_result.get('relevantdate', [{}])[0].get('$', '') if oaf_result.get('relevantdate') else ''
                    year = pub_date[:4] if pub_date and isinstance(pub_date, str) else ''
                    
                    # Licencja
                    access = oaf_result.get('bestaccessright', {})
                    license_name = access.get('@classid', '') if isinstance(access, dict) else ''
                    
                    # Link (z instance → webresource.url)
                    link = ''
                    instances = oaf_result.get('children', {}).get('instance', [])
                    if instances and isinstance(instances, list) and len(instances) > 0:
                        webresources = instances[0].get('webresource', [])
                        if webresources and isinstance(webresources, list) and len(webresources) > 0:
                            url_obj = webresources[0].get('url', {})
                            link = url_obj if isinstance(url_obj, str) else (url_obj.get('$', '') if isinstance(url_obj, dict) else '')

                    # Fallback: DOI
                    if not link:
                        pid_list = oaf_result.get('pid', [])
                        for pid in pid_list:
                            if isinstance(pid, dict) and pid.get('@classid') == 'doi':
                                doi = pid.get('$', '')
                                if doi:
                                    link = f"https://doi.org/{doi}"
                                    break
                    
                    # Journal title
                    source = oaf_result.get('source', '')
                    journal_title = ''
                    if isinstance(source, str):
                        journal_title = source
                    elif isinstance(source, list) and len(source) > 0:
                        journal_title = source[0] if isinstance(source[0], str) else (source[0].get('$', '') if isinstance(source[0], dict) else '')
                    
                    # dodaje tylko jesli jest abstract
                    if abstract and abstract.strip():
                        all_records.append({
                            "title": title.strip() if title else "",
                            "abstract": abstract.strip(),
                            "keywords": keywords,
                            "year": year,
                            "journal_title": journal_title,
                            "license": license_name,
                            "link": link or "",
                            "query": keywords
                        })

                except Exception as e:
                    print(f"  > Błąd parsowania rekordu: {e}")
                    continue
            
            current_page += 1
            
        except requests.exceptions.RequestException as e:
            print(f"Błąd podczas zapytania dla '{keywords}' (strona {page}): {e}")
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_data = e.response.json()
                    print(f"Odpowiedź serwera (JSON): {error_data}")
                except requests.exceptions.JSONDecodeError:
                    print(f"Odpowiedź serwera (TEXT): {e.response.text[:500]}")
            break
        
        # przerwa między stronami
        time.sleep(2)
    
    print(f"Pobrano {len(all_records)} artykułów z abstraktami dla słów kluczowych: '{keywords}'.")
    return all_records

# Główna część skryptu
if __name__ == "__main__":
    
    KEYWORDS_TO_SEARCH = [
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
    
    ARTICLES_PER_KEYWORD = 50
    MAX_PAGES_PER_KEYWORD = 2  # do 100 wyników na keyword
    
    all_articles_data = []
    
    print("Rozpoczynam zbieranie danych z OpenAIRE (Search API)...")
    
    for keyword in KEYWORDS_TO_SEARCH:
        articles = get_openaire_articles(keyword, page_size=ARTICLES_PER_KEYWORD, max_pages=MAX_PAGES_PER_KEYWORD)
        all_articles_data.extend(articles)
        
        # przerwa między keywordami
        time.sleep(3) 
    
    if all_articles_data:
        df = pd.DataFrame(all_articles_data)
        
        ## output_filename = f"openaire_articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_filename = "openaire_articles.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8')
        
        print(f"\nGotowe! Zapisano łącznie {len(df)} artykułów do pliku: {output_filename}")
        print("Przykładowe dane:")
        print(df.head())
    else:
        print("Nie udało się pobrać żadnych artykułów.")