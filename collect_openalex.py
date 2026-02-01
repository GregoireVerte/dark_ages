import requests
import pandas as pd
import time
import random
import os

def reconstruct_abstract(inverted_index):
    """Rekonstruuje abstrakt z formatu Inverted Index OpenAlex."""
    if not inverted_index or not isinstance(inverted_index, dict):
        return ""
    
    word_index = {}
    for word, locations in inverted_index.items():
        for loc in locations:
            word_index[loc] = word
    
    return " ".join([word_index[i] for i in sorted(word_index.keys())])

def get_openalex_data(keywords, email, max_results_per_kw=2500):
    base_url = "https://api.openalex.org/works"
    csv_filename = 'openalex_articles.csv'
    
    # sprawdza czy plik istnieje żeby wiedzieć czy dopisać nagłówek
    file_exists = os.path.isfile(csv_filename)

    per_page = 50  # optymalna liczba wyników na jedną stronę API

    for kw in keywords:
        print(f"\n--- Rozpoczynam pobieranie dla: {kw} ---")
        current_kw_results = [] ## lista tymczasowa dla pojedynczego keyworda
        results_for_kw = 0
        page = 1
        
        while results_for_kw < max_results_per_kw:
            print(f"  Pobieranie strony {page}...")
            
            params = {
                'search': kw,
                'mailto': email,
                'page': page,
                'per_page': per_page,
                'select': 'id,title,authorships,abstract_inverted_index,publication_year,topics,primary_location'
            }

            try:
                response = requests.get(base_url, params=params, timeout=30)
                
                if response.status_code == 429:
                    print("  Otrzymano Error 429 (Rate Limit). Czekam dłużej...")
                    time.sleep(60)
                    continue
                
                response.raise_for_status()
                data = response.json()
                results = data.get('results', [])
                
                if not results:
                    print("  Brak więcej wyników dla tego słowa.")
                    break

                for work in results:
                    if results_for_kw >= max_results_per_kw:
                        break
                    
                    # Autorzy
                    authors = ", ".join([auth.get('author', {}).get('display_name', '') 
                                       for auth in work.get('authorships', [])])
                    
                    # Tematy ('topics')
                    subjects = ", ".join([t.get('display_name', '')
                                        for t in work.get('topics', [])])
                    
                    # Wydawca (schowany głębiej w primary_location -> source)
                    primary_loc = work.get('primary_location') or {}
                    source = primary_loc.get('source') or {}
                    publisher = source.get('publisher') or ""

                    # Budowa rekordu - jeśli czegoś brakuje, wpisuje pusty string
                    row = {
                        'keyword': kw,
                        'title': work.get('title') or "",
                        'creator': authors,
                        'abstract': reconstruct_abstract(work.get('abstract_inverted_index')),
                        'year': work.get('publication_year') or "",
                        'subject': subjects,
                        'publisher': publisher,
                        'id': (work.get('id') or "").replace("https://openalex.org/", "")
                    }
                    current_kw_results.append(row)
                    results_for_kw += 1

                print(f"  Pobrano łącznie: {results_for_kw}/{max_results_per_kw}")

                # Sprawdzenie czy jest sens iść do następnej strony
                if len(results) < per_page:
                    break
                
                page += 1
                
                # Losowy delay między stronami (ochrona przed 429)
                delay = random.uniform(3.5, 6.5)
                print(f"  Czekam {delay:.2f}s przed kolejną stroną...")
                time.sleep(delay)

            except Exception as e:
                print(f"  Błąd krytyczny przy {kw}: {e}")
                break

        ## po każdym słowie kluczowym zapis do pliku
        if current_kw_results:
            temp_df = pd.DataFrame(current_kw_results)
            # 'a' oznacza append, header tylko jeśli plik nie istniał
            temp_df.to_csv(csv_filename, mode='a', index=False, header=not file_exists, encoding='utf-8')
            file_exists = True # następne słowa kluczowe już nie dodają kolejnego nagłówka
            print(f"  ZAPISANO: {len(current_kw_results)} rekordów dla {kw}")
        
        # czyszczenie listy przed następnym słowem by zwolnić RAM
        current_kw_results = []

        # Dodatkowy delay po zakończeniu całego słowa kluczowego
        time.sleep(random.uniform(2.0, 5.0))

    return None

# ustawienia
# e-mail - dzięki temu trafiasz do "Polite Pool" w OpenAlex
MY_EMAIL = "trevor84@wp.pl"

# keywords
openalex_keywords = [
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

# Limit rekordów na każde słowo kluczowe
MAX_PER_KW = 2500

# Uruchomienie
get_openalex_data(openalex_keywords, MY_EMAIL, max_results_per_kw=MAX_PER_KW)

print(f"\nProces zakończony. Sprawdź plik 'openalex_articles.csv'.")