import requests
import pandas as pd
import time

# keywords
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

# base URL dla JSON
base_url = "https://api.lib.harvard.edu/v2/items.json"
params_base = {
    "limit": 100,
}

all_records = []

for kw in keywords:
    print(f"Wyszukiwanie dla: \"{kw}\"")
    max_records_per_kw = 10000
    collected_for_kw = 0
    start = 0
    while collected_for_kw < max_records_per_kw:
        params = params_base.copy()
        params["q"] = f'"{kw}"' ### fraza w cudzysłowie dla dokładnego wyszukiwania
        params["start"] = start
        
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Błąd HTTP: {response.status_code} dla {kw}")
            break
        
        try:
            data = response.json()
        except Exception as e:
            print(f"Błąd parsowania JSON: {e}")
            break

        pagination = data.get("pagination", {})
        num_found = pagination.get("numFound", 0)
        print(f"  --> Znaleziono ogółem: {num_found} rekordów")
        
        items_data = data.get("items")
        if items_data is None or not isinstance(items_data, dict):
            print("  Brak lub niepoprawny klucz 'items' w odpowiedzi – kończę dla tego keywordu")
            break

        items = items_data.get("mods", [])

        if not items or collected_for_kw >= max_records_per_kw:
            break

        print(f"  Pobrano {len(items)} rekordów z tej strony")
        
        for raw_item in items:
            # dla bezpieczeństwa - jeśli item jest stringiem (rzadki błąd API) to pomiń
            if not isinstance(raw_item, dict):
                print(f"  Pominięto niepoprawny rekord (nie dict): {type(raw_item)}")
                continue

            item = raw_item  # teraz bezpieczny dict

            # Title
            title_info = item.get("titleInfo", {})
            if isinstance(title_info, dict):
                title = title_info.get("title", "")
            elif isinstance(title_info, list) and title_info:
                title = title_info[0].get("title", "")
            else:
                title = ""
            
            # Authors – roleTerm #text = creator/author  ### zabezpieczenie: role jako dict lub lista
            authors_list = []
            for name in item.get("name", []):
                if isinstance(name, dict):
                    role_val = name.get("role")
                    if role_val:
                        ## role może być dict lub listą dictów
                        roles = [role_val] if isinstance(role_val, dict) else role_val
                        for r in roles:
                            if isinstance(r, dict):
                                role_term = r.get("roleTerm", {})
                                if isinstance(role_term, dict):
                                    role_text = role_term.get("#text", "").lower()
                                elif isinstance(role_term, list):
                                    role_text = ";".join([rt.get("#text", "").lower() for rt in role_term if isinstance(rt, dict)])
                                else:
                                    role_text = ""
                                if role_text in ["author", "creator"]:
                                    part = name.get("namePart", "")
                                    if isinstance(part, str) and part:
                                        authors_list.append(part)
                                    elif isinstance(part, list):
                                        authors_list.extend([p for p in part if isinstance(p, str)])
            authors = "; ".join(authors_list)
            
            # Abstract / description (abstract lub note) –> czyści #text z dictów
            abstract = ""
            for candidate in [item.get("abstract"), item.get("note")]:
                if not candidate:
                    continue
                if isinstance(candidate, list):
                    for c in candidate:
                        if isinstance(c, dict):
                            abstract = c.get("#text", "") or abstract
                        elif isinstance(c, str):
                            abstract = c or abstract
                elif isinstance(candidate, dict):
                    abstract = candidate.get("#text", "") or abstract
                elif isinstance(candidate, str):
                    abstract = candidate
                if abstract:
                    break  ## bierze pierwsze niepuste
            
            # Subject
            subjects_list = []
            for s in item.get("subject", []):
                if isinstance(s, dict):
                    topic = s.get("topic", "")
                    if topic:
                        if isinstance(topic, list):
                            subjects_list.extend([t for t in topic if t])  # filtr None/empty
                        elif topic:
                            subjects_list.append(topic)
                elif isinstance(s, str) and s:
                    subjects_list.append(s)
                elif isinstance(s, list):
                    subjects_list.extend([t for t in s if isinstance(t, str) and t])
            subjects = "; ".join(subjects_list)
            
            # Publisher i PubDate – obsługa originInfo jako lista dictów
            publisher = ""
            pub_date = ""
            origin_info = item.get("originInfo", [])
            if isinstance(origin_info, dict):
                origin_info = [origin_info]  # na listę jeśli pojedynczy dict
            for oi in origin_info:
                if isinstance(oi, dict):
                    pub = oi.get("publisher", "")
                    if pub:
                        publisher = pub  # bierze pierwsze niepuste
                    date = oi.get("dateIssued", "")
                    if date:
                        pub_date = date  # pierwsze niepuste
            
            # ID – z recordInfo > recordIdentifier
            record_id = ""
            rec_info = item.get("recordInfo", {})
            rec_id_val = rec_info.get("recordIdentifier", {})
            if isinstance(rec_id_val, dict):
                record_id = rec_id_val.get("#text", "")
            elif isinstance(rec_id_val, list) and rec_id_val:
                record_id = rec_id_val[0].get("#text", "")
            
            
            all_records.append({
                "keyword": kw,
                "title": title,
                "authors": authors,
                "abstract": abstract,
                "subject": subjects,
                "publisher": publisher,
                "pubDate": pub_date,
                "id": record_id
            })

        
        collected_for_kw += len(items)
        start += params["limit"]
        time.sleep(3)  ### bezpieczny delay


df = pd.DataFrame(all_records)


# zapis
df.to_csv("harvard_articles.csv", index=False, encoding="utf-8")
print(f"Zebrano {len(df)} unikalnych rekordów. Plik: harvard_articles.csv")