import requests
import csv
import time
import json

# lista keywordów
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

base_url = "https://www.loc.gov/search/"

# pola do wyciągnięcia
desired_fields = ["title", "creator", "description", "date", "subject", "publisher", "id", "url"]

all_results = []

# limity – bezpieczeństwo + kontrola czasu
MAX_PAGES_PER_KEYWORD = 100      ## max 100 stron na jedno hasło
#MAX_TOTAL_RECORDS = 10000        ## globalny limit rekordów
records_collected = 0

for kw in keywords:
    #if records_collected >= MAX_TOTAL_RECORDS:
    #    print("Osiągnięto globalny limit rekordów. Kończę.")
    #    break
        
    print(f"Searching for: {kw}")
    page = 1
    while page <= MAX_PAGES_PER_KEYWORD:
        #if records_collected >= MAX_TOTAL_RECORDS:
        #    print("Osiągnięto globalny limit rekordów w trakcie keywordu. Przerywam.")
        #    break
            
        params = {
            "q": f'"{kw}"',       ## exact phrase dzięki cudzysłowom
            "fo": "json",
            "c": 100,      ## max wyników na stronę
            "sp": page,
        }
        response = requests.get(base_url, params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code} dla strony {page}")
            break
        
        data = response.json()
        
        results = data.get("results", [])
        if not results:
            print(f"Brak dalszych wyników na stronie {page}. Kończę ten keyword.")
            break
        
        for item in results:
            #if records_collected >= MAX_TOTAL_RECORDS:
            #    break

            # Filtr post-processing – pomiń amerykańskie "tematy-śmieci"
            subject = item.get("subject", [])
            if isinstance(subject, list):
                subject_str = " | ".join([str(s) for s in subject if s]).lower()
            else:
                subject_str = str(subject).lower()

            description = item.get("description", "") or ""
            description_str = str(description).lower()

            bad_keywords = ["united states", "america", "american", "u.s.", "african americans", "segregation", "suffrage", "civil rights", "naacp", "washington", "new york", "roosevelt", "lincoln", "new england", "hampton"]

            if any(bad in subject_str or bad in description_str for bad in bad_keywords):
                continue  # pomija dany rekord
                
            row = {}
            row["keyword"] = kw

            for field in desired_fields:
                ### niektóre pola są listami lub w pod-słownikach
                value = item.get(field)
                if isinstance(value, list):
                    value = " | ".join([str(v) for v in value if v])
                elif isinstance(value, dict):
                    value = json.dumps(value)
                row[field] = value or ""
            
            # dodatkowy URL do rekordu
            row["item_url"] = item.get("url", "")
            
            all_results.append(row)
            records_collected += 1
        
        print(f"Strona {page} przetworzona – zebrano łącznie {records_collected} rekordów")
        
        # paginacja – sprawdzenie czy jest następna strona
        pagination = data.get("pagination", {})
        if pagination.get("next") is None:
            print("Brak dalszych stron (pagination.next = None). Kończę ten keyword.")
            break
        page += 1
        
        time.sleep(1.5)  ### rate limiting – b. ważne!!

    time.sleep(2)  ## przerwa między keywordami

# zapisz do csv
with open("congress_articles.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["keyword"] + desired_fields + ["item_url"])
    writer.writeheader()
    writer.writerows(all_results)

print(f"\nZebrano łącznie {len(all_results)} rekordów. Zapisano do congress_articles.csv")