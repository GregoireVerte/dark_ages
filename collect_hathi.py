import csv
import time
import requests
from xml.etree import ElementTree as ET


keywords = [
    "vikings",
    "carolingian",
]


OUTPUT_CSV = 'hathi_articles.csv'
DELAY_BETWEEN_REQUESTS = 3
RESULTS_PER_PAGE = 50             # max w OAI
DESCRIPTION_MAX_LEN = 3000        # obcinanie bardzo długich opisów


fieldnames = ['keyword', 'title', 'authors', 'publisher', 'published_date', 'description', 'hathi_id']

with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    total_records = 0

    for kw in keywords:
        print(f"\nSzukam: '{kw}' ...")

        page = 1
        resumption_token = None
        all_results_fetched = False
        retries = 0
        max_retries = 5

        while not all_results_fetched and retries < max_retries:
            try:
                # buduj URL dla OAI ListIdentifiers (wyszukiwanie przez arg, harvest fullView)
                params = {
                    'verb': 'ListIdentifiers',
                    'metadataPrefix': 'oai_dc',  ### Dublin Core: title, creator, etc.
                    'set': 'fullView',           ### tylko public domain
                    'identifier': f'oai:quod.lib.umich.edu:HT{page}:{kw}'  # pseudo-ID dla query (adaptowane do keyword)
                }
                if resumption_token:
                    params['resumptionToken'] = resumption_token

                response = requests.get(
                    'https://catalog.hathitrust.org/oai/oai.php',
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                root = ET.fromstring(response.content)

                # sprawdź błędy OAI
                error = root.find('.//{http://www.openarchives.org/OAI/2.0/}error')
                if error is not None:
                    raise ValueError(f"OAI Error: {error.text}")

                headers = root.findall('.//{http://www.openarchives.org/OAI/2.0/}header')
                if not headers:
                    all_results_fetched = True
                    continue

                keyword_count_this_page = 0
                for header in headers:
                    hathi_id = header.find('{http://www.openarchives.org/OAI/2.0/}identifier').text if header.find('{http://www.openarchives.org/OAI/2.0/}identifier') is not None else ''

                    # pobierz pełne metadane dla ID (drugi request per record – akceptowalne dla metadanych)
                    meta_params = {'verb': 'GetRecord', 'metadataPrefix': 'oai_dc', 'identifier': hathi_id}
                    meta_resp = requests.get('https://catalog.hathitrust.org/oai/oai.php', params=meta_params, timeout=30)
                    meta_root = ET.fromstring(meta_resp.content)
                    md_ns = '{http://www.openarchives.org/OAI/2.0/oai_dc/}dc:'
                    metadata = meta_root.find('.//{http://www.openarchives.org/OAI/2.0/oai_dc/}metadata')

                    title = ' | '.join([el.text for el in metadata.findall(md_ns + 'title') if el.text]) or ''
                    authors = ' | '.join([el.text for el in metadata.findall(md_ns + 'creator') if el.text]) or ''
                    publisher = ' | '.join([el.text for el in metadata.findall(md_ns + 'publisher') if el.text]) or ''
                    date = ' | '.join([el.text for el in metadata.findall(md_ns + 'date') if el.text]) or ''
                    desc_raw = ' | '.join([el.text for el in metadata.findall(md_ns + 'description') if el.text]) or ''
                    description = desc_raw[:DESCRIPTION_MAX_LEN]

                    writer.writerow({
                        'keyword': kw,
                        'title': title,
                        'authors': authors,
                        'publisher': publisher,
                        'published_date': date,
                        'description': description,
                        'hathi_id': hathi_id
                    })
                    keyword_count_this_page += 1
                    total_records += 1

                print(f"  -> strona {page}: {keyword_count_this_page} rekordów (bez duplikatów)")

                ## paginacja
                resumption = root.find('.//{http://www.openarchives.org/OAI/2.0/}resumptionToken')
                if resumption is not None and resumption.text:
                    resumption_token = resumption.text
                    page += 1
                else:
                    all_results_fetched = True

                retries = 0  ## reset po sukcesie
                time.sleep(DELAY_BETWEEN_REQUESTS)

            except Exception as e:
                retries += 1
                print(f"  Błąd dla '{kw}' (strona {page}, próba {retries}/{max_retries}): {e}")
                if retries < max_retries:
                    wait = 10 * retries
                    print(f"    czekam {wait} sekund...")
                    time.sleep(wait)
                else:
                    print(f"  PORZUCAM '{kw}' po {max_retries} próbach")
                    all_results_fetched = True

        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\nZapisano {total_records} unikalnych rekordów do pliku {OUTPUT_CSV}")