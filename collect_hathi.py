import csv
import time
from sickle import Sickle


keywords = [
    "vikings",
    "carolingian",
]


OUTPUT_CSV = 'hathi_articles.csv'
DELAY_BETWEEN_REQUESTS = 3


sickle = Sickle('https://catalog.hathitrust.org/oai/oai.php')


#### nagłówki CSV
fieldnames = ['keyword', 'title', 'authors', 'publisher', 'published_date', 'description', 'hathi_id']


with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    total_records = 0

    for kw in keywords:
        print(f"\nSzukam: '{kw}' ...")
        try:
            #### ListRecords automatycznie obsługuje resumptionToken (paginacja)
            records = sickle.ListRecords(
                metadataPrefix='oai_dc',
                set='fullView',  #### tylko domena publiczna
                ignore_deleted=True,
                query=f'(dc.title:"{kw}" OR dc.creator:"{kw}" OR dc.description:"{kw}" OR dc.subject:"{kw}")'
            )

            keyword_count = 0
            for record in records:
                md = record.metadata
                header = record.header

                title = ' | '.join(md.get('title', [''])) or 'brak tytułu'
                authors = ' | '.join(md.get('creator', [''])) or 'brak autora'
                publisher = ' | '.join(md.get('publisher', [''])) or ''
                date = ' | '.join(md.get('date', [''])) or ''
                description = ' | '.join(md.get('description', [''])) or ''

                writer.writerow({
                    'keyword': kw,
                    'title': title,
                    'authors': authors,
                    'publisher': publisher,
                    'published_date': date,
                    'description': description[:2000],  #### obcinanie bardzo długich opisów
                    'hathi_id': header.identifier
                })
                keyword_count += 1
                total_records += 1

            print(f"  → znaleziono {keyword_count} rekordów dla '{kw}'")

            time.sleep(DELAY_BETWEEN_REQUESTS)

        except Exception as e:
            print(f"  Błąd dla '{kw}': {e}")

    print(f"\nGOTOWE! Zapisano {total_records} rekordów do pliku: {OUTPUT_CSV}")