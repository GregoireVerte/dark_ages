import pandas as pd
import csv
import re


keywords = [
    "vikings",
    "carolingian",
    "fall of rome",
    "justinian reconquest",
    "anglo-saxon settlement",
]


INPUT_TSV = 'hathifiles20251201-26-1nw25t'
OUTPUT_CSV = 'hathi_articles.csv'
DESCRIPTION_MAX_LEN = 6000          # obcinanie długich opisów


# wczytuje bez headera po indeksach kolumn (pozycje z docs HathiFiles)
df = pd.read_csv(
    INPUT_TSV,
    sep='\t',
    header=None,                    # brak nagłówka
    usecols=[0,1,4,11,12,16,25],   # indeksy: htid=0, access=1, description=4, title=11, imprint=12, rights_date_used=16, author=25
    names=['htid', 'access', 'description', 'title', 'imprint', 'rights_date_used', 'author'],
    dtype=str,
    low_memory=False,
    encoding='utf-8'
)

# uzupełnia brakujące/puste wartości pustymi stringami (bez None/NaN)
df['title'] = df['title'].fillna('').astype(str)
df['author'] = df['author'].fillna('').astype(str)
df['imprint'] = df['imprint'].fillna('').astype(str)
df['rights_date_used'] = df['rights_date_used'].fillna('').astype(str)
df['description'] = df['description'].fillna('').astype(str)
df['htid'] = df['htid'].fillna('').astype(str)
df['access'] = df['access'].fillna('deny').astype(str)

# filtr: tylko full-view (public domain) i rekordy z tytułem
df = df[(df['access'] == 'allow') & (df['title'] != '') & (df['title'].notna())]

print(f"Wczytano {len(df)} rekordów po filtrze (tylko public domain z tytułem).")

total_records = 0

# nagłówki CSV
fieldnames = ['keyword', 'title', 'authors', 'publisher', 'published_date', 'description', 'hathi_id']

with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for kw in keywords:
        print(f"\nSzukam: '{kw}' ...")
        
        # filtrowanie po keyword w title OR author OR description OR imprint (case-insensitive)
        mask = (
            df['title'].str.contains(kw, case=False, na=False) |
            df['author'].str.contains(kw, case=False, na=False) |
            df['description'].str.contains(kw, case=False, na=False) |
            df['imprint'].str.contains(kw, case=False, na=False)
        )
        results = df[mask].copy()
        
        keyword_count = len(results)
        print(f" --> znaleziono {keyword_count} unikalnych rekordów dla '{kw}'")
        
        for _, row in results.iterrows():
            desc_raw = row['description']
            description = desc_raw[:DESCRIPTION_MAX_LEN] if len(desc_raw) > DESCRIPTION_MAX_LEN else desc_raw
            
            writer.writerow({
                'keyword': kw,
                'title': row['title'],
                'authors': row['author'],
                'publisher': row['imprint'],  ### publishing info (wydawca/miejsce/rok)
                'published_date': row['rights_date_used'],
                'description': description,
                'hathi_id': row['htid']
            })
            total_records += 1

print(f"\nZapisano {total_records} unikalnych rekordów do {OUTPUT_CSV}")