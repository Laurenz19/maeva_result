import pandas as pd 
import os
import csv
from datetime import datetime, timedelta

all_files = os.listdir()    
csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
dfs = []

for csv_file in csv_files:
    dfs.append(pd.read_csv(csv_file))

len(dfs)

df = pd.concat(dfs)
df.to_csv("camping_all.csv", index=False)

def clean_campings(src, dest):
    def clean_line(line, references):
        for key in line.keys():
            if str(line[key]) == 'nan':
                line[key] = ''

            line['localite'] = line['localite'].split(',')[0].strip()
            if line['localite'] in references.keys() and type(references[line['localite']]['station']) == str:
                line['nom_station'] = references[line['localite']]['station']

            if type(line['nom_station']) != str:
                line['nom_station'] = ""

            line['web-scrapper-order'] = ""
            line['n_offre'] = ""
            line['date_debut-jour'] = ""
            line['localite'] = line['localite'].replace(', ', ' ') if not line['localite'].split(' ')[0].isdigit() else ' '.join(line['localite'].split(' ')[1:]).replace(', ', ' ')
            line['typologie'] = line['typologie'].replace(', ', ' - ')
            line['prix_init'] = str(int(float(line['prix_init']))) if line['prix_init'] != 'prix_init' else 'prix_init'
            line['prix_actuel'] = str(int(float(line['prix_actuel']))) if line['prix_actuel'] != 'prix_actuel' else 'prix_actuel'

        return line
    


    csv_source = pd.read_csv(src, encoding='utf-8')
    csv_source.dropna(subset=['Nb personnes'], inplace=True)
    csv_source = csv_source[csv_source['Nb personnes'].isin(['4 Adultes','6 Adultes', '8 Adultes'])]
    csv_source.drop_duplicates(inplace=True, subset=['date_price', 'date_debut', 'date_fin', 'prix_init', 'prix_actuel', 'typologie', 'nom', 'localite'])
    csv_source.sort_values(inplace=True, ascending = True, by=['Nb semaines', 'date_debut'])
    csv_dict = csv_source.to_dict(orient='records')

    new_dict = []

    all_references_list = pd.read_excel('referencement stations.xlsm', sheet_name='Feuil1').to_dict(orient='records')

    all_references_dict = {}
    for ref in all_references_list:
        if ref['Localite'] not in all_references_dict.keys():
            all_references_dict[ref['Localite'].split(',')[0]] = {'station': ref['Station'], 'dest': ref['Cle station']}

    for line in csv_dict:
        cleaned_line = clean_line(line, all_references_dict)
        new_dict.append(cleaned_line)

    with open(dest, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['web-scrapper-order', 'date_price', 'date_debut', 'date_fin', 'prix_init', 'prix_actuel', 'typologie', 'Nb personnes','n_offre', 'nom', 'localite', 'date_debut-jour','Nb semaines', 'cle_station', 'nom_station']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for line in new_dict:
            try:
                writer.writerow(line)
            except Exception as e:
                print(e)
                pass

def merge_campings(srcs, dest):
    csv_list = []

    csv_headers = pd.DataFrame(columns=[
    'web-scrapper-order',
    'date_price',
    'date_debut',
    'date_fin',
    'prix_init',
    'prix_actuel',
    'Nb personnes',
    'typologie',
    'n_offre',
    'nom',
    'localite',
    'date_debut-jour',
    'Nb semaines'
    ])

    csv_list.append(csv_headers)

    for file in srcs:
        csv_list.append(pd.read_csv(file, encoding='utf-8'))

        csv_merged = pd.concat(csv_list)
        csv_merged.sort_values(inplace=True, ascending = True, by=['Nb semaines', 'date_debut'])
        csv_merged.drop_duplicates(subset=['date_price', 'date_debut', 'date_fin', 'prix_init', 'prix_actuel', 'typologie', 'nom', 'localite'])
        csv_merged.to_csv(dest, index=False)
        

clean_campings('./camping_all.csv', 'camping_cleaned_09_10_2023.csv')