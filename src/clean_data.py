import pandas as pd
from pathlib import Path
import os
import numpy as np

directory = Path('data/')
filenames = directory.glob("*.csv")
original = 'data/original'
cleaned = Path('data/cleaned/')
cleaned.mkdir(exist_ok=True)

# Units dictionary

units = pd.read_excel('src/unitList_SISALv3.xlsx', index_col='variable name')

def fix_names(name):
    multiple_names = name.split(" | ")

    def format_name(part):
        name_split = part.split() # Split each name into parts
        if len(name_split) == 2:
            return f"{name_split[1]}, {name_split[0]}"
        elif len(name_split) == 3:
            return f"{name_split[2]}, {name_split[0]} {name_split[1]}" 
        else:
            return part   

    if len(multiple_names) > 1:
        return " | ".join([format_name(part) for part in multiple_names])
    else:
        return format_name(name)

for file in filenames:
    print(file)
    data = pd.read_csv(file)

    # To remove empty columns
    #cols_to_drop = [col for col in data.columns if col not in ['sample_id', 'entity_id'] and data[col].isna().all()]
    #data = data.drop(columns=cols_to_drop)

    citations = data[['sample_id', 'ref_id', 'publication_DOI', 'citation']]
    data = data.drop(columns = ['ref_id', 'publication_DOI', 'citation'])

    # Add agetype
    data['agetype'] = 'Calibrated radiocarbon years BP'

    # Fix contacts
    data['contact'] = data['contact'].str.replace(',', ' |')
    data['contact'] = data['contact'].apply(fix_names)

    # Fix duplicates
    filled_df = data.fillna('FFILL')
    data_fixed = filled_df.drop_duplicates()
    data = data_fixed.copy()
    data[data == 'FFILL'] = np.nan

    # Fix publications 
    try:
        citations = citations.groupby('sample_id').agg({'ref_id': lambda x: ', '.join(x.astype(str)),
                                                        'publication_DOI': ', '.join,
                                                        'citation': ' | '.join}).reset_index()
    except Exception as e:
        print(e)
        citations = citations.groupby('sample_id').agg({'ref_id': lambda x: ', '.join(map(str, x.dropna())),  
                                                        'publication_DOI': lambda x: ', '.join(map(str, x.dropna())),  
                                                        'citation': lambda x: ' | '.join(map(str, x.dropna()))}).reset_index()
        print(citations)
    for var_name in units.index:
        if var_name in data.columns:
            new_col = f'{var_name}_units'
            data[new_col] = units.loc[var_name, 'unit']
    
    data = data.merge(citations, on = 'sample_id', how = 'left')
    original_path = os.path.join(original, os.path.basename(file))
    os.makedirs(original, exist_ok=True)
    os.replace(file, original_path)
    file = file.relative_to("data")
    data.to_csv(f'data/cleaned/{file}', index = False)    

print("Finished!")