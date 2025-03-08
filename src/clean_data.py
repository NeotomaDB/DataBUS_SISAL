import pandas as pd
from pathlib import Path
import os

directory = Path('data/')
filenames = directory.glob("*.csv")
original = 'data/original'
cleaned = Path('data/cleaned/')
cleaned.mkdir(exist_ok=True)

# Units dictionary

units = pd.read_excel('src/unitList_SISALv3.xlsx', index_col='variable name')

for file in filenames:
    data = pd.read_csv(file)
    for var_name in units.index:
        if var_name in data.columns:
            new_col = f'{var_name}_units'
            data[new_col] = units.loc[var_name, 'unit']
    data['contact'] = data['contact'].str.replace(',', ' |')
    original_path = os.path.join(original, os.path.basename(file))
    os.makedirs(original, exist_ok=True)
    os.replace(file, original_path)
    file = file.relative_to("data")
    data.to_csv(f'data/cleaned/{file}', index = False)    

print("Finished!")