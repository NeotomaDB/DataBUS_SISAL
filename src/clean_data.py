import pandas as pd
from pathlib import Path
import os

directory = Path('data-all/')
filenames = directory.glob("*.csv")
original = 'data-all/original'
cleaned = Path('data-all/cleaned/')
cleaned.mkdir(exist_ok=True)

for file in filenames:
    data = pd.read_csv(file)
    data['contact'] = data['contact'].str.replace(',', ' |')
    original_path = os.path.join(original, os.path.basename(file))
    os.makedirs(original, exist_ok=True)
    os.replace(file, original_path)
    file = file.relative_to("data-all")
    data.to_csv(f'data-all/cleaned/{file}', index = False)    

print("Finished!")