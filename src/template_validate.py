"""_Validate 210Pb csv Files_
   Assumes there is a `data` folder from which the python script is run.
   The script obtains all `csv` files in ./data and then reads through
   each of them, validating each field to ensure they are acceptable for
   valid upload.
"""
from datetime import datetime
import os
from pathlib import Path
import json
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import DataBUS.neotomaValidator as nv
import DataBUS.neotomaHelpers as nh
from DataBUS.neotomaHelpers.logging_dict import logging_dict, logging_response
"""
To run:
python src/template_validate.py --template src/templates/template.yml
"""

args = nh.parse_arguments()
load_dotenv()
data = json.loads(os.getenv('PGDB_LOCAL'))
conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

#filenames = glob.glob(args['data'] + "*.csv")
directory = Path(args['data'])
filenames = directory.glob("*.csv")
valid_logs = Path('data/validation_logs')
valid_logs_wrong = Path('data/validation_logs/not_validated/')
valid_logs.mkdir(exist_ok=True)
valid_logs_wrong.mkdir(exist_ok=True)

for filename in filenames:
    print(filename)
    logfile = []

    hashcheck = nh.hash_file(filename)
    filecheck = nv.check_file(filename)
    logfile = logfile + hashcheck['message'] + filecheck['message']
    logfile.append(f"\nNew validation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if hashcheck['pass'] and filecheck['pass']:
        print("  - File is correct and hasn't changed since last validation.")
    else:
        # Load the yml template as a dictionary
        yml_dict = nh.template_to_dict(temp_file=args['template'])
        yml_data = yml_dict['metadata']
        validator = dict()
        csv_file = nh.read_csv(filename)
        
        # Get the unitcols and units to be used
        # Check that the vocab in the template matches the csv vcocab
        #vocab_ = nv.vocabDict(yml_data)

        logfile.append('\n=== File Validation ===')
        validator['csvValid'] = nv.valid_csv(filename = filename,
                                   yml_data = yml_data)
        logfile = logging_dict(validator['csvValid'], logfile)

        logfile.append('\n === Validating Template Unit Definitions ===')
        df = pd.read_csv(filename) #use csv reader
        validator['units'] = nv.valid_units(cur = cur,
                                 yml_dict = yml_dict,
                                 df = df)
        logfile = logging_dict(validator['units'], logfile)

        logfile.append('\n === Validating Sites ===')
        validator['sites'] = nv.valid_site(cur = cur,
                                 yml_dict = yml_dict,
                                 csv_file = csv_file)
        logfile = logging_response(validator['sites'], logfile)#, 'sitelist')
        
        ########### Geopolitical unit:
        # logfile.append('=== Checking Against Geopolitical Units ===')
        # validator['geopol'] = nv.validGeoPol(cur = cur,
        #                            yml_dict = yml_dict,
        #                            csv_file = csv_file)
        # logfile.append(f"Geopol: {validator['geopol']}")

        logfile.append('\n === Checking Against Collection Units ===')
        validator['collunits'] = nv.valid_collunit(cur = cur,
                                                         yml_dict = yml_dict,
                                                         csv_file = csv_file)
        logfile = logging_response(validator['collunits'], logfile)

        logfile.append('\n === Checking Against Analysis Units ===')
        validator['analysisunit'] = nv.valid_analysisunit(yml_dict = yml_dict,
                                                          csv_file = csv_file)
        logfile = logging_response(validator['analysisunit'], logfile)

        # add valid/insert to ndb.leadmodels
        logfile.append('\n === Checking Against Lead Models ===')
        validator['pbmodel'] = nv.valid_pbmodel(cur = cur,
                                                yml_dict = yml_dict,
                                                     csv_file = csv_file,
                                                     validator = validator)
        logfile = logging_response(validator['pbmodel'], logfile)


        logfile.append('\n === Checking Chronologies ===')
        validator['chronologies'] = nv.valid_chronologies(cur = cur,
                                                          yml_dict = yml_dict,
                                                          csv_file = csv_file)
        logfile = logging_response(validator['chronologies'], logfile)
        #
        logfile.append('\n === Checking Chron Controls ===')
        validator['chron_controls'] = nv.valid_chroncontrols(yml_dict = yml_dict,
                                                          csv_file = csv_file)
        logfile = logging_response(validator['chron_controls'], logfile)

        # TODO: Validate dataset - looks like this should be a geochron
        logfile.append('\n === Checking Dataset ===')
        validator['dataset'] = nv.valid_dataset(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_file = csv_file)
        logfile = logging_response(validator['dataset'], logfile)

        ########### PI names:
        logfile.append('\n === Checking Against Contact Names ===')
        validator['agent'] = nv.valid_contact(cur,
                                            csv_file,
                                            yml_dict)
        logfile = logging_response(validator['agent'], logfile)

        logfile.append('\n === Checking the Dating Horizon is Valid ===')
        validator['horizoncheck'] = nv.valid_horizon(yml_dict,
                                                     csv_file)
        logfile = logging_response(validator['horizoncheck'], logfile)

        logfile.append('\n=== Validating Specimens Repository ===')
        validator['repository'] = nv.valid_dataset_repository(cur = cur,
                                                            yml_dict = yml_dict,
                                                            csv_file = csv_file)
        logfile = logging_response(validator['repository'], logfile)

        logfile.append('\n=== Validating Dataset Database ===')
        validator['database'] = nv.valid_dataset_database(cur = cur,
                                                        yml_dict = yml_dict)
        logfile = logging_response(validator['database'], logfile)

        logfile.append('\n=== Validating Samples ===')
        validator['sample'] = nv.valid_sample(cur = cur,
                                              yml_dict = yml_dict,
                                              csv_file = csv_file,
                                              validator = validator)
        logfile = logging_response(validator['sample'], logfile)

        logfile.append('\n=== Validating Sample Ages ===')
        validator['sample_age'] = nv.valid_sample_age(cur = cur,
                                              yml_dict = yml_dict,
                                              csv_file = csv_file,
                                              validator = validator)
        logfile = logging_response(validator['sample_age'], logfile)

        logfile.append('\n === Validating Taxa Names ===')
        validator['taxa'] = nv.valid_data(cur = cur,
                                              yml_dict = yml_dict,
                                              csv_file = csv_file,
                                              validator = validator)
        logfile = logging_response(validator['taxa'], logfile)

        validator['datauncertainty'] = nv.valid_datauncertainty(cur = cur,
                                              yml_dict = yml_dict,
                                              csv_file = csv_file,
                                              validator = validator)
        logfile = logging_response(validator['datauncertainty'], logfile)

        # Nothing needs to be committed to the database
        conn.rollback()
        
        all_true = all([validator[key].validAll for key in ['sites', 'collunits', 'analysisunit', 'pbmodel', 'chronologies', 
                                                            'chron_controls', 'dataset', 'agent', 'horizoncheck', 'repository',
                                                            'database', 'sample', 'sample_age', 'taxa', 'datauncertainty']])
        #all_true = False
        not_validated_files = "data/not_validated_files"

        #all_true = True
        if all_true == False:
            print(f"{filename} cannot be validated.\nMoved {filename} to the 'not_validated_files' folder.")
            os.makedirs(not_validated_files, exist_ok=True)
            uploaded_path = os.path.join(not_validated_files, os.path.basename(filename))
            os.replace(filename, uploaded_path)

        ########### Write to log.
        if all_true == False:
            modified_filename = f'{filename}'.replace('data/', 'data/validation_logs/not_validated/')
            modified_filename = Path(modified_filename + '.valid.log')
        else:
            modified_filename = f'{filename}'.replace('data/', 'data/validation_logs/')
            modified_filename = Path(modified_filename + '.valid.log')
        
        with modified_filename.open(mode = 'w', encoding = "utf-8") as writer:
            for i in logfile:
                writer.write(i)
                writer.write('\n') 