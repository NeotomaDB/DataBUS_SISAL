"""_Validate SISAL csv Files_
   Assumes there is a `data` folder from which the python script is run.
   The script obtains all `csv` files in ./data and then reads through
   each of them, validating each field to ensure they are acceptable for
   valid upload.

   Since DataBUS 2.0.0 each `valid_*` function also performs its own insert
   when it is handed the accumulated `databus` dict, so a validation-only run
   ends in `conn.rollback()`. Pass `--upload True` to keep the records.
"""
from datetime import datetime
import os
from pathlib import Path
import json
import psycopg2
from dotenv import load_dotenv
import DataBUS.neotomaValidator as nv
import DataBUS.neotomaHelpers as nh
from DataBUS.neotomaHelpers.logging_dict import logging_response
"""
To run:
uv run python src/template_validate.py --template src/templates/template.yml

To validate and then commit the records:
uv run python src/template_validate.py --template src/templates/template.yml --upload True
"""
args = nh.parse_arguments()
upload = args.get('upload', False)
load_dotenv()
data = json.loads(os.getenv('PGDB_TANK'))

conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()
directory = Path(args['data'])
filenames = directory.glob("*.csv")
filenames = [f for f in filenames if os.path.basename(f) != "references_entities.csv"]
valid_logs = Path('data/validation_logs')
valid_logs_wrong = Path('data/validation_logs/not_validated/')
valid_logs.mkdir(parents = True, exist_ok = True)
valid_logs_wrong.mkdir(parents = True, exist_ok = True)


def run_step(name, key, fn, logfile, databus):
    """Run one validation step in its own savepoint and log the Response.

    `fn` must be a zero-argument callable. `nh.safe_step` rolls back to the
    savepoint on error so a single failing step no longer aborts the whole
    transaction and poisons every step after it.
    """
    logfile.append(f'\n === {name} ===')
    result = nh.safe_step(key, fn, logfile, conn)
    if result is not None:
        databus[key] = result
        logging_response(result, logfile)


for filename in filenames:
    print(filename)
    conn.rollback()
    logfile = []
    databus = dict()
    hashcheck = nh.hash_file(filename)
    filecheck = nh.check_file(filename, validation_files = f'{valid_logs}/')
    logfile = logfile + hashcheck['message'] + filecheck['message']
    logfile.append(f"\nNew validation started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if hashcheck['pass'] and filecheck['pass']:
        print("  - File is correct and hasn't changed since last validation.")
        continue

    yml_dict = nh.template_to_dict(temp_file=args['template'])
    csv_file = nh.read_csv(filename)
    try:
        run_step('Validating Sites', 'sites',
                 lambda: nv.valid_site(cur = cur,
                                       yml_dict = yml_dict,
                                       csv_file = csv_file),
                 logfile, databus)

        run_step('Checking Geopolitical Units', 'gpuid',
                 lambda: nv.valid_geopolitical_units(cur = cur,
                                                     yml_dict = yml_dict,
                                                     csv_file = csv_file,
                                                     databus = databus),
                 logfile, databus)

        run_step('Checking Against Collection Units', 'collunits',
                 lambda: nv.valid_collunit(cur = cur,
                                           yml_dict = yml_dict,
                                           csv_file = csv_file,
                                           databus = databus),
                 logfile, databus)

        run_step('Checking Against Speleothem Entities', 'speleothems',
                 lambda: nv.valid_speleothem(cur = cur,
                                             yml_dict = yml_dict,
                                             csv_file = csv_file,
                                             databus = databus),
                 logfile, databus)

        run_step('Checking External Speleothems', 'external_speleo',
                 lambda: nv.valid_external_speleothem(cur = cur,
                                                      yml_dict = yml_dict,
                                                      csv_file = csv_file,
                                                      databus = databus),
                 logfile, databus)

        run_step('Checking Against Analysis Units', 'analysisunits',
                 lambda: nv.valid_analysisunit(cur = cur,
                                               yml_dict = yml_dict,
                                               csv_file = csv_file,
                                               databus = databus),
                 logfile, databus)

        run_step('Checking Dataset', 'datasets',
                 lambda: nv.valid_dataset(cur = cur,
                                          yml_dict = yml_dict,
                                          csv_file = csv_file,
                                          databus = databus),
                 logfile, databus)

        run_step('Checking GeoChronDataset', 'geodataset',
                 lambda: nv.valid_geochron_dataset(cur = cur,
                                                   yml_dict = yml_dict,
                                                   csv_file = csv_file,
                                                   databus = databus),
                 logfile, databus)

        run_step('Checking Chronologies', 'chronologies',
                 lambda: nv.valid_chronologies(cur = cur,
                                               yml_dict = yml_dict,
                                               csv_file = csv_file,
                                               databus = databus),
                 logfile, databus)

        run_step('Checking ChronControls', 'chron_controls',
                 lambda: nv.valid_chroncontrols(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_file = csv_file,
                                                databus = databus),
                 logfile, databus)

        run_step('Checking Hiatuses', 'hiatus',
                 lambda: nv.valid_hiatus(cur = cur,
                                         yml_dict = yml_dict,
                                         csv_file = csv_file,
                                         databus = databus),
                 logfile, databus)

        run_step('Validating Samples', 'samples',
                 lambda: nv.valid_sample(cur = cur,
                                         yml_dict = yml_dict,
                                         csv_file = csv_file,
                                         databus = databus),
                 logfile, databus)

        run_step('Validating Sample Ages', 'sample_age',
                 lambda: nv.valid_sample_age(cur = cur,
                                             yml_dict = yml_dict,
                                             csv_file = csv_file,
                                             databus = databus),
                 logfile, databus)

        run_step('Validating Geochrons', 'geochron',
                 lambda: nv.valid_geochron(cur = cur,
                                           yml_dict = yml_dict,
                                           csv_file = csv_file,
                                           databus = databus),
                 logfile, databus)

        run_step('Checking Geochron Control', 'geochroncontrol',
                 lambda: nv.valid_geochroncontrol(cur = cur,
                                                  databus = databus),
                 logfile, databus)

        run_step('Checking UTh Series', 'uthseries',
                 lambda: nv.valid_uth_series(cur = cur,
                                             yml_dict = yml_dict,
                                             csv_file = csv_file,
                                             databus = databus),
                 logfile, databus)

        run_step('Checking Against Contact Names', 'contacts',
                 lambda: nv.valid_contact(cur = cur,
                                          yml_dict = yml_dict,
                                          csv_file = csv_file,
                                          databus = databus),
                 logfile, databus)

        run_step('Validating Dataset Database', 'database',
                 lambda: nv.valid_dataset_database(cur = cur,
                                                   yml_dict = yml_dict,
                                                   databus = databus),
                 logfile, databus)

        run_step('Validating Data', 'data',
                 lambda: nv.valid_data(cur = cur,
                                       yml_dict = yml_dict,
                                       csv_file = csv_file,
                                       databus = databus),
                 logfile, databus)

        run_step('Validating Data Uncertainties', 'uncertainty',
                 lambda: nv.valid_datauncertainty(cur = cur,
                                                  yml_dict = yml_dict,
                                                  csv_file = csv_file,
                                                  databus = databus),
                 logfile, databus)

        run_step('Validating Publication', 'publications',
                 lambda: nv.valid_publication(cur = cur,
                                              yml_dict = yml_dict,
                                              csv_file = csv_file,
                                              databus = databus),
                 logfile, databus)

        all_true = all([databus[key].validAll for key in databus.keys()])

        if upload and all_true:
            databus['finalize'] = nv.insert_final(cur, databus = databus)
            conn.commit()
            logfile.append('\nData has been successfully uploaded to the database.')
        else:
            conn.rollback()
            if upload:
                logfile.append('\nData must be fully validated before it can be uploaded.')
            elif all_true:
                logfile.append('\nData has been fully validated and is ready for upload.')

        not_validated_files = "data/not_validated_files"
        if all_true is False:
            print(f"{filename} cannot be validated.\nMoved {filename} to the 'not_validated_files' folder.")
            os.makedirs(not_validated_files, exist_ok=True)
            uploaded_path = os.path.join(not_validated_files, os.path.basename(filename))
            os.replace(filename, uploaded_path)
            modified_filename = f'{filename}'.replace('data/', 'data/validation_logs/not_validated/')
            modified_filename = Path(modified_filename + '.valid.log')
        else:
            modified_filename = f'{filename}'.replace('data/', 'data/validation_logs/')
            modified_filename = Path(modified_filename + '.valid.log')

        with modified_filename.open(mode = 'w', encoding = "utf-8") as writer:
            for i in logfile:
                writer.write(i)
                writer.write('\n')
    except Exception as e:
        conn.rollback()
        logfile.append(f"✗ File validation failed: {e}")
        not_validated_files = "data/not_validated_files"
        os.makedirs(not_validated_files, exist_ok=True)
        uploaded_path = os.path.join(not_validated_files, os.path.basename(filename))
        os.replace(filename, uploaded_path)
        os.makedirs('data/validation_logs/not_validated/', exist_ok=True)
        modified_filename = f'{filename}'.replace('data/', 'data/validation_logs/not_validated/')
        modified_filename = Path(modified_filename + '.valid.log')
        with open(modified_filename, 'w', encoding = "utf-8") as writer:
            for i in logfile:
                writer.write(i)
                writer.write('\n')
