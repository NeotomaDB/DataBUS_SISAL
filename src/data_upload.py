import json
import os
import psycopg2
import glob
from datetime import datetime
from dotenv import load_dotenv
import DataBUS.neotomaHelpers as nh
import DataBUS.neotomaUploader as nu
from DataBUS.neotomaValidator.valid_csv import valid_csv
from DataBUS.neotomaValidator.check_file import check_file
from DataBUS.neotomaHelpers.logging_dict import logging_response
"""
Use this command after having validated the files to 
upload to Neotoma.
To run, you can use: 
python src/data_upload.py --template='src/templates/template.yml'

You can also use a different template file by running:
python src/data_upload.py --template='template.xlsx'

Change 'template.xlsx' to desired filename as long as 
template file that has an .xlsx or .yml extension
"""

load_dotenv()
data = json.loads(os.getenv('PGDB_TANK'))

conn = psycopg2.connect(**data, connect_timeout = 5)
cur = conn.cursor()

args = nh.parse_arguments()
overwrite = args['overwrite']

filenames = glob.glob(args['data'] + "*.csv")
upload_logs = 'data/upload_logs'
if not os.path.exists(upload_logs):
            os.makedirs(upload_logs)

uploaded_files = "data/uploaded_files"

for filename in filenames:
    test_dict = {}
    print(filename)
    logfile = []

    hashcheck = nh.hash_file(filename)
    filecheck = check_file(filename)

    logfile = logfile + hashcheck['message'] + filecheck['message']
    logfile.append(f"\nNew Upload started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    uploader = {}
    if hashcheck['pass'] is False and filecheck['pass'] is False:
        csv_file = nh.read_csv(filename)
        logfile.append("File must be properly validated before it can be uploaded.")
        hashcheck = False
    else:
        csv_file = nh.read_csv(filename)
        hashcheck = True
        
    yml_dict = nh.template_to_dict(temp_file=args['template'])
    yml_data = yml_dict['metadata']

    csv_valid = valid_csv(filename = filename,
                                yml_data = yml_data)

    logfile.append('\n=== Inserting New Site ===')
    uploader['sites'] = nu.insert_site(cur = cur,
                                    yml_dict = yml_dict,
                                    csv_file = csv_file)
    logfile = logging_response(uploader['sites'], logfile)

    logfile.append('\n === Inserting Collection Units ===')
    uploader['collunitid'] = nu.insert_collunit(cur = cur,
                                            yml_dict = yml_dict,
                                            csv_file = csv_file,
                                            uploader = uploader)
    logfile = logging_response(uploader['collunitid'], logfile)
  
    logfile.append('\n=== Inserting Analysis Units ===')
    uploader['anunits'] = nu.insert_analysisunit(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_file = csv_file,
                                                uploader = uploader)
    logfile = logging_response(uploader['anunits'], logfile)

    logfile.append('\n=== Inserting Chronology ===')
    uploader['chronology'] = nu.insert_chronology(cur = cur,
                                                  yml_dict = yml_dict,
                                                  csv_file = csv_file,
                                                  uploader = uploader)
    logfile = logging_response(uploader['chronology'], logfile)
    
    logfile.append('\n=== Inserting Chroncontrol ===')
    uploader['chroncontrol'] = nu.insert_chron_control(cur = cur,
                                                    yml_dict = yml_dict,
                                                    csv_file = csv_file,
                                                    uploader = uploader)
    logfile = logging_response(uploader['chroncontrol'], logfile)

    logfile.append('\n=== Inserting Dataset ===')
    uploader['datasetid'] = nu.insert_dataset(cur = cur,
                                            yml_dict = yml_dict,
                                            csv_file = csv_file,
                                            uploader = uploader)
    logfile = logging_response(uploader['datasetid'], logfile)

    logfile.append('\n=== Inserting Dataset PI ===')
    uploader['datasetpi'] = nu.insert_dataset_pi(cur = cur,
                                                yml_dict = yml_dict,
                                                csv_file = csv_file,
                                                uploader = uploader)
    logfile = logging_response(uploader['datasetpi'], logfile)
 
    logfile.append('\n=== Inserting Data Processor ===')
    uploader['processor'] = nu.insert_data_processor(cur = cur,
                                                    yml_dict = yml_dict,
                                                    csv_file = csv_file,
                                                    uploader = uploader)
    logfile = logging_response(uploader['processor'], logfile)

    logfile.append('\n=== Inserting Dataset Database ===')
    uploader['database'] = nu.insert_dataset_database(cur = cur,
                                                    yml_dict = yml_dict,
                                                    uploader = uploader)
    logfile = logging_response(uploader['database'], logfile)

    logfile.append('\n=== Inserting Samples ===')
    uploader['samples'] = nu.insert_sample(cur, 
                                        yml_dict = yml_dict,
                                        csv_file = csv_file,
                                        uploader = uploader)
    logfile = logging_response(uploader['samples'], logfile)

    logfile.append('\n=== Inserting Sample Analyst ===')
    uploader['sampleAnalyst'] = nu.insert_sample_analyst(cur, 
                                        yml_dict = yml_dict,
                                        csv_file = csv_file,
                                        uploader = uploader)
    logfile = logging_response(uploader['sampleAnalyst'], logfile)

    logfile.append('\n === Inserting Sample Age ===')
    uploader['sampleAge'] = nu.insert_sample_age(cur, 
                                        yml_dict = yml_dict,
                                        csv_file = csv_file,
                                        uploader = uploader)
    logfile = logging_response(uploader['sampleAge'], logfile)

    logfile.append('\n === Inserting Data ===')
    uploader['data'] = nu.insert_data(cur, 
                                    yml_dict = yml_dict,
                                    csv_file = csv_file,
                                    uploader = uploader)
    logfile = logging_response(uploader['data'], logfile)

    logfile.append('\n === Inserting Publications ===')
    uploader['publications'] = nu.insert_publication(cur, 
                                    yml_dict = yml_dict,
                                    csv_file = csv_file,
                                    uploader = uploader)
    logfile = logging_response(uploader['publications'], logfile)

    modified_filename = filename.replace('data/', 'data/upload_logs/')
    with open(modified_filename + '.upload.log', 'w', encoding = "utf-8") as writer:
        for i in logfile:
            writer.write(i)
            writer.write('\n')
            
    all_true = all([uploader[key].validAll for key in uploader])
    all_true = all_true and hashcheck
    if all_true:
        print(f"{filename} was uploaded.\nMoved {filename} to the 'uploaded_files' folder.")
        #conn.commit()
        conn.rollback()
        os.makedirs(uploaded_files, exist_ok=True)
        uploaded_path = os.path.join(uploaded_files, os.path.basename(filename))
        os.replace(filename, uploaded_path)
    else:
        print(f"filename {filename} could not be uploaded.")
        conn.rollback()