# Export Data From SISAL

This folder is intended to be used to export data directly from the [SISAL database](https://pastglobalchanges.org/science/wg/sisal/intro) into a set of comma separated data files, one for each individual site (or composite site) within the database for use in the more comprehensive DataBUS workflow.

The script connects to a local instance of the SISAL MySQL database using a connection string read from a `.env` file in this directory. The `.env` file is not included in this repository because it is likely to include usernames and passwords that should not be shared publically.

The file should have the structure:

```env
SISAL_CONNECT={"host":"127.0.0.1","port":3306,"user":"USERNAME","password":"PASSWORD","database":"sisal"}
```

The scrpt itself reads in this environment variable using the python package `python-dotenv`.

## Expected Behaviour

To run the script, type:

`uv run export_by_entity.py`

The script will then open the database connection, read in all the unique `site_id` values in the database, and return a single file for each site into a `./data` folder in this repository. Each `csv` file should have a set of unique columns that match columns in the `sisal_template.csv` or `sisal_template.yaml` file.

## Modifying the File Output

The file output is determined by the [wide_export](./sql/wide_export.sql) file which performs a set of `LEFT JOIN`s on the tables in SISAL to generate a comprehensive output for each record. The query as currently written dumps all contents of each table using `SELECT table.*`. We then strip down the full output within the function `write_to_csv()`. This is a bit lazy and may cause complications if multiple tables have variables of the same name with different meanings (see for example the column 'notes' in Neotoma).  To modify file outputs, the first option should be modifying [wide_export](./sql/wide_export.sql).

The file output location is `../../data` relative to this directory location. To change this, look at the `assert` statements in the `export_by_site.py` file, and in the `write_to_csv()` function.

## Importing All Contacts

The SISAL contacts are stored as names in the `entity.contact` column of the database. The format of these names is different than in Neotoma, where we use a Lastname, Firstname format. This presents some challenges to uploading since the names come from several different international naming standards.

NOTE: This is not, *per se* an issue with SISAL, but more directly, an issue with the way we manage names generally.

Importing names is done through the [`contact_upload.py`](contact_upload.py) script. To run the script, use the `uv run contact_upload.py`. This will ensure that all dependencies are met, and that names are uploaded properly. I have added a tag for the 'NOTES' field: **Inserted through SISAL Bulk Upload** so that if we need to revisit these contacts we can adapt.

## Importing Publications

