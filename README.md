# DataQualityDashboard (Python Port)

This project contains an UNOFFICIAL Python port of the open-source [Data Quality Dashboard](https://github.com/OHDSI/DataQualityDashboard) (DQD) application, which is written in R.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [DataQualityDashboard (Python Port)](#dataqualitydashboard-python-port)
  - [Introduction](#introduction)
  - [System Requirements](#system-requirements)
  - [Installation](#installation)
  - [Project Structure](#project-structure)
  - [Getting Started](#getting-started)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Introduction

This package runs data quality checks against an OMOP CDM instance. It is a Python port of the R-based Data Quality Dashboard built by members of the OHDSI commnunities.

## System Requirements

* Requires Python 3.7+

* Currently compatible with the following Datbase management systems:
    * Google BigQuery
    * Microsoft SQL Server (tsql)
    * Sqlite

## Installation

Clone the repository locally.

## Project Structure

```bash
├── README.md
├── app
    ...
│   ├── index.html
│   ├── js
│   ├── results.json
│   ├── results_default.json
│   ...
├── csv
│   ├── OMOP_CDMv5.2_Check_Descriptions.csv
│   ├── OMOP_CDMv5.2_Concept_Level.csv
│   ├── OMOP_CDMv5.2_Field_Level.csv
    ...
├── db.py
├── r_to_python_sql_template.py
├── requirements.txt
├── run_checks.py
└── sql
    ├── concept_plausible_gender.sql
    ├── concept_plausible_unit_concept_ids.sql
    ├── concept_plausible_value_high.sql
    ├── concept_plausible_value_low.sql
    ...

```

**run_checks.py**: Entrypoint script that executes the DQD on an OMOP CDM instance.

**r_to_python_sql_template.py**: A helper script for converting R-compatible templating used by official DQD to Python-compatible templating

**csv/**: Directory containing instructions for running DQD checks.

**sql/**: Directory containing SQL queries for running DQD checks.

**app/**: Directory containing static files for running web application

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp; **results.json**: Output file of DQD execution.
**db.py**: Helper module for standardizing database management.


## Getting Started

1. Install package requirements.
```bash
pip install -r requirements.txt
```
2. Install http-server for static file rendering.
```bash
npm install -g requirements.txt
```
3. The script `run_checks.py` is the entry point. View command line parameters for the DBMS hosting OMOP CDM instance. E.g. for a BigQuery instance:
```
python run_checks.py bigquery -h

usage: run_checks.py bigquery [-h] [--output_folder OUTPUT_FOLDER] [--output_file OUTPUT_FILE]
                              [--check_names [{cdmTable,measurePersonCompleteness,measureConditionEraCompleteness,cdmField,isRequired,cdmDatatype,isPrimaryKey,isForeignKey,fkDomain,fkClass,isStandardValidConcept,measureValueCompleteness,standardConceptRecordCompleteness,sourceConceptRecordCompleteness,sourceValueCompleteness,plausibleValueLow,plausibleValueHigh,plausibleTemporalAfter,plausibleDuringLife,withinVisitDates,plausibleGender,plausibleUnitConceptIds} ...]]
                              [--tables_to_include [TABLES_TO_INCLUDE ...]] [--sql_only]
                              project_id dataset_id

positional arguments:
  project_id            BigQuery project id
  dataset_id            BigQuery dataset id

optional arguments:
  -h, --help            show this help message and exit
  --output_folder OUTPUT_FOLDER
                        Folder to save DQD output
  --output_file OUTPUT_FILE
                        Name of output json file to save results.
  --check_names [{cdmTable,measurePersonCompleteness,measureConditionEraCompleteness,cdmField,isRequired,cdmDatatype,isPrimaryKey,isForeignKey,fkDomain,fkClass,isStandardValidConcept,measureValueCompleteness,standardConceptRecordCompleteness,sourceConceptRecordCompleteness,sourceValueCompleteness,plausibleValueLow,plausibleValueHigh,plausibleTemporalAfter,plausibleDuringLife,withinVisitDates,plausibleGender,plausibleUnitConceptIds} ...]
                        Subgroup of checks to run
  --tables_to_include [TABLES_TO_INCLUDE ...]
                        Subgroup of OMOP tables to include in checks
  --sql_only
```
4. Execute `run_checks.py` script with desired parameters to execute DQD on the OMOP CDM instace. E.g.
```
python run_checks.py bigquery my_bq_project my_bq_dataset --check_names cdmTable: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████████████████████| 1/1 [00:35<00:00, 35.38s/it]
Writing results to app/results.json.
```
5. If **output_path** and **output_dir** are left unspecified, the results are by default written to the **app/** directory with the filename **results.json**. If the defaults were not using, the resulting file must be moved to the directory with this same name.
6. Visualize the DQD results by running the DQD static web application. To start, run the following command:
```
http-server app/

Starting up http-server, serving app/

http-server version: 14.1.1

http-server settings:
CORS: disabled
Cache: 3600 seconds
Connection Timeout: 120 seconds
Directory Listings: visible
AutoIndex: visible
Serve GZIP Files: false
Serve Brotli Files: false
Default File Extension: none

Available on:
  http://127.0.0.1:8080
  http://192.168.1.64:8080
Hit CTRL-C to stop the server
```
7. Open a web browser to URL http://127.0.0.1:8080 to view resulting tables.