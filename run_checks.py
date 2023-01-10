# Python Imports
from pathlib import Path
from tqdm import tqdm
import re
import logging
from functools import reduce
import json
import datetime
import argparse

# Third-Party Imports
import pandas as pd
from jinja2 import FileSystemLoader, Environment
import numpy as np
from sqlalchemy import create_engine
import sqlglot

# Package Imports
from db import DBMS_NAMES

TEMPLATE_LOADER = FileSystemLoader(searchpath="./sql")
TEMPLATE_ENV = Environment(loader=TEMPLATE_LOADER)


class DQD():
    """Manager of the Data Quality Dashboard execution pipeline

    Raises:
        ValueError: Raised if a valid DBMS is not previded
        ValueError: _description_

    """
    _csv_dir = Path('csv')

    def __init__(self,
                 dbms,
                 dbms_params,
                 output_folder="output",
                 output_file="results.json",
                 tables_to_include=[],
                 tables_to_exclude=[
                     'CONCPT', 'VOCABULARY', 'CONCEPT_ANCESTOR',
                     'CONCEPT_RELATIONSHIP', 'CONCEPT_CLASS',
                     'CONCEPT_SYNONYM', 'RELATIONSHIP', 'DOMAIN'
                 ],
                 check_names=[],
                 cdm_version="5.3"):

        self.output_folder = output_folder
        self.output_file = output_file
        self.tables_to_include = tables_to_include
        self.tables_to_exclude = tables_to_exclude
        self.check_names = check_names
        self.cdm_version = cdm_version

        # Formate database connection string based on DBMS
        if dbms == 'bigquery':
            conn_string = f'bigquery://{dbms_params["project_id"]}'
            cdm_schema = f'{dbms_params["project_id"]}.{dbms_params["dataset_id"]}'
            vocab_schema = cdm_schema
        elif dbms == 'tsql':
            conn_string = f'mssql+pyodbc://{dbms_params["user"]}:{dbms_params["passwd"]}@{dbms_params["db_server"]}'
            cdm_schema = dbms_params['schema']
            vocab_schema = dbms_params['schema']
        else:
            raise ValueError(
                f'DBMS not recognized. Must be one of: {DBMS_NAMES}')

        self.conn_string = conn_string
        self.dbms = dbms
        self.cdm_schema = cdm_schema
        self.vocab_schema = vocab_schema

    def execute(self, sql_only=False, write_to_csv=False):
        """Executes the DQD pipeline

        Args:
            sql_only (bool, optional): Determines whether the DQD queries will be executed (False) or only formatted and saved (True). Defaults to False.
            write_to_csv (bool, optional): Determines whether DQD results are also written to a CSV file. Defaults to False.
        """
        logging.info('Beginning check execution')
        if not sql_only:
            # Create SQLalchemy engine for executing queries
            self.engine = create_engine(self.conn_string)

        Path(self.output_folder).mkdir(exist_ok=True)

        check_descriptions = pd.read_csv(
            Path(self._csv_dir) /
            f'OMOP_CDMv{self.cdm_version}_Check_Descriptions.csv',
            keep_default_na=False)
        table_checks = pd.read_csv(
            Path(self._csv_dir) /
            f'OMOP_CDMv{self.cdm_version}_Table_Level.csv',
            keep_default_na=False)
        field_checks = pd.read_csv(
            Path(self._csv_dir) /
            f'OMOP_CDMv{self.cdm_version}_Field_Level.csv',
            keep_default_na=False)
        concept_checks = pd.read_csv(
            Path(self._csv_dir) /
            f'OMOP_CDMv{self.cdm_version}_Concept_Level.csv',
            keep_default_na=False)

        start_time = datetime.datetime.now()

        # ensure we use only checks that are intended to be run

        if len(self.tables_to_include) > 0:
            tables_to_include = [s.upper() for s in self.tables_to_include]

            table_checks = table_checks[table_checks['cdmTableName'].isin(
                tables_to_include)]
            field_checks = field_checks[field_checks['cdmTableName'].isin(
                tables_to_include)]
            concept_checks = concept_checks[
                concept_checks['cdmTableName'].isin(tables_to_include)]

        if len(self.tables_to_exclude) > 0:
            tables_to_exclude = [s.upper() for s in self.tables_to_exclude]

            table_checks = table_checks[~table_checks['cdmTableName'].
                                        isin(tables_to_exclude)]
            field_checks = field_checks[~field_checks['cdmTableName'].
                                        isin(tables_to_exclude)]
            concept_checks = concept_checks[~concept_checks['cdmTableName'].
                                            isin(tables_to_exclude)]

        # remove offset from being checked
        field_checks = field_checks[field_checks['cdmFieldName'] != 'offset']

        if len(self.check_names) > 0:
            check_descriptions = check_descriptions[
                check_descriptions['checkName'].isin(self.check_names)]

        if len(check_descriptions) == 0:
            logging.info(
                "No checks are available based on excluded tables. Please review tables_to_exclude."
            )
            exit(1)

        results_list = []
        logging.info('Running checks')
        pbar = tqdm(list(check_descriptions.iterrows()), position=0)
        for _, check_description in pbar:
            pbar.set_description(f"{check_description['checkName']}")

            # Run the DQ check
            result = self.run_check(check_description, table_checks,
                                    field_checks, concept_checks,
                                    self.cdm_schema, self.vocab_schema,
                                    self.output_folder, sql_only)
            results_list.append(result)

        all_results = None
        if not sql_only:
            check_results = pd.concat(results_list)

            check_results = self.evaluate_thresholds(check_results,
                                                     table_checks,
                                                     field_checks,
                                                     concept_checks)

            overview = self.summarize_results(check_results)
            end_time = datetime.datetime.now()

            delta = (end_time - start_time).seconds

            # TODO: Remove this filler metadata and read from CDM Source table
            metadata = [{
                "CDM_SOURCE_NAME": "All of Us EHR Ops",
                "CDM_SOURCE_ABBREVIATION": "AoU",
                "CDM_HOLDER": "All of Us",
                "SOURCE_DESCRIPTION":
                "EHR Data for All of Us Research Program",
                "SOURCE_DOCUMENTATION_REFERENCE": "",
                "CDM_ETL_REFERENCE": "",
                "SOURCE_RELEASE_DATE": "2019-06-11",
                "CDM_RELEASE_DATE": "2019-08-01",
                "CDM_VERSION": "v5.3.1",
                "VOCABULARY_VERSION": "v5.0 17-JUN-19",
                "DQD_VERSION": "1.0.0"
            }]

            all_results = {
                'startTimestamp': start_time.isoformat(),
                'endTimestamp': end_time.isoformat(),
                'executionTime': f"{delta} seconds",
                'CheckResults':
                json.loads(check_results.to_json(orient='records')),
                'Overview': overview,
                'Metadata': metadata
            }

            # Write DQD output to file
            self.write_results_to_json(all_results)

            if write_to_csv:
                check_results.to_csv(
                    Path(self.output_folder) /
                    Path(self.output_file).with_suffix('.csv'),
                    index=False)

    def run_check(self, check_description, table_checks, field_checks,
                  concept_checks, cdm_database_schema, vocab_database_schema,
                  output_folder, sql_only):
        """Run an individual Data Quality check

        Args:
            check_description (dict): A dictionary containing details about the check
            table_checks (pandas.DataFrame): A dataframe containing tabel-level checks to run
            field_checks (pandas.DataFrame): A dataframe containing field-level checks to run
            concept_checks (pandas.DataFrame): A datafram contain concept-level checks to run
            cdm_database_schema (str): Schema name of CDM tables
            vocab_database_schema (str): Schema name of Vocab tables (often the same as cdm_database_schema)
            output_folder (str): Output directory of DQD results
            sql_only (bool): Determines if DQD queries or executed (False) or only formatted and output to files (True)

        Raises:
            ValueError: Raised if the checkLevel passed in is not one of ('TABLE', 'FIELD', 'CONCEPT)

        Returns:
            pandas.DataFrame: A Dataframe containing outcomes of DQ checks.
        """
        sql_file = check_description['sqlFile']

        evaluation_filter = check_description['evaluationFilter']

        # Execute evaluations as partial SQL
        if check_description['checkLevel'] == 'TABLE':
            checks = table_checks[table_checks.eval(f'{evaluation_filter}')]
        elif check_description['checkLevel'] == 'FIELD':
            checks = field_checks[field_checks.eval(f'{evaluation_filter}')]
        elif check_description['checkLevel'] == 'CONCEPT':
            checks = concept_checks[concept_checks.eval(
                f'{evaluation_filter}')]
        else:
            raise ValueError('Invalid check level')

        if sql_only:
            (Path(output_folder) /
             f"{check_description['checkName']}.sql").unlink(missing_ok=True)

        if len(checks) > 0:
            # checks = checks.head(1)
            template = TEMPLATE_ENV.get_template(str(sql_file))
            dfs = []
            pbar = tqdm(list(checks.iterrows()), position=1, leave=False)
            for _, check in pbar:

                if 'cdmFieldName' not in check.index:
                    check['cdmFieldName'] = None
                if 'conceptId' not in check.index:
                    check['conceptId'] = None
                if 'unitConceptId' not in check.index:
                    check['unitConceptId'] = None

                pbar.set_description(
                    self.get_check_id(check_description['checkLevel'],
                                      check_description['checkName'],
                                      check["cdmTableName"],
                                      check["cdmFieldName"],
                                      check["conceptId"],
                                      check["unitConceptId"]), )

                # Lower names of all expected variables of checks
                lowered_variables = [
                    'cdmTableName', 'cdmFieldName', 'fkTableName',
                    'fkFieldName', 'standardConceptFieldName',
                    'plausibleTemporalAfterTableName',
                    'plausibleTemporalAfterFieldName'
                ]
                variables = zip(check.index, check.values)
                variables = [(var[0],
                              var[1].lower()) if var[0] in lowered_variables
                             and type(var[1]) == str else (var[0], var[1])
                             for var in variables]
                # variables = [
                #     (var[0],
                #      f"unioned_ehr_{var[1]}") if var[0] in ('cdmTableName',
                #                                             'fkTableName') else
                #     (var[0], var[1]) for var in variables
                # ]

                # Render formatted JINJA-templated queries with variables.

                sql = template.render(
                    cdmDatabaseSchema=f'"{cdm_database_schema.lower()}"',
                    vocabDatabaseSchema=f'"{vocab_database_schema.lower()}"',
                    **dict(variables))

                # Use custom dialect to transpile input sql_server SQL to target DBMS
                sql = sqlglot.transpile(sql,
                                        'tsql_extension',
                                        self.dbms,
                                        pretty=True)[0]

                # TODO Determine if there is a better way to maintain semicolon of transpilation
                sql = f"{sql}\n;\n"

                if sql_only:
                    Path(output_folder).mkdir(exist_ok=True)
                    with open(
                            Path(output_folder) /
                            f"{check_description['checkName']}.sql", 'a') as f:
                        f.write(sql)

                    dfs.append(pd.DataFrame())

                else:
                    # Execute query and process results
                    dfs.append(
                        self.process_check(check, check_description, sql))

            return pd.concat(dfs)
        else:
            logging.info('Evaluation resulted in no checks, ',
                         evaluation_filter)
            return pd.DataFrame()

    def process_check(self, check, check_description, sql):
        """Execute query check and process its results

        Args:
            check (pandas.Series): A pandas Series containing check information
            check_description (pandas.Series): A pandas Series containing check information
            sql (str): Formatted SQL query to execute

        Returns:
            pandas.DataFrame: DataFrame containing check results
        """
        start = datetime.datetime.now()

        # TODO Report errors to a file
        error_report_file = Path(
            self.output_folder
        ) / f"{check_description['checkLevel']}_{check_description['checkName']}_{check['cdmTableName']}.txt"

        try:
            # Execute query
            result = pd.read_sql(sql, self.engine)
            end = datetime.datetime.now()
            delta = (end - start).seconds

            return self.record_result(check,
                                      check_description,
                                      sql,
                                      result=result,
                                      execution_time=f"{delta} seconds")
        except Exception as e:
            msg = getattr(e, 'message', str(e))
            logging.error(
                f"[Level: {check_description['checkLevel']}] [Check: {check_description['checkName']}] [CDM Table: {check['cdmTableName']}]\n {msg}"
            )
            return self.record_result(check, check_description, sql, error=msg)

    def record_result(self,
                      check,
                      check_description,
                      sql,
                      result=None,
                      execution_time=None,
                      warning=None,
                      error=None):
        """Records DQD results in format expected for processing

        Args:
            check (pandas.Series)
            check_description (pandas.Series)
            sql (str): Executed SQL query
            result (pandas.DataFrame, optional): Results infomration. Defaults to None.
            execution_time (int, optional): Elapsed time in seconds of execution. Defaults to None.
            warning (str, optional): A warning encountered during execution. Defaults to None.
            error (str, optional): An error encountered during execution. Defaults to None.

        Returns:
            pandas.DataFrame: DataFrame containing check results in expected format
        """

        columns = check.index
        original_templated_check_description = check_description[
            'checkDescription']
        templated_check_description = re.sub(
            '@(\w+)', lambda match: f'{{{{{match.group(1)}}}}}',
            original_templated_check_description)

        untemplated_check_description = TEMPLATE_ENV.from_string(
            templated_check_description).render(
                **dict(zip(check.index, check.values)))

        report_result = {
            "NUM_VIOLATED_ROWS":
            None,
            "PCT_VIOLATED_ROWS":
            None,
            "NUM_DENOMINATOR_ROWS":
            None,
            "EXECUTION_TIME":
            execution_time,
            "QUERY_TEXT":
            sql,
            "CHECK_NAME":
            check_description['checkName'],
            "CHECK_LEVEL":
            check_description['checkLevel'],
            "CHECK_DESCRIPTION":
            untemplated_check_description,
            "CDM_TABLE_NAME":
            check["cdmTableName"],
            "CDM_FIELD_NAME":
            check["cdmFieldName"],
            "CONCEPT_ID":
            check["conceptId"],
            "UNIT_CONCEPT_ID":
            check["unitConceptId"],
            "SQL_FILE":
            check_description['sqlFile'],
            "CATEGORY":
            check_description['kahnCategory'],
            "SUBCATEGORY":
            check_description['kahnSubcategory'],
            "CONTEXT":
            check_description['kahnContext'],
            "WARNING":
            warning,
            "ERROR":
            error,
            "checkId":
            self.get_check_id(check_description['checkLevel'],
                              check_description['checkName'],
                              check["cdmTableName"], check["cdmFieldName"],
                              check["conceptId"], check["unitConceptId"]),
            # row.names : NULL, stringsAsFactors : FALSE
        }

        if result is not None:
            report_result['NUM_VIOLATED_ROWS'] = result[
                'num_violated_rows'].iloc[0]
            report_result['PCT_VIOLATED_ROWS'] = result[
                'pct_violated_rows'].iloc[0]
            report_result['NUM_DENOMINATOR_ROWS'] = result[
                'num_denominator_rows'].iloc[0]

        report_results = [report_result]

        return pd.DataFrame.from_records(report_results)

    def get_check_id(self,
                     check_level,
                     check_name,
                     cdm_table_name,
                     cdm_field_name=None,
                     concept_id=None,
                     unit_concept_id=None):
        """Create a unique check_id based on check attributes

        Args:
            check_level (str)
            check_name (str)
            cdm_table_name (str)
            cdm_field_name (str, optional): Defaults to None.
            concept_id (int, optional): Defaults to None.
            unit_concept_id (int, optional):  Defaults to None.

        Returns:
            str: Concatenated string of check attributes
        """
        items = [
            check_level, check_name, cdm_table_name, cdm_field_name,
            concept_id, unit_concept_id
        ]
        items = [item for item in items if item != None]
        items = [str(item).replace(' ', '') for item in items]
        # items = [item.replace('', None) for item in items if item != None]
        items = [item for item in items if item]

        return '_'.join(items).lower()

    def _evaluate_thresholds(self, check_results, table_checks, field_checks,
                             concept_checks):
        check_results.reset_index(inplace=True, drop=True)
        check_results = check_results.replace(r'^\s*$', np.nan, regex=True)

        check_results['FAILED'] = 0
        check_results['PASSED'] = 0
        check_results['IS_ERROR'] = 0
        check_results['NOT_APPLICABLE'] = 0
        check_results['NOT_APPLICABLE_REASON'] = None
        check_results['THRESHOLD_VALUE'] = None
        check_results['NOTES_VALUE'] = None

        for i in range(len(check_results)):
            threshold_field = f"{check_results.iloc[i]['CHECK_NAME']}Threshold"
            notes_field = f"{check_results.iloc[i]['CHECK_NAME']}Notes"

            if check_results.iloc[i]['CHECK_LEVEL'] == 'TABLE':
                threshold_field_exists = threshold_field in table_checks
            elif check_results.iloc[i]['CHECK_LEVEL'] == 'FIELD':
                threshold_field_exists = threshold_field in field_checks
            elif check_results.iloc[i]['CHECK_LEVEL'] == 'CONCEPT':
                threshold_field_exists = threshold_field in concept_checks
            else:
                logging.error('Invalid check level')
                exit(1)

            if not threshold_field_exists:
                threshold_value = None
                notes_value = None
            else:
                if check_results.iloc[i]['CHECK_LEVEL'] == 'TABLE':
                    threshold_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}'"
                    notes_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}'"
                elif check_results.iloc[i]['CHECK_LEVEL'] == 'FIELD':
                    threshold_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}' & cdmFieldName == '{check_results.iloc[i]['CDM_FIELD_NAME']}'"
                    notes_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}' & cdmFieldName == '{check_results.iloc[i]['CDM_FIELD_NAME']}'"
                elif check_results.iloc[i]['CHECK_LEVEL'] == 'CONCEPT':
                    if pd.isna(check_results.iloc[i]['UNIT_CONCEPT_ID']):
                        threshold_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}' & \
                            cdmFieldName == '{check_results.iloc[i]['CDM_FIELD_NAME']}' & \
                            conceptId ==  {check_results.iloc[i]['CONCEPT_ID']}"

                        notes_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}' & \
                            cdmFieldName == '{check_results.iloc[i]['CDM_FIELD_NAME']}' & \
                            conceptId ==  {check_results.iloc[i]['CONCEPT_ID']}"

                    else:
                        threshold_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}' & \
                            cdmFieldName == '{check_results.iloc[i]['CDM_FIELD_NAME']}' & \
                            conceptId ==  {check_results.iloc[i]['CONCEPT_ID']} & \
                            unitConceptId == {check_results.iloc[i]['UNIT_CONCEPT_ID'].astype(int)} "

                        notes_filter = f"cdmTableName == '{check_results.iloc[i]['CDM_TABLE_NAME']}' & \
                            cdmFieldName == '{check_results.iloc[i]['CDM_FIELD_NAME']}' & \
                            conceptId ==  {check_results.iloc[i]['CONCEPT_ID']} & \
                            unitConceptId == {check_results.iloc[i]['UNIT_CONCEPT_ID'].astype(int)} "

                if check_results.iloc[i]['CHECK_LEVEL'] == 'TABLE':
                    threshold_value = table_checks[table_checks.eval(
                        threshold_filter)][threshold_field].item()
                    notes_value = table_checks[table_checks.eval(
                        notes_filter)][notes_field].item()
                elif check_results.iloc[i]['CHECK_LEVEL'] == 'FIELD':
                    threshold_value = field_checks[field_checks.eval(
                        threshold_filter)][threshold_field].item()
                    notes_value = field_checks[field_checks.eval(
                        notes_filter)][notes_field].item()
                elif check_results.iloc[i]['CHECK_LEVEL'] == 'CONCEPT':
                    threshold_value = concept_checks[concept_checks.eval(
                        threshold_filter)][threshold_field].item()
                    notes_value = concept_checks[concept_checks.eval(
                        notes_filter)][notes_field].item()

                if threshold_value:
                    threshold_value = float(threshold_value)

                check_results.at[i, 'THRESHOLD_VALUE'] = threshold_value
                check_results.at[i, 'NOTES_VALUE'] = notes_value

            if not pd.isna(check_results.iloc[i]['ERROR']):
                check_results.at[i, "IS_ERROR"] = 1
            elif pd.isna(threshold_value) or threshold_value == 0:
                # If no threshold, or threshold is 0%, then any violating records will cause this check to fail
                if not pd.isna(
                        check_results.at[i, 'NUM_VIOLATED_ROWS']
                ) and check_results.iloc[i]['NUM_VIOLATED_ROWS'] > 0:
                    check_results.at[i, 'FAILED'] = 1
            elif check_results.iloc[i][
                    'PCT_VIOLATED_ROWS'] * 100 > threshold_value:
                check_results.at[i, 'FAILED'] = 1

        missing_tables = check_results[
            (check_results['CHECK_NAME'] == "cdmTable")
            & (check_results['FAILED'] == 1)]['CDM_TABLE_NAME']

        if len(missing_tables) > 0:
            missing_tables['TABLE_IS_MISSING'] = 1
            check_results = check_results.merge(missing_tables,
                                                how='left',
                                                on='CDM_TABLE_NAME')

            mask = (check_results['CHECK_NAME'] !=
                    "cdmTable") & (check_results['IS_ERROR'] == 0)
            check_results['TABLE_IS_MISSING'] = np.where(
                mask, check_results['TABLE_IS_MISSING'], pd.NA)
        else:
            check_results['TABLE_IS_MISSING'] = pd.NA

        missing_fields = check_results[
            (check_results['CHECK_NAME'] == "cdmField")
            & (check_results['FAILED'] == 1)
            & (check_results['TABLE_IS_MISSING'].isna())][[
                'CDM_TABLE_NAME', 'CDM_FIELD_NAME'
            ]]

        if len(missing_fields) > 0:
            missing_fields['FIELD_IS_MISSING'] = 1
            check_results = check_results.merge(
                missing_fields,
                how='left',
                on=['CDM_TABLE_NAME', 'CDM_FIELD_NAME'])
            mask = (check_results['CHECK_NAME'] !=
                    "cdmField") & (check_results['IS_ERROR'] == 0)
            check_results['FIELD_IS_MISSING'] = np.where(
                mask, check_results['FIELD_IS_MISSING'], pd.NA)
        else:
            check_results['FIELD_IS_MISSING'] = pd.NA

        empty_tables = check_results[
            (check_results['CHECK_NAME'] == 'measureValueCompleteness')
            & (check_results['NUM_DENOMINATOR_ROWS'] == 0) &
            (check_results['IS_ERROR'] == 0) &
            (check_results['TABLE_IS_MISSING'].isna()) &
            (check_results['FIELD_IS_MISSING'].isna()
             )]['CDM_TABLE_NAME'].unique()

        if len(empty_tables) > 0:
            empty_tables['TABLE_IS_EMPTY'] = 1

            check_results = check_results.merge(empty_tables,
                                                how='left',
                                                on=['CDM_TABLE_NAME'])
            mask = (check_results['CHECK_NAME'] != "cdmField") & (
                check_results['CHECK_NAME'] !=
                "cdmTable") & (check_results['IS_ERROR'] == 0)
            check_results['TABLE_IS_EMPTY'] = np.where(
                mask, check_results['TABLE_IS_EMPTY'], pd.NA)
        else:
            check_results['TABLE_IS_EMPTY'] = pd.NA

        empty_fields = check_results[
            (check_results['CHECK_NAME'] == 'measureValueCompleteness')
            & (check_results['NUM_DENOMINATOR_ROWS'] ==
               check_results['NUM_VIOLATED_ROWS']) &
            (check_results['TABLE_IS_MISSING'].isna()) &
            (check_results['FIELD_IS_MISSING'].isna()) &
            (check_results['TABLE_IS_EMPTY'].isna())][[
                'CDM_TABLE_NAME', 'CDM_FIELD_NAME'
            ]]

        if len(empty_fields) > 0:
            empty_fields['FIELD_IS_EMPTY'] = 1

            check_results = check_results.merge(
                empty_fields,
                how='left',
                on=['CDM_TABLE_NAME', 'CDM_FIELD_NAME'])
            mask = (check_results['CHECK_NAME'] != "measureValueCompleteness"
                    ) & (check_results['CHECK_NAME'] != "cdmField") & (
                        check_results['CHECK_NAME'] !=
                        "isRequired") & (check_results['IS_ERROR'] == 0)
            check_results['FIELD_IS_EMPTY'] = np.where(
                mask, check_results['FIELD_IS_EMPTY'], pd.NA)
        else:
            check_results['FIELD_IS_EMPTY'] = pd.NA

        mask = (check_results['IS_ERROR']
                == 0) & (check_results['TABLE_IS_MISSING'].isna()) & (
                    check_results['FIELD_IS_MISSING'].isna()
                ) & (check_results['TABLE_IS_EMPTY'].isna()) & (
                    check_results['FIELD_IS_EMPTY'].isna()) & (
                        check_results['CHECK_LEVEL'] == 'CONCEPT') & (
                            check_results['UNIT_CONCEPT_ID'].isna()) & (
                                check_results['NUM_DENOMINATOR_ROWS'] == 0)

        check_results['CONCEPT_IS_MISSING'] = np.where(mask, 1, pd.NA)

        mask = (check_results['IS_ERROR']
                == 0) & (check_results['TABLE_IS_MISSING'].isna()) & (
                    check_results['FIELD_IS_MISSING'].isna()
                ) & (check_results['TABLE_IS_EMPTY'].isna()) & (
                    check_results['FIELD_IS_EMPTY'].isna()) & (
                        check_results['CHECK_LEVEL'] == 'CONCEPT') & (
                            ~check_results['UNIT_CONCEPT_ID'].isna()) & (
                                check_results['NUM_DENOMINATOR_ROWS'] == 0)

        check_results['CONCEPT_AND_UNIT_ARE_MISSING'] = np.where(
            mask, 1, pd.NA)

        cols_to_coalesce = [
            check_results[c] for c in [
                'TABLE_IS_MISSING', 'FIELD_IS_MISSING', 'TABLE_IS_EMPTY',
                'FIELD_IS_EMPTY', 'CONCEPT_IS_MISSING',
                'CONCEPT_AND_UNIT_ARE_MISSING'
            ]
        ]
        coalesced = reduce(lambda acc, col: acc.combine_first(col),
                           cols_to_coalesce)
        not_applicable = coalesced.fillna(0)
        check_results['NOT_APPLICABLE'] = not_applicable

        condlist = [
            ~check_results['TABLE_IS_MISSING'].isna(),
            ~check_results['FIELD_IS_MISSING'].isna(),
            ~check_results['TABLE_IS_EMPTY'].isna(),
            ~check_results['FIELD_IS_EMPTY'].isna(),
            ~check_results['CONCEPT_IS_MISSING'].isna(),
            ~check_results['CONCEPT_AND_UNIT_ARE_MISSING'].isna()
        ]

        def case_when(row, conds, thens):
            for i, cond in enumerate(conds):
                if row['index'] in cond[cond].index.values:
                    then = thens[i].format(row)
                    return then

        check_results['index'] = check_results.index
        thens = [
            "Table {0[CDM_TABLE_NAME]} does not exist.",
            "Field {0[CDM_TABLE_NAME]}.{0[CDM_FIELD_NAME]} does not exist.",
            "Table {0[CDM_TABLE_NAME]} is empty.",
            "Field {0[CDM_TABLE_NAME]}.{0[CDM_FIELD_NAME]} is not populated.",
            "{0[CDM_FIELD_NAME]}={0[CONCEPT_ID]} is missing from the {0[CDM_TABLE_NAME]} table.",
            "Combination of {0[CDM_FIELD_NAME]}={0[CONCEPT_ID]}, UNIT_CONCEPT_ID={0[UNIT_CONCEPT_ID]} and VALUE_AS_NUMBER IS NOT NULL is missing from the {0[CDM_TABLE_NAME]} table."
        ]

        check_results['NOT_APPLICABLE_REASON'] = check_results.apply(
            case_when, args=(condlist, thens), axis=1)
        check_results.drop(columns='index', inplace=True)

        check_results.drop(columns=[
            'TABLE_IS_MISSING', 'FIELD_IS_MISSING', 'TABLE_IS_EMPTY',
            'FIELD_IS_EMPTY', 'CONCEPT_IS_MISSING',
            'CONCEPT_AND_UNIT_ARE_MISSING'
        ],
                           inplace=True)

        mask = check_results['NOT_APPLICABLE'] == 1
        check_results['FAILED'] = np.where(mask, 0, check_results['FAILED'])

        mask = (check_results['FAILED']
                == 0) & (check_results['IS_ERROR']
                         == 0) & (check_results['NOT_APPLICABLE'] == 0)
        check_results['PASSED'] = np.where(mask, 1, 0)

        return check_results

    def evaluate_thresholds(self, check_results, table_checks, field_checks,
                            concept_checks):

        return self._evaluate_thresholds(check_results, table_checks,
                                         field_checks, concept_checks)

    def summarize_results(self, check_results):
        countTotal = len(check_results)
        countThresholdFailed = len(
            check_results[(check_results['FAILED'] == 1)
                          & (check_results['ERROR'].isna())])
        countErrorFailed = len(check_results[(~check_results['ERROR'].isna())])
        countOverallFailed = len(check_results[(check_results['FAILED'] == 1)])
        countPassed = countTotal - countOverallFailed
        countTotalPlausibility = len(
            check_results[(check_results['CATEGORY'] == 'Plausibility')])
        countTotalConformance = len(
            check_results[(check_results['CATEGORY'] == 'Conformance')])
        countTotalCompleteness = len(
            check_results[(check_results['CATEGORY'] == 'Completeness')])
        countFailedPlausibility = len(
            check_results[(check_results['CATEGORY'] == 'Plausibility')
                          & (check_results['FAILED'] == 1)])
        countFailedConformance = len(
            check_results[(check_results['CATEGORY'] == 'Conformance')
                          & (check_results['FAILED'] == 1)])
        countFailedCompleteness = len(
            check_results[(check_results['CATEGORY'] == 'Completeness')
                          & (check_results['FAILED'] == 1)])
        countPassedPlausibility = len(
            check_results[(check_results['CATEGORY'] == 'Plausibility')
                          & (check_results['PASSED'] == 1)])
        countPassedConformance = len(
            check_results[(check_results['CATEGORY'] == 'Conformance')
                          & (check_results['PASSED'] == 1)])
        countPassedCompleteness = len(
            check_results[(check_results['CATEGORY'] == 'Completeness')
                          & (check_results['PASSED'] == 1)])

        return {
            'countTotal': countTotal,
            'countThresholdFailed': countThresholdFailed,
            'countErrorFailed': countErrorFailed,
            'countOverallFailed': countOverallFailed,
            'countPassed': countPassed,
            'countTotalPlausibility': countTotalPlausibility,
            'countTotalConformance': countTotalConformance,
            'countTotalCompleteness': countTotalCompleteness,
            'countFailedPlausibility': countFailedPlausibility,
            'countFailedConformance': countFailedConformance,
            'countFailedCompleteness': countFailedCompleteness,
            'countPassedPlausibility': countPassedPlausibility,
            'countPassedConformance': countPassedConformance,
            'countPassedCompleteness': countPassedCompleteness
        }

    def write_results_to_json(self, result):
        result_filename = Path(self.output_folder) / self.output_file
        result['outputFile'] = self.output_file

        logging.info(f"Writing results to {result_filename}")
        with open(result_filename, 'w') as f:
            # result.to_json(result_filename)
            json.dump(result, f, indent=4)


def main(dbms,
         dbms_conn_params,
         output_folder="app/",
         output_file="results.json",
         check_names=[],
         tables_to_include=[],
         sql_only=False):

    dqd = DQD(dbms,
              dbms_conn_params,
              output_folder=output_folder,
              output_file=output_file,
              check_names=check_names,
              tables_to_include=tables_to_include)

    dqd.execute(sql_only=sql_only, write_to_csv=False)


if __name__ == '__main__':
    check_descriptions = pd.read_csv(
        './DataQualityDashboard/inst/csv/OMOP_CDMv5.3_Check_Descriptions.csv',
        keep_default_na=False)

    parser = argparse.ArgumentParser(
        description=
        'Execute the OHDSI Data Quality Dashboard on an OMOP instance')

    common_args = [(('--output_folder', ), {
        'default': 'app/',
        'required': False,
        'help': 'Folder to save DQD output'
    }),
                   (('--output_file', ), {
                       'default': 'results.json',
                       'required': False,
                       'help': 'Name of output json file to save results.'
                   }),
                   (('--check_names', ), {
                       'nargs': "*",
                       'choices': list(check_descriptions['checkName']),
                       'default': [],
                       'help': 'Subgroup of checks to run'
                   }),
                   (('--tables_to_include', ), {
                       'nargs': "*",
                       'default': [],
                       'help': "Subgroup of OMOP tables to include in checks"
                   }), (('--sql_only', ), {
                       'action': 'store_true'
                   })]

    subparsers = parser.add_subparsers(
        title='dbms',
        description='Database Management System',
        help='dbms',
        dest='dbms')
    subparsers_dict = {}
    for supported_dbms in DBMS_NAMES:
        subparser = subparsers.add_parser(supported_dbms)
        subparsers_dict[supported_dbms] = subparser

    for subparser_name in subparsers_dict:
        if subparser_name in ['bigquery']:
            subparsers_dict[subparser_name].add_argument(
                'project_id', help='BigQuery project id')
            subparsers_dict[subparser_name].add_argument(
                'dataset_id', help='BigQuery dataset id')
        elif subparser_name in ['tsql', 'sqlite']:
            subparsers_dict[subparser_name].add_argument(
                'db_server', help='Database Server')
            subparsers_dict[subparser_name].add_argument('database',
                                                         help='Database Name')
            subparsers_dict[subparser_name].add_argument(
                'schema', help='Database Schema')
            subparsers_dict[subparser_name].add_argument('user',
                                                         help='Database User')
            subparsers_dict[subparser_name].add_argument('--port',
                                                         help='Database port')
            subparsers_dict[subparser_name].add_argument(
                '--passwd', help='Database passwd')
        else:
            subparsers_dict[subparser_name].add_argument(
                'db_server', help='Database Server')
            subparsers_dict[subparser_name].add_argument('database',
                                                         help='Database Name')
            subparsers_dict[subparser_name].add_argument(
                'schema', help='Database Schema')
            subparsers_dict[subparser_name].add_argument('user',
                                                         help='Database User')
            subparsers_dict[subparser_name].add_argument('--port',
                                                         help='Database port')
            subparsers_dict[subparser_name].add_argument(
                '--passwd', help='Database passwd')
        for common_arg in common_args:
            subparsers_dict[subparser_name].add_argument(
                *common_arg[0], **common_arg[1])

    args = parser.parse_args()

    if args.dbms in ['bigquery']:
        dbms_params = {
            'project_id': args.project_id,
            'dataset_id': args.dataset_id
        }
    elif args.dbms in ['tsql', 'sqlite']:
        dbms_params = {
            'db_server': args.db_server,
            'database': args.database,
            'user': args.user,
            'port': args.port,
            'passwd': args.passwd,
            'schema': args.schema
        }
    else:
        dbms_params = {
            'db_server': args.db_server,
            'database': args.database,
            'user': args.user,
            'port': args.port,
            'passwd': args.passwd,
            'schema': args.schema
        }

    main(args.dbms,
         dbms_params,
         output_folder=args.output_folder,
         output_file=args.output_file,
         check_names=args.check_names,
         tables_to_include=args.tables_to_include,
         sql_only=args.sql_only)
