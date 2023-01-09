
/*********
PLAUSIBLE_TEMPORAL_AFTER
get number of records and the proportion to total number of eligible records with datetimes that do not occur on or after their corresponding datetimes

Parameters used in this template:
cdmDatabaseSchema = {{cdmDatabaseSchema}}
cdmTableName = {{cdmTableName}}
cdmFieldName = {{cdmFieldName}}
plausibleTemporalAfterTableName = {{plausibleTemporalAfterTableName}}
plausibleTemporalAfterFieldName = {{plausibleTemporalAfterFieldName}}
{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
cohortDefinitionId = {{cohortDefinitionId}}
cohortDatabaseSchema = {{cohortDatabaseSchema}}
{% endif %}   
            
**********/

SELECT num_violated_rows, CASE WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows END  AS pct_violated_rows, 
  denominator.num_rows as num_denominator_rows
FROM
(
	SELECT COUNT_BIG(violated_rows.violating_field) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT '{{cdmTableName}}.{{cdmFieldName}}' AS violating_field, cdmTable.*
    FROM {{cdmDatabaseSchema}}.{{cdmTableName}} cdmTable
    {% if {{cdmDatabaseSchema}}.{{cdmTableName}} != {{cdmDatabaseSchema}}.{{plausibleTemporalAfterTableName}} %}
		JOIN {{cdmDatabaseSchema}}.{{plausibleTemporalAfterTableName}} plausibleTable
			ON cdmTable.person_id = plausibleTable.person_id{% endif %}   
            
		{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
    JOIN {{cohortDatabaseSchema}}.COHORT c 
      ON cdmTable.PERSON_ID = c.SUBJECT_ID
    	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}{% endif %}   
            
    WHERE 
    {% if '{{plausibleTemporalAfterTableName}}' == 'PERSON' %}
		COALESCE(CAST(plausibleTable.{{plausibleTemporalAfterFieldName}} AS DATE),CAST(CONCAT(plausibleTable.YEAR_OF_BIRTH,'-06-01') AS DATE)) 
	{% else %}
		CAST(cdmTable.{{plausibleTemporalAfterFieldName}} AS DATE)
	{% endif %}   
             > CAST(cdmTable.{{cdmFieldName}} AS DATE)
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT COUNT_BIG(*) AS num_rows
	FROM {{cdmDatabaseSchema}}.{{cdmTableName}} cdmTable
	{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
  JOIN {{cohortDatabaseSchema}}.COHORT c 
    ON cdmTable.PERSON_ID = c.SUBJECT_ID
    AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}} {% endif %}   
            
) denominator
;
