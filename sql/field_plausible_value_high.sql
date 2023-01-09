
/*********
PLAUSIBLE_VALUE_HIGH
get number of records and the proportion to total number of eligible records that exceed this threshold

Parameters used in this template:
cdmDatabaseSchema = {{cdmDatabaseSchema}}
cdmTableName = {{cdmTableName}}
cdmFieldName = {{cdmFieldName}}
plausibleValueHigh = {{plausibleValueHigh}}
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
    from {{cdmDatabaseSchema}}.{{cdmTableName}} cdmTable
    {% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
    	JOIN {{cohortDatabaseSchema}}.COHORT c 
    	ON cdmTable.PERSON_ID = c.SUBJECT_ID
    	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
    	{% endif %}   
            
    {% if {{cdmDatatype}} == "datetime" | {{cdmDatatype}} == "date" %}
      where cast(cdmTable.{{cdmFieldName}} as date) > cast({{plausibleValueHigh}} as date)
    {% else %}
      where cdmTable.{{cdmFieldName}} > {{plausibleValueHigh}}
    {% endif %}   
            
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
(
	SELECT COUNT_BIG(*) AS num_rows
	FROM {{cdmDatabaseSchema}}.{{cdmTableName}} cdmTable
	{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
    	JOIN {{cohortDatabaseSchema}}.COHORT c 
    	ON cdmTable.PERSON_ID = c.SUBJECT_ID
    	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
    	{% endif %}   
            
  where {{cdmFieldName}} is not null
) denominator
;
