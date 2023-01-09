
/*********
IS_FOREIGN_KEY
Foreign key check

Parameters used in this template:
cdmDatabaseSchema = {{cdmDatabaseSchema}}
{% if '{{fkTableName}}' IN ('CONCEPT','DOMAIN') %}vocabDatabaseSchema = {{vocabDatabaseSchema}}{% endif %}   
            
cdmTableName = {{cdmTableName}}
cdmFieldName = {{cdmFieldName}}
fkTableName = {{fkTableName}}
fkFieldName = {{fkFieldName}}
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
		{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
    	JOIN {{cohortDatabaseSchema}}.COHORT c 
    	ON cdmTable.PERSON_ID = c.SUBJECT_ID
    	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
    	{% endif %}   
            
		LEFT JOIN {% if '{{fkTableName}}' IN ('CONCEPT','DOMAIN') %}{{vocabDatabaseSchema}}.{{fkTableName}}{% else %}{{cdmDatabaseSchema}}.{{fkTableName}}{% endif %}   
             fkTable
		ON cdmTable.{{cdmFieldName}} = fkTable.{{fkFieldName}}
		WHERE fkTable.{{fkFieldName}} IS NULL AND cdmTable.{{cdmFieldName}} IS NOT NULL
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
            
) denominator
;