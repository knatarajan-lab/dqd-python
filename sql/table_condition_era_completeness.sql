/*********
Table Level:  
MEASURE_CONDITION_ERA_COMPLETENESS
Determine what #/% of persons have condition_era built successfully 
for persons in condition_occurrence table

Parameters used in this template:
cdmTableName = {{cdmTableName}}
{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
cohortDefinitionId = {{cohortDefinitionId}}
cohortDatabaseSchema = {{cohortDatabaseSchema}}
{% endif %}   
            
**********/


SELECT num_violated_rows, CASE WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows END  AS pct_violated_rows, 
  denominator.num_rows as num_denominator_rows
FROM
(
	SELECT COUNT_BIG(violated_rows.person_id) AS num_violated_rows
	FROM
	(
		SELECT DISTINCT co.person_id
		FROM
  	  	{{cdmDatabaseSchema}}.condition_occurrence co
			{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
    		JOIN {{cohortDatabaseSchema}}.COHORT c 
    		ON co.PERSON_ID = c.SUBJECT_ID
    		AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
    		{% endif %}   
            
		LEFT JOIN {{cdmDatabaseSchema}}.{{cdmTableName}} cdmTable
  	  	ON
    	co.person_id = cdmTable.person_id
  	WHERE
  	cdmTable.person_id IS NULL
	) violated_rows
) violated_row_count,
( 
	SELECT COUNT_BIG(DISTINCT person_id) AS num_rows
	FROM {{cdmDatabaseSchema}}.condition_occurrence co
	{% if {{cohort}} & '{{runForCohort}}' == 'Yes' %}
    	JOIN {{cohortDatabaseSchema}}.COHORT c 
    	ON co.PERSON_ID = c.SUBJECT_ID
    	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
    	{% endif %}   
            
) denominator
;