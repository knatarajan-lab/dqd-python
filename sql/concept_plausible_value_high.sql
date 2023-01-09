
/*********
CONCEPT LEVEL check:
PLAUSIBLE_VALUE_HIGH - find any MEASUREMENT records that have VALUE_AS_NUMBER with non-null value > plausible high value

Parameters used in this template:
cdmDatabaseSchema = {{cdmDatabaseSchema}}
cdmTableName = {{cdmTableName}}
cdmFieldName = {{cdmFieldName}}
conceptId = {{conceptId}}
unitConceptId = {{unitConceptId}}
plausibleValueHigh = {{plausibleValueHigh}}
{% if {{cohort}} %}
cohortDefinitionId = {{cohortDefinitionId}}
cohortDatabaseSchema = {{cohortDatabaseSchema}}
{% endif %}   
            
**********/


SELECT num_violated_rows, CASE WHEN denominator.num_rows = 0 THEN 0 ELSE 1.0*num_violated_rows/denominator.num_rows END  AS pct_violated_rows,
  denominator.num_rows as num_denominator_rows
FROM
(
	SELECT COUNT_BIG(*) AS num_violated_rows
	FROM
	(
		/*violatedRowsBegin*/
		SELECT m.* 
		FROM {{cdmDatabaseSchema}}.{{cdmTableName}} m
		{% if {{cohort}} %}
  	JOIN {{cohortDatabaseSchema}}.COHORT c
  	ON m.PERSON_ID = c.SUBJECT_ID
  	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
  	{% endif %}   
            
		WHERE m.{{cdmFieldName}} = {{conceptId}}
		AND m.unit_concept_id = {{unitConceptId}}
		AND m.value_as_number IS NOT NULL
		AND m.value_as_number > {{plausibleValueHigh}}
		/*violatedRowsEnd*/
	) violated_rows
) violated_row_count,
( 
	SELECT COUNT_BIG(*) AS num_rows
	FROM {{cdmDatabaseSchema}}.{{cdmTableName}} m
	{% if {{cohort}} %}
	JOIN {{cohortDatabaseSchema}}.COHORT c
	ON m.PERSON_ID = c.SUBJECT_ID
	AND c.COHORT_DEFINITION_ID = {{cohortDefinitionId}}
	{% endif %}   
            
	WHERE m.{{cdmFieldName}} = {{conceptId}}
	AND unit_concept_id = {{unitConceptId}}
	AND value_as_number IS NOT NULL
) denominator
;