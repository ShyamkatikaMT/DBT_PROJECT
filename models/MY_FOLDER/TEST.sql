WITH CTE AS(
SELECT * FROM TEST_DB.TEST_SCHEMA.TEST_TABLE
)
SELECT {{age_dept('dept') }},{{age('age')}} FROM CTE