WITH CTE AS (
    SELECT h.*,
        d.department,
        j.job,
        YEAR(CAST(LEFT(datetime, 10) AS DATE)) AS YearOnly,
        MONTH(CAST(LEFT(datetime, 10) AS DATE)) AS MonthNumber
    FROM hired_employees h
    LEFT JOIN departments d ON h.department_id = d.id
    LEFT JOIN jobs j ON h.job_id = j.id
),
CTE2 AS (
    SELECT *, 
    CASE
        WHEN MonthNumber BETWEEN 1 AND 3 THEN 'Q1'
        WHEN MonthNumber BETWEEN 4 AND 6 THEN 'Q2'
        WHEN MonthNumber BETWEEN 7 AND 9 THEN 'Q3'
        WHEN MonthNumber BETWEEN 10 AND 12 THEN 'Q4'
    END AS Quarter
    FROM CTE
    WHERE YEARONLY = 2021
),
CTE3 AS (
    SELECT department, job, Quarter, COUNT(*) AS HiredCount
    FROM CTE2
    GROUP BY department, job, Quarter
)
SELECT 
    department,
    job,
    ISNULL([Q1], 0) AS Q1,
    ISNULL([Q2], 0) AS Q2,
    ISNULL([Q3], 0) AS Q3,
    ISNULL([Q4], 0) AS Q4
FROM CTE3
PIVOT (
    SUM(HiredCount)
    FOR Quarter IN ([Q1], [Q2], [Q3], [Q4])
) AS PivotTable
ORDER BY department,job;