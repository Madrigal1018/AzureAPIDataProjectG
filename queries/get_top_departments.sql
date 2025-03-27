WITH CTE AS (
    SELECT h.*,
        d.department,
        j.job,
        YEAR(CAST(LEFT(datetime, 10) AS DATE)) AS YearOnly
    FROM hired_employees h
    LEFT JOIN departments d ON h.department_id = d.id
    LEFT JOIN jobs j ON h.job_id = j.id
),
DepartmentHires AS (
    SELECT department_id AS id, department, COUNT(*) AS hired
    FROM CTE
    WHERE YearOnly = 2021
    GROUP BY department_id, department
)
SELECT *
FROM DepartmentHires
WHERE hired > (
    SELECT AVG(hired * 1.0)  -- multiply by 1.0 to ensure decimal division
    FROM DepartmentHires
)
ORDER BY hired DESC;