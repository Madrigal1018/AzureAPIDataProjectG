from .db import get_connection
import logging

# Query aggregated hiring by department and quarter
def get_hiring_summary():
    query = """
    WITH CTE AS (
        SELECT h.*, d.department, j.job,
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
    ORDER BY department, job
    """

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        return result
    except Exception as e:
        logging.error(f"Failed to fetch hiring summary: {e}")
        raise
    finally:
        conn.close()

# Query departments with above-average hires in 2021
def get_top_departments_by_hiring():
    query = """
    WITH CTE AS (
        SELECT h.*, d.department, j.job,
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
        SELECT AVG(hired * 1.0)
        FROM DepartmentHires
    )
    ORDER BY hired DESC
    """

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        result = [dict(zip(columns, row)) for row in rows]
        return result
    except Exception as e:
        logging.error(f"Failed to fetch top departments: {e}")
        raise
    finally:
        conn.close()