--currently active employees
SELECT COUNT(*) cnt FROM employee_t2
    WHERE TRUE
    AND is_active
    AND to_date = '9999-12-31';  --currently


--active employees on day 1995-12-01
SELECT COUNT(DISTINCT employee_id) cnt
    FROM employee_t2
    WHERE TRUE
    AND is_active
    AND from_date <= '1995-12-01'
    AND to_date >= '1995-12-01' ;

--active employees in all of 1995
SELECT COUNT(DISTINCT employee_id) cnt FROM employee_t2
    WHERE TRUE
    AND is_active
    AND YEAR(from_date) <= 1995
    AND YEAR(to_date) >= 1995 ;


--active employees on day 1995-12-01
--who were hired in Q1 of 1994
SELECT COUNT(DISTINCT employee_id) cnt
    FROM employee_t2
    WHERE TRUE
    AND is_active
    AND hire_date BETWEEN '1994-01-01' AND '1994-03-31'
    AND from_date <= '1995-12-01'
    AND to_date >= '1995-12-01';


--active employees on day 1995-12-01
--who were hired in Q1 of 1994
--and received a promotion
WITH promotions AS (
    SELECT DISTINCT employee_id FROM employee_t2
    WHERE TRUE
    AND last_change = 'Promoted' )
    SELECT COUNT(DISTINCT employee_id) cnt
    FROM employee_t2
    INNER JOIN promotions USING (employee_id)
    WHERE TRUE
    AND is_active
    AND hire_date BETWEEN '1994-01-01' AND '1994-03-31'
    AND from_date <= '1995-12-01'
    AND to_date >= '1995-12-01';


--what are the total changes per day by change type
--since the first load ( excluding 1995-12-01)
SELECT from_date, last_change,  COUNT( employee_id) cnt
    FROM employee_t2
    WHERE TRUE
    AND from_date > '1995-12-01'
    AND to_date = '9999-12-31'  --currently
    GROUP BY 1,2
    ORDER BY 1,2;
