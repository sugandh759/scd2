-- Databricks notebook source
CREATE TABLE Employee_src(
empno int NOT NULL,
emp_name varchar (10),
emp_salary int ,
emp_deptno int ,
emp_compnay varchar (10) ,
load_timestamp timestamp
)

-- COMMAND ----------

insert into Employee_src
values (102, 'Suryansh', 50000, 12,'comp_xyz',getdate())

-- COMMAND ----------

select * from Employee_src

-- COMMAND ----------

CREATE TABLE Employee_tgt(
empno int NOT NULL,
emp_name varchar (25) ,
emp_salary int ,
emp_deptno int ,
emp_compnay varchar (25) ,
load_timestamp TIMESTAMP,
startDate TIMESTAMP,
endDate TIMESTAMP,
current_status BOOLEAN
)

-- COMMAND ----------



-- COMMAND ----------

select * from Employee_tgt

-- COMMAND ----------

-- cetas command
create or replace table updated_emp_src as (select *from Employee_src where load_timestamp >(SELECT 
  CASE 
    WHEN MAX(load_timestamp) IS NULL 
    THEN TO_TIMESTAMP('1999-02-09T16:50:32.781Z') 
    ELSE MAX(load_timestamp) 
  END AS result
FROM Employee_tgt))

-- COMMAND ----------

select * from updated_emp_src

-- COMMAND ----------

SELECT updated_emp_src.empno as mergeKey, updated_emp_src.*
  FROM updated_emp_src

-- COMMAND ----------

SELECT NULL as mergeKey, updated_emp_src.*
  FROM updated_emp_src JOIN Employee_tgt
  ON updated_emp_src.empno = Employee_tgt.empno 
  WHERE Employee_tgt.current_status = true AND (updated_emp_src.emp_salary <> Employee_tgt.emp_salary OR  updated_emp_src.emp_deptno <> Employee_tgt.emp_deptno OR updated_emp_src.emp_compnay <> Employee_tgt.emp_compnay)

-- COMMAND ----------

select * from (SELECT updated_emp_src.empno as mergeKey, updated_emp_src.*
  FROM updated_emp_src

  UNION ALL
    
  SELECT NULL as mergeKey, updated_emp_src.*
  FROM updated_emp_src JOIN Employee_tgt
  ON updated_emp_src.empno = Employee_tgt.empno 
  WHERE Employee_tgt.current_status = true AND (updated_emp_src.emp_salary <> Employee_tgt.emp_salary OR  updated_emp_src.emp_deptno <> Employee_tgt.emp_deptno OR updated_emp_src.emp_compnay <> Employee_tgt.emp_compnay))

-- COMMAND ----------

MERGE INTO Employee_tgt
USING (
  select * from (SELECT updated_emp_src.empno as mergeKey, updated_emp_src.*
  FROM updated_emp_src

  UNION ALL
    
  SELECT NULL as mergeKey, updated_emp_src.*
  FROM updated_emp_src JOIN Employee_tgt
  ON updated_emp_src.empno = Employee_tgt.empno 
  WHERE Employee_tgt.current_status = true AND (updated_emp_src.emp_salary <> Employee_tgt.emp_salary OR  updated_emp_src.emp_deptno <> Employee_tgt.emp_deptno OR updated_emp_src.emp_compnay <> Employee_tgt.emp_compnay))
) staged_updates
on Employee_tgt.empno=staged_updates.mergeKey
WHEN MATCHED AND Employee_tgt.current_status = true AND (staged_updates.emp_salary <> Employee_tgt.emp_salary OR  staged_updates.emp_deptno <> Employee_tgt.emp_deptno OR staged_updates.emp_compnay <> Employee_tgt.emp_compnay) THEN  
  UPDATE SET Employee_tgt.current_status = false, Employee_tgt.endDate = staged_updates.load_timestamp    
WHEN NOT MATCHED THEN 
  INSERT(empno,emp_name,emp_salary,emp_deptno,emp_compnay,load_timestamp,startDate,endDate,current_status) 
  VALUES(staged_updates.empno,staged_updates.emp_name,staged_updates.emp_salary,staged_updates.emp_deptno,staged_updates.emp_compnay,
  staged_updates.load_timestamp,staged_updates.load_timestamp, null,true)

-- COMMAND ----------

select * from Employee_tgt order by empno, emp_compnay

-- COMMAND ----------



-- COMMAND ----------


