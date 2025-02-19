-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Getting Started
-- MAGIC We recommend that you use the Databricks environment to do these exercises in order to have an interactive experience. When you import the .sql file in databricks, the markdown cells are rendered properly, and you can run the code interactively. You can register and use Databricks community edition. Alternatively, you can just use VSCode or other code editor which renders .ipynb files correctly, but then you don't get the interactive experience. Which ever way you choose to do the exercises does not matter, only the answers count.
-- MAGIC
-- MAGIC Be prepared to show your work during the technical interview.
-- MAGIC
-- MAGIC
-- MAGIC ### Access Databricks Commmunity Edition
-- MAGIC Go to https://databricks.com/try-databricks and sign up by filling in your details. Then press sign up and choose Community Edition platform on the right-hand side and press GET STARTED. Confirm your email and you are ready to go.
-- MAGIC
-- MAGIC Here you can find more detailed documentation https://docs.databricks.com/getting-started/try-databricks.html
-- MAGIC
-- MAGIC If possible, please use git and share your exercise results using your own github account (or similar).
-- MAGIC
-- MAGIC ### Starting the exercise in Community Edition
-- MAGIC - create a new notebook
-- MAGIC - from file/import import this file to databricks
-- MAGIC - create a new cluster
-- MAGIC - ramping up the cluster in Databricks Community Edition might take a some time
-- MAGIC - read the instructions carefully and try to answer based on dbt and databricks documentation and implementing the tools features in your answers
-- MAGIC
-- MAGIC ### Resources
-- MAGIC - [Here](https://learn.microsoft.com/en-us/azure/databricks/delta/) you can find information about delta lake features
-- MAGIC - [Here](https://docs.getdbt.com/reference/resource-configs/databricks-configs) you can find some dbt configurations which might be relevant
-- MAGIC - [Here](https://www.databricks.com/blog/category/platform/product) some blog posts about new features
-- MAGIC - [Here](https://www.databricks.com/blog/category/engineering/data-warehousing) some blog posts about new features on data warehousing 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC In Databricks Community Edition there is no unity catalog (three level naming), so you can complete the exercises in hive metastore (two level naming). We have created widgets such that you can use ```$database_name.table_name```. However, if you experience problems you can always hard code the table address.

-- COMMAND ----------

-- DBTITLE 1,Create widgets for catalog and schema
-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll()
-- MAGIC dbutils.widgets.text("database_name", "default")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Introduction to exercises
-- MAGIC
-- MAGIC This exercise is ment to give you an opportunity to:
-- MAGIC - demonstrate knowlege of the Azure Databricks SQL platform and optimization decisions
-- MAGIC - demonstrate basic dbt knowledge
-- MAGIC - demonstrate basic sql knowleage and your interpretation of clear but performant code
-- MAGIC - demonstrate basic git knowledge
-- MAGIC
-- MAGIC These exercises are designed to give you the possibility to demonstrate your coding skills, conventions, ability to search for solutions, and reasoning. If you do not succeed or get stuck in the exercises provide explanation with pseudo code when possible. This notebook is designed in a way that if you encounter problems with for example databricks community edition or using widget values, you can still provide a solution. Likewise, if you do not possess dbt experience, now is your chance to go trought the docs. Finally, optimisation should based on Azure Databricks SQL if possible.
-- MAGIC
-- MAGIC Pay special attention to following:
-- MAGIC - code clarity and readability. Please use cte style code e.g. 
-- MAGIC   ```
-- MAGIC     with source as (
-- MAGIC
-- MAGIC       ... 
-- MAGIC
-- MAGIC     ),
-- MAGIC
-- MAGIC     ...
-- MAGIC
-- MAGIC     final as (
-- MAGIC
-- MAGIC       ...
-- MAGIC
-- MAGIC     )
-- MAGIC
-- MAGIC     select *
-- MAGIC     from final
-- MAGIC   ```
-- MAGIC - Document if you have linted your code using the sqlfluff configuration below or followed the sql format described in the rules when writing the code. See https://docs.sqlfluff.com/en/stable/reference/rules.html
-- MAGIC
-- MAGIC   ```
-- MAGIC   [sqlfluff]
-- MAGIC   dialect = databricks
-- MAGIC   templater = dbt
-- MAGIC   output_line_length = 150
-- MAGIC   max_line_length = 150
-- MAGIC   large_file_skip_byte_limit = 50000
-- MAGIC
-- MAGIC   [sqlfluff:templater:dbt]
-- MAGIC   project_dir = ./
-- MAGIC
-- MAGIC   [sqlfluff:indentation]
-- MAGIC   indented_joins = true
-- MAGIC   template_blocks_indent = true
-- MAGIC   indented_using_on = false
-- MAGIC
-- MAGIC   [sqlfluff:layout:type:comma]
-- MAGIC   spacing_before = touch
-- MAGIC   line_position = trailing
-- MAGIC
-- MAGIC   [sqlfluff:layout:type:binary_operator]
-- MAGIC   line_position = trailing
-- MAGIC
-- MAGIC   [sqlfluff:rules:capitalisation.keywords]
-- MAGIC   capitalisation_policy = lower
-- MAGIC
-- MAGIC   [sqlfluff:rules:capitalisation.identifiers]
-- MAGIC   extended_capitalisation_policy = lower
-- MAGIC
-- MAGIC   [sqlfluff:rules:capitalisation.functions]
-- MAGIC   extended_capitalisation_policy = lower
-- MAGIC
-- MAGIC   [sqlfluff:rules:capitalisation.literals]
-- MAGIC   capitalisation_policy = lower
-- MAGIC
-- MAGIC   [sqlfluff:rules:convention.casting_style]
-- MAGIC   preferred_type_casting_style = shorthand
-- MAGIC
-- MAGIC   [sqlfluff:rules:ambiguous.column_references]
-- MAGIC   group_by_and_order_by_style = consistent
-- MAGIC   ```
-- MAGIC - code performance
-- MAGIC - documentation

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.1. Create tables
-- MAGIC
-- MAGIC Create the tables below. 
-- MAGIC
-- MAGIC Consider the following:
-- MAGIC - are there any column level definitions or changes that would speed up the queries?
-- MAGIC - are there any table level configs that would speed up the usage of this table? 
-- MAGIC
-- MAGIC At least following operations will be common:
-- MAGIC - checking up distinct unit_id's
-- MAGIC - joins based on unit and unit_id
-- MAGIC - string comparisons like lower()
-- MAGIC - there might be other keys added to employee table later on
-- MAGIC - accesses to table history
-- MAGIC - frequent delete, merge, and update operations
-- MAGIC
-- MAGIC Provide your suggestions by adding statements to the create table statement directly or you can also provide alter table statements. Provide links to resources if possible.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Remove any existing tables in case of problems recreating tables
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/", recurse=True);
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/", recurse=True);

-- COMMAND ----------

-- DBTITLE 1,create tables
-- Run this code
create or replace table $database_name.unit ( 
  unit_name string,
  country string,
  unit_id integer
);

create or replace table $database_name.employee ( 
  first_name string,
  last_name string,
  unit integer
);

-- COMMAND ----------

create or replace table $database_name.unit (
  unit_id integer not null generated always as identity, -- auto_increment
  unit_name string not null, 
  country string not null,
  constraint pk_unit_id primary key (unit_id)
)
using delta
partitioned by (country);

create or replace table $database_name.employee ( 
  employee_id integer not null generated always as identity, 
  first_name string not null,
  last_name string not null,
  unit_id integer not null,
  created_at timestamp default current_timestamp(),
  updated timestamp default current_timestamp()
)
using delta
partitioned by (unit_id);

-- Optimize
optimize $database_name.employee;
optimize $database_name.unit;

-- COMMAND ----------

-- Manually join when foreign keys are not possible to use

select 
  e.employee_id, 
  e.first_name,
  e.last_name,
  u.unit_name, 
  u.country
from 
  $database_name.employee e
join 
  $database_name.unit u on e.unit_id = u.unit_id

-- COMMAND ----------

-- TODO:
-- Fill in any changes you would make to the table definitions, column definitions or alter table statements you would run

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.2. Populate tables
-- MAGIC
-- MAGIC
-- MAGIC The following rows are just to aid you in the next section to grasp the typical values you would get during daily loads to these tables. That is, treat the tables as if they would be very big.
-- MAGIC
-- MAGIC What kind of problems you see with the input data and do you have suggestions how to avoid them?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **TODO:** list problems you see in the input data
-- MAGIC 1. Problem: The id is not running so it is possible that it has duplicate input
-- MAGIC
-- MAGIC Solution: Make the insert running number when creating the table, so the id will be either updated by the id or created new one when there wont be duplicate persons with same id.
-- MAGIC
-- MAGIC 2. Problem: Upper or lower cases. This could lead to inconsistencies when filtering or joining tables.
-- MAGIC
-- MAGIC Solution: Handle tables with either lower() or upper() funnction
-- MAGIC
-- MAGIC 3. Problem: Lack of foreing keys, the relations between tables. The employees can be assigned into units that doesnt exist.
-- MAGIC
-- MAGIC Solution: Enforce the relationships manually with select
-- MAGIC
-- MAGIC 4. Problem: Scalability (if the datasets are large). If they grow big the querying and joining manually could become hard and slow without proper indexing, partitioning or clustering.
-- MAGIC
-- MAGIC Solution: Optimize the tables and implement clustering

-- COMMAND ----------

-- Note: leave this as it is, just answer the question above
-- populate employee table
insert into $database_name.employee values('James', 'Barry', 1);
insert into $database_name.employee values('James', 'Barry', 3);
insert into $database_name.employee values('James', 'barry', 1);
insert into $database_name.employee values('Mohan', 'Kumar', 2);
insert into $database_name.employee values('Raj', 'gupta', 3);
insert into $database_name.employee values('Raj', 'Gupta', 4);

-- populate unit table
insert into $database_name.unit values ('HR', 'UK', 1);
insert into $database_name.unit values ('R&D', 'USA', 2);
insert into $database_name.unit values ('Sales', 'India', 3);
insert into $database_name.unit values ('R&D', 'India', 4);

-- COMMAND ----------

-- I ran the insert code more than once so now there is more the persons as should. Here I empty the table

DELETE FROM $database_name.employee;
DELETE FROM $database_name.unit;



-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.3. Joining tables
-- MAGIC Create a cte style sql query which lists employees whom have worked in two different units. The result set will include the following information:
-- MAGIC - first name of the employee
-- MAGIC - last name of the employee
-- MAGIC - unit name
-- MAGIC - unique row id
-- MAGIC
-- MAGIC Consider the following when doing this sql query:
-- MAGIC - clarity
-- MAGIC - possibility to extend the business rules in the future
-- MAGIC - performance
-- MAGIC - styling based on linter configuration on defined earlier in this notebook

-- COMMAND ----------

select *
from $database_name.employee e


-- COMMAND ----------

select *
from $database_name.unit

-- COMMAND ----------

-- Test code to see if code for duplicates work:

select
    first_name,
    last_name,
    count(*) as duplicate_count
from $database_name.employee
group by first_name, last_name
having count(*) > 1

-- COMMAND ----------

-- I dont personally like the name units because it can be easily misunderstood as unit, i would prefer employee_units

with units as (
    select
    e.first_name,
    e.last_name,
    e.unit,
    u.unit_name,
    u.country
    from $database_name.employee e
    join $database_name.unit u on e.unit = u.unit_id
),
employees_with_experience as (
    select
    upper(first_name),
    upper(last_name)
    from units
    group by upper(first_name), upper(last_name)
    having count(distinct unit) > 1
)


select*
from employees_with_experience


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.4. Fill in .yml
-- MAGIC Provide dbt model .yml file with column descriptions and tests for the model you created above. You can add as many tests you think is appropriate. You can also use external dbt package tests if you think that provides value.
-- MAGIC ```{yml}
-- MAGIC models:
-- MAGIC   - name: employees_with_experience
-- MAGIC     description: "employees who have worked in multiple units"
-- MAGIC     columns:
-- MAGIC     - name: first_name
-- MAGIC       description: "Employee first name"
-- MAGIC       tests:
-- MAGIC         - not_null
-- MAGIC     - name: last_name
-- MAGIC       description: "Employee last name"
-- MAGIC       tests:
-- MAGIC         - not_null
-- MAGIC     - name: unit_name
-- MAGIC       description: "Unit name where the employee works"
-- MAGIC       tests:
-- MAGIC         - not_null
-- MAGIC     - name: country
-- MAGIC       description: "Units country"
-- MAGIC       tests:
-- MAGIC         - not_null
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.5. dbt model configuration
-- MAGIC Suppose your dbt model runs the code above by joining employee and unit information into a table every day and we don't want to clear the history. Please provide a dbt model configuration for that model with the following configurations:
-- MAGIC - your proposal for the most reasonable clustering or partitioning config
-- MAGIC - any relevant pre or post-hooks you think the model might need
-- MAGIC - any relevant table configs discussed in part 1.1
-- MAGIC
-- MAGIC Provide either config block or yml specification.
-- MAGIC
-- MAGIC ```
-- MAGIC models:
-- MAGIC   - name: employees_with_experience
-- MAGIC     description: "Employees who have worked in multiple units"
-- MAGIC     config:
-- MAGIC       schema: employees
-- MAGIC       materialized: incremental
-- MAGIC       unique_key:
-- MAGIC         - first_name
-- MAGIC         - last_name
-- MAGIC         - unit_name
-- MAGIC         - country
-- MAGIC       clustering:
-- MAGIC         - unit_id
-- MAGIC         - country
-- MAGIC       incremental_strategy: merge
-- MAGIC       post-hook:
-- MAGIC         - "alter table {{ this }} add column if not exists ingestion_date date default current_date"
-- MAGIC
-- MAGIC ```
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.6. Create unit test for duplicate row handling in dbt
-- MAGIC Suppose your dbt model runs the code above by joining employee and unit information into a table. You can assume that the tables are their own dbt models such that you can reference employee table using ref('employee').
-- MAGIC
-- MAGIC Create a dbt unit test configuration that returns fail if the model does not handle duplicates correctly.
-- MAGIC
-- MAGIC ```{yml}
-- MAGIC unit_tests:
-- MAGIC   - name: test_handling_duplicates
-- MAGIC     description: "Tests if there are duplicates"
-- MAGIC     test: |
-- MAGIC       with duplicates as (
-- MAGIC         select
-- MAGIC           upper(first_name) as first_name,
-- MAGIC           upper(last_name) as last_name,
-- MAGIC           count(*) as duplicate_count
-- MAGIC         from {{ ref('employees_with_experience') }}
-- MAGIC         group by first_name, last_name
-- MAGIC         having count(*) > 1
-- MAGIC       )
-- MAGIC     select count(*) from duplicates
-- MAGIC
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1.7. git
-- MAGIC - Describe or provide code how you start development from the main branch and creating your own branch
-- MAGIC ```
-- MAGIC Swich to main
-- MAGIC git checkout main
-- MAGIC
-- MAGIC Pull request
-- MAGIC git pull origin main
-- MAGIC
-- MAGIC Create own branch
-- MAGIC git checkout -b branch_data_engineer_milla
-- MAGIC ```
-- MAGIC - Describe or provide code how you get the most recent changes from the main branch to your development branch
-- MAGIC ```
-- MAGIC git fetch origin
-- MAGIC git merge origin/main
-- MAGIC
-- MAGIC ```
-- MAGIC - Describe or provide code how you can keep the main branch tidy when there are many developers working in the same project
-- MAGIC ```
-- MAGIC git add .
-- MAGIC git commit -m "Added features in test and config"
-- MAGIC git push origin branch_data_engineer_milla
-- MAGIC
-- MAGIC and
-- MAGIC git rebase origin/main
-- MAGIC
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Feedback
-- MAGIC Provide your view on the exercises and what you would change or add to the exercises.
