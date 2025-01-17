-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC # Bench Demo:
-- MAGIC ### SQL UDFs and Control Flow
-- MAGIC by Jorge Erosa

-- COMMAND ----------

-- DBTITLE 1,Creating Table
CREATE TABLE IF NOT EXISTS table_demo (
  item_id            INT NOT NULL, --constraint name as pk
  item_name          STRING,
  item_price         DECIMAL(5,1)
) 

-- COMMAND ----------

-- DBTITLE 1,Inserting Values into Table
INSERT INTO table_demo VALUES
(1, 'Premium Queen Mattress', 1795),
(2, 'Standard Full Mattress', 945),
(3, 'Premium Full Mattress', 1695),
(4, 'Premium Twin Mattress', 1095),
(5, 'Premium King Mattress', 1995)

-- COMMAND ----------

-- MAGIC %md ## User-Defined Functions
-- MAGIC
-- MAGIC User Defined Functions (UDFs) in Spark SQL allow you to register custom SQL logic as functions in a database, making these methods reusable anywhere SQL can be run on Databricks. These functions are registered natively in SQL and maintain all of the optimizations of Spark when applying custom logic to large datasets.
-- MAGIC
-- MAGIC At minimum, creating a SQL UDF requires a function name, optional parameters, the type to be returned, and some custom logic.
-- MAGIC
-- MAGIC more info at [Databricks UDF Docs](https://docs.databricks.com/en/udf/index.html)

-- COMMAND ----------

-- DBTITLE 1,Table View
select * from table_demo

-- COMMAND ----------

-- DBTITLE 1,SQL Statement
--declaring function name plus input parameters with data type
CREATE OR REPLACE FUNCTION sale_offer(item_name STRING, item_price DECIMAL(5,1))
--declaring output data type
RETURNS STRING 
--actual function
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0), " (previously ", (item_price), ")");

-- test function
SELECT *, sale_offer(item_name, item_price) AS message FROM table_demo

-- COMMAND ----------

-- MAGIC %md Note that this function is applied to all values of the column in a parallel fashion within the Spark processing engine. SQL UDFs are an efficient way to define custom logic that is optimized for execution on Databricks.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Scoping and Permissions of SQL UDFs
-- MAGIC We can use **`DESCRIBE FUNCTION`** to see where a function was registered and basic information about expected inputs and what is returned (and even more information with **`DESCRIBE FUNCTION EXTENDED`**).
-- MAGIC
-- MAGIC more info at [Databricks UDF Docs](https://docs.databricks.com/en/udf/index.html)

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_offer

-- COMMAND ----------

-- MAGIC %md Note that the **`Body`** field at the bottom of the function description shows the SQL logic used in the function itself.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Simple Control Flow Functions
-- MAGIC
-- MAGIC Combining SQL UDFs with control flow in the form of **`CASE`** / **`WHEN`** clauses provides optimized execution for control flows within SQL workloads. The standard SQL syntactic construct **`CASE`** / **`WHEN`** allows the evaluation of multiple conditional statements with alternative outcomes based on table contents.
-- MAGIC
-- MAGIC Here, we demonstrate wrapping this control flow logic in a function that will be reusable anywhere we can execute SQL. 
-- MAGIC
-- MAGIC more info at [Databricks UDF Docs](https://docs.databricks.com/en/udf/index.html)

-- COMMAND ----------

-- DBTITLE 1,SQL Control Flow
CREATE OR REPLACE FUNCTION different_offers(item_price DECIMAL(5,1))
RETURNS STRING
RETURN CASE 
  WHEN item_price>1500 THEN concat("This item is on sale with 30% off (", round(item_price * 0.7, 0), ")")
  ELSE concat("This item is on sale with 10% off (", round(item_price * 0.9, 0), ")")
END;

SELECT *, different_offers(item_price) FROM table_demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC For more info please visit [Databricks UDF Docs](https://docs.databricks.com/en/udf/index.html)
