-- Databricks notebook source
-- 00 – Record the in-scope catalogs and workspaces. Notebook 01 and the sizing SQL queries filter on these two tables.
-- Edit the INSERT values, then run once on a SQL warehouse or cluster as a user who can create a schema in `main`.

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS main.migration_sizing;

-- COMMAND ----------

CREATE OR REPLACE TABLE main.migration_sizing.scope_catalogs (catalog_name STRING);
INSERT INTO main.migration_sizing.scope_catalogs VALUES ('catalog_a'), ('catalog_b');      -- edit

-- COMMAND ----------

CREATE OR REPLACE TABLE main.migration_sizing.scope_workspaces (workspace_id STRING, workspace_name STRING);
INSERT INTO main.migration_sizing.scope_workspaces VALUES ('1234567890123456', 'prod');   -- edit
