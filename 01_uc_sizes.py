# Databricks notebook source
# 01 – In-scope UC sizes (tables + volumes) and federation (foreign catalogs). Run once per metastore as a metastore admin, after creating the scope table (2.1).
# Delta tables are sized with DESCRIBE DETAIL (current snapshot). Non-Delta external tables and volumes are sized by listing files,
# which can take a while for volumes with millions of files.
dbutils.widgets.text("sizing_schema", "main.migration_sizing", "Sizing schema")
sch = dbutils.widgets.get("sizing_schema")

# COMMAND ----------

from concurrent.futures import ThreadPoolExecutor

def walk(path):
    size = files = 0
    stack = [path]
    while stack:
        for f in dbutils.fs.ls(stack.pop()):
            if f.isDir():
                stack.append(f.path)
            else:
                size, files = size + f.size, files + 1
    return size, files

# COMMAND ----------

# Tables
tables = spark.sql(f"""
  SELECT t.table_catalog, t.table_schema, t.table_name, t.table_type, t.data_source_format, t.storage_path
  FROM system.information_schema.tables t
  JOIN {sch}.scope_catalogs s ON t.table_catalog = s.catalog_name
  WHERE t.table_type IN ('MANAGED', 'EXTERNAL')
""").collect()

def size_table(t):
    try:
        if t.data_source_format == "DELTA":
            d = spark.sql(f"DESCRIBE DETAIL `{t.table_catalog}`.`{t.table_schema}`.`{t.table_name}`").first()
            location, size, files = d.location, d.sizeInBytes, d.numFiles
        else:
            location = t.storage_path
            size, files = walk(location)
        return (t.table_catalog, t.table_schema, t.table_name, t.table_type, t.data_source_format, location, size, files, None)
    except Exception as e:
        return (t.table_catalog, t.table_schema, t.table_name, t.table_type, t.data_source_format, t.storage_path,
                None, None, str(e)[:300])

with ThreadPoolExecutor(16) as pool:
    table_rows = list(pool.map(size_table, tables))

spark.createDataFrame(table_rows, "table_catalog string, table_schema string, table_name string, table_type string, "
                                  "format string, location string, size_bytes long, num_files long, error string") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{sch}.uc_table_sizes")

# COMMAND ----------

# Volumes
volumes = spark.sql(f"""
  SELECT v.volume_catalog, v.volume_schema, v.volume_name, v.volume_type, v.storage_location
  FROM system.information_schema.volumes v
  JOIN {sch}.scope_catalogs s ON v.volume_catalog = s.catalog_name
""").collect()

def size_volume(v):
    try:
        size, files = walk(f"/Volumes/{v.volume_catalog}/{v.volume_schema}/{v.volume_name}")
        return (*v, size, files, None)
    except Exception as e:
        return (*v, None, None, str(e)[:300])

with ThreadPoolExecutor(8) as pool:
    volume_rows = list(pool.map(size_volume, volumes))

spark.createDataFrame(volume_rows, "volume_catalog string, volume_schema string, volume_name string, volume_type string, "
                                   "storage_location string, size_bytes long, num_files long, error string") \
     .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{sch}.uc_volume_sizes")

print(f"tables: {len(table_rows)} ({sum(r[-1] is not None for r in table_rows)} errors), "
      f"volumes: {len(volume_rows)} ({sum(r[-1] is not None for r in volume_rows)} errors)")

# COMMAND ----------

# Federation: list of in-scope foreign catalogs and their connections
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
fed = []
for r in spark.table(f"{sch}.scope_catalogs").collect():
    try:
        c = w.catalogs.get(r.catalog_name)
        if not c.connection_name:
            continue
        conn = w.connections.get(c.connection_name)
        fed.append((r.catalog_name, c.catalog_type.value if c.catalog_type else None, c.connection_name,
                    conn.connection_type.value if conn.connection_type else None, (conn.options or {}).get("host"), None))
    except Exception as e:
        fed.append((r.catalog_name, None, None, None, None, str(e)[:300]))

display(spark.createDataFrame(fed, "catalog string, catalog_type string, connection string, connection_type string, "
                                   "host string, error string"))
