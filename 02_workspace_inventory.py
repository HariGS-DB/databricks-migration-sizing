# Databricks notebook source
# MAGIC %md **02 – Workspace asset counts.** Run once from any workspace. It loops over the in-scope workspaces listed in the widget, authenticating to each
# MAGIC as a **service principal (OAuth M2M)** that is **workspace admin in every listed workspace**. Read-only. Output: one row per workspace and asset type with a count.

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("workspace_urls", "", "Workspace URLs (comma-separated)")
dbutils.widgets.text("secret_scope", "migration-sizing", "Secret scope holding the SP credentials")
dbutils.widgets.text("client_id_key", "sp-client-id", "Secret key: SP client ID")
dbutils.widgets.text("client_secret_key", "sp-client-secret", "Secret key: SP OAuth secret")

workspace_urls = [u.strip().rstrip("/") for u in dbutils.widgets.get("workspace_urls").split(",") if u.strip()]
scope = dbutils.widgets.get("secret_scope")
client_id = dbutils.secrets.get(scope, dbutils.widgets.get("client_id_key"))
client_secret = dbutils.secrets.get(scope, dbutils.widgets.get("client_secret_key"))
assert workspace_urls, "Enter at least one workspace URL"

# COMMAND ----------

def count(it):
    return sum(1 for _ in it)

def notebooks_and_files(w):
    n = {"NOTEBOOK": 0, "FILE": 0}
    for o in w.workspace.list("/", recursive=True):
        kind = o.object_type.value if o.object_type else None
        if kind in n:
            n[kind] += 1
    return n

def genie_spaces(w):
    n, tok = 0, None
    while True:
        r = w.genie.list_spaces(page_token=tok)
        n, tok = n + len(r.spaces or []), r.next_page_token
        if not tok:
            return n

def vector_search(w):
    eps = list(w.vector_search_endpoints.list_endpoints())
    return {"endpoints": len(eps), "indexes": sum(e.num_indexes or 0 for e in eps)}

def lakebase_projects(w):
    return count(w.postgres.list_projects()) if hasattr(w, "postgres") else None

# (category, item, function returning an int or a dict of sub-item -> int)
CHECKS = [
    ("code",      "notebooks_and_files",       notebooks_and_files),
    ("code",      "git_folders",               lambda w: count(w.repos.list())),
    ("workflows", "jobs",                      lambda w: count(w.jobs.list())),
    ("workflows", "pipelines",                 lambda w: count(w.pipelines.list_pipelines())),
    ("compute",   "all_purpose_clusters",      lambda w: count(c for c in w.clusters.list()
                                                               if c.cluster_source and c.cluster_source.value in ("UI", "API"))),
    ("compute",   "instance_pools",            lambda w: count(w.instance_pools.list())),
    ("compute",   "cluster_policies",          lambda w: count(w.cluster_policies.list())),
    ("compute",   "sql_warehouses",            lambda w: count(w.warehouses.list())),
    ("apps",      "databricks_apps",           lambda w: count(w.apps.list())),
    ("lakebase",  "database_instances",        lambda w: count(w.database.list_database_instances())),
    ("lakebase",  "autoscaling_projects",      lakebase_projects),
    ("ml",        "serving_endpoints",         lambda w: count(e for e in w.serving_endpoints.list()
                                                               if not e.name.startswith("databricks-"))),
    ("ml",        "vector_search",             vector_search),
    ("ml",        "mlflow_experiments",        lambda w: count(w.experiments.list_experiments(max_results=100))),
    ("ml",        "workspace_registry_models", lambda w: count(w.model_registry.list_models())),
    ("bi",        "aibi_dashboards",           lambda w: count(w.lakeview.list())),
    ("bi",        "legacy_dashboards",         lambda w: count(w.dashboards.list())),
    ("bi",        "queries",                   lambda w: count(w.queries.list())),
    ("bi",        "alerts",                    lambda w: count(w.alerts.list())),
    ("bi",        "genie_spaces",              genie_spaces),
    ("other",     "secret_scopes",             lambda w: count(w.secrets.list_scopes())),
]

# COMMAND ----------

rows = []
for url in workspace_urls:
    host = url if url.startswith("https://") else f"https://{url}"
    try:
        w = WorkspaceClient(host=host, client_id=client_id, client_secret=client_secret, auth_type="oauth-m2m")
        w.current_user.me()
    except Exception as e:
        rows.append((host, "connect", "ERROR", None, f"{type(e).__name__}: {str(e)[:300]}"))
        continue
    for category, item, fn in CHECKS:
        try:
            result = fn(w)
            for sub, n in (result.items() if isinstance(result, dict) else [(None, result)]):
                rows.append((host, category, f"{item}:{sub.lower()}" if sub else item, n, None))
        except Exception as e:
            rows.append((host, category, item, None, f"{type(e).__name__}: {str(e)[:300]}"))
    print(f"done: {host}")

display(spark.createDataFrame(rows, "workspace string, category string, item string, count long, error string")
        .orderBy("workspace", "category", "item"))
