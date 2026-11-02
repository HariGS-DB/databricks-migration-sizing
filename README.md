# Azure Databricks → GCP Databricks migration sizing notebooks

All three notebooks are read-only against your workspaces. The only things they write are the scope tables and the two result tables in `main.migration_sizing`.

| Notebook | Run as | Where | Output |
|---|---|---|---|
| `00_scope_setup.sql` | User who can create a schema in `main` | Once per metastore | `scope_catalogs`, `scope_workspaces` tables |
| `01_uc_sizes.py` | Metastore admin (or BROWSE / SELECT / READ VOLUME / READ FILES on in-scope catalogs) | Once per metastore, on a cluster | `uc_table_sizes`, `uc_volume_sizes` tables + list of federated (foreign) catalogs |
| `02_workspace_inventory.py` | Service principal (OAuth M2M), workspace admin in every listed workspace | Once, from any workspace | One table: workspace / category / item / count |

## Import
Workspace → **Import** → upload this ZIP (or each file on its own). The files are Databricks source-format notebooks.

## Steps
1. **00_scope_setup**: replace the example catalog names and workspace IDs with the ones in scope, then run it.
2. **01_uc_sizes**: leave the `sizing_schema` widget as `main.migration_sizing` (or change it if you used a different schema in step 1). Delta tables are sized with `DESCRIBE DETAIL`. Non-Delta tables and volumes are sized by listing their files, which can be slow for volumes with millions of files.
3. **02_workspace_inventory**:
   - Create an OAuth secret for a service principal that is workspace admin in every in-scope workspace.
   - Store its client ID and secret in a secret scope in the workspace you run from. The defaults are scope `migration-sizing` with keys `sp-client-id` and `sp-client-secret`.
   - Enter the workspace URLs, comma-separated, in the `workspace_urls` widget.
   - The workspace you run from must be able to reach every URL (check Private Link and IP access lists). Any workspace it can't reach shows up as a `connect / ERROR` row, and the others still run.
4. Export the outputs as CSV and send them back:
   - `uc_table_sizes` and `uc_volume_sizes`
   - the federation list from notebook 01
   - the counts table from notebook 02
