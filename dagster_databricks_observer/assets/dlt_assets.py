import os
import re
import json
import requests
from typing import List, Set
from dagster import (
    AssetKey,
    AssetSpec,
    MetadataValue,
    Output,
    asset,
    sensor,
    AssetMaterialization,
    SensorEvaluationContext,
    TableSchema, 
    TableColumn,
    job,
    op,
    ScheduleDefinition
)
from ..pipeline_discovery import get_all_dlt_pipelines, trigger_dlt_pipeline_and_wait
from ..job_discovery import get_all_jobs, trigger_job_and_wait
from ..table_discovery import get_all_tables, get_sql_connection

DATABRICKS_HOST = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

EXCLUDED_CATALOGS = {""}
EXCLUDED_SCHEMAS = {"information_schema"}

SKIP_ASSETS = {AssetKey(["workspace", "default", "us_customers"])}

def sanitize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name)

def get_lineage_for_table(catalog: str, schema: str, table: str) -> List[AssetKey]:
    try:
        response = requests.get(
            f"{DATABRICKS_HOST}/api/2.0/lineage-tracking/table-lineage",
            headers={
                "Authorization": f"Bearer {DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            },
            params={"table_name": f"{catalog}.{schema}.{table}", "include_entity_lineage": "true"}
        )
        response.raise_for_status()
        upstreams = response.json().get("upstreams", [])
        return [
            AssetKey([
                src["tableInfo"]["catalog_name"],
                src["tableInfo"]["schema_name"],
                src["tableInfo"]["name"]
            ])
            for src in upstreams if "tableInfo" in src
        ]
    except Exception:
        return []

def get_recursive_lineage(catalog: str, schema: str, table: str, visited: Set[str] = None) -> List[AssetKey]:
    visited = visited or set()
    self_key = AssetKey([catalog, schema, table])
    key_str = f"{catalog}.{schema}.{table}"
    if key_str in visited:
        return []
    visited.add(key_str)

    direct = get_lineage_for_table(catalog, schema, table)
        # Filter out self-dependency
    direct = [k for k in direct if k != self_key]
    all_upstreams = list(direct)
    for up in direct:
        try:
            c, s, t = up.path
            all_upstreams.extend(get_recursive_lineage(c, s, t, visited))
        except Exception:
            continue
    return list(set(all_upstreams))

def get_external_asset_specs_with_lineage():
    specs = []
    seen = set()
    conn = get_sql_connection()
    cursor = conn.cursor()

    try:
        # 🔍 Query all table metadata once
        cursor.execute("""
            SELECT table_catalog, table_schema, table_name, table_type, comment, data_source_format
            FROM system.information_schema.tables
        """)
        rows = cursor.fetchall()
        table_info_map = {
            (r[0], r[1], r[2]): {
                "kind": r[3].lower().replace(" ", "_"),
                "description": r[4] or None,
                "format": r[5].lower().replace(" ", "_"),
            }
            for r in rows
        }

        for catalog, schema, table in get_all_tables():
            if catalog.lower() in EXCLUDED_CATALOGS or schema.lower() in EXCLUDED_SCHEMAS:
                continue

            key = AssetKey([catalog, schema, table])
            if str(key) in seen:
                continue
            seen.add(str(key))

            info = table_info_map.get((catalog, schema, table), {})
            table_kind = info.get("kind", "table")
            raw_format = info.get("format", "").lower()

            if raw_format == "delta":
                datasource_kind = "deltalake"
            elif raw_format == "unknown_data_source_format":
                datasource_kind = ""
            else:
                datasource_kind = raw_format
            description = info.get("description", None)

            deps = get_recursive_lineage(catalog, schema, table)

            specs.append(AssetSpec(
                key=key,
                deps=deps,
                kinds={table_kind, datasource_kind, "databricks"},
                description=description
            ))
    finally:
        conn.close()

    return [a for a in specs if a.key not in SKIP_ASSETS]


@op
def emit_all_external_materializations(context):
    from .dlt_assets import get_all_tables, get_sql_connection
    from dagster import AssetMaterialization, MetadataValue, TableSchema, TableColumn, AssetKey

    conn = get_sql_connection()
    cursor = conn.cursor()
    try:
        for catalog, schema, table in get_all_tables():
            if catalog.lower() in EXCLUDED_CATALOGS or schema.lower() in EXCLUDED_SCHEMAS:
                continue
            key = AssetKey([catalog, schema, table])
            try:
                # Row count
                cursor.execute(f"SELECT COUNT(*) FROM {catalog}.{schema}.{table}")
                row_count = cursor.fetchone()[0]

                # Last modified
                cursor.execute(f"DESCRIBE DETAIL {catalog}.{schema}.{table}")
                row = dict(zip([d[0] for d in cursor.description], cursor.fetchone()))
                last_modified = row.get("lastModified") or row.get("createdAt")

                # Column schema
                cursor.execute(f"""
                    SELECT column_name, data_type, comment
                    FROM system.information_schema.columns
                    WHERE table_catalog = '{catalog}'
                      AND table_schema = '{schema}'
                      AND table_name = '{table}'
                """)
                schema_rows = cursor.fetchall()
                table_schema = TableSchema([
                    TableColumn(name=col[0], type=col[1], description=col[2] or None)
                    for col in schema_rows
                ])

                metadata = {
                    "row_count": int(row_count),
                    "last_modified": str(last_modified),
                    "dagster/column_schema": table_schema,
                    "Databricks Table": MetadataValue.md(
                        f"[{catalog}.{schema}.{table}]({os.getenv('DATABRICKS_HOSTNAME')}/explore/data/{catalog}/{schema}/{table})"
                    )
                }
#                if table_description:
#                    metadata["dagster/description"] = MetadataValue.text(table_description)

                context.log_event(AssetMaterialization(asset_key=key, metadata=metadata))

            except Exception as e:
                context.log.warn(f"[OBSERVE ERROR] {catalog}.{schema}.{table}: {e}")
    finally:
        conn.close()



def get_materializable_assets():
    assets = []
    seen = set()
    for pipeline_name, (pipeline_id, asset_key) in get_all_dlt_pipelines().items():
    #for pipeline_name, pipeline_id in get_all_dlt_pipelines().items():
        key = AssetKey(["dlt_pipeline", sanitize_name(pipeline_name)])
        if str(key) in seen: continue
        seen.add(str(key))
        def make_pipeline_asset(pipeline_id, pipeline_name, key):
        #def make_pipeline_asset(pid, pname, k):
            @asset(key=key,kinds={"pipeline", "databricks"})
            def _pipeline(context):
                state, update_id = trigger_dlt_pipeline_and_wait(pipeline_id, context=context)
                return Output(value=None, metadata={
                    "status": MetadataValue.text(state),
                    "update_id": MetadataValue.text(str(update_id)),
                    "pipeline_name": MetadataValue.text(pipeline_name),
                    "Databricks Pipeline": MetadataValue.md(f"[View in Databricks]({DATABRICKS_HOST}/pipelines/{pipeline_id}/latest)")
                })
            return _pipeline
        assets.append(make_pipeline_asset(pipeline_id, pipeline_name, key))

    for job in get_all_jobs():
        job_id = job.get("job_id")
        job_name = job.get("settings", {}).get("name") or f"job_{job_id}"
        key = AssetKey(["job", sanitize_name(job_name)])
        if str(key) in seen: continue
        seen.add(str(key))
        def make_job_asset(jid, jname, k):
            @asset(key=k, kinds={"job", "databricks"})
            def _job(context):
                run_id, status = trigger_job_and_wait(jid, context=context)
                return Output(value=None, metadata={
                    "status": MetadataValue.text(status),
                    "run_id": MetadataValue.text(str(run_id)),
                    "job_name": MetadataValue.text(jname),
                    "Databricks Job": MetadataValue.md(f"[View in Databricks]({DATABRICKS_HOST}/jobs/{jid})")
                })
            return _job
        assets.append(make_job_asset(job_id, job_name, key))

    return assets


@job
def materialize_all_external_assets():
    emit_all_external_materializations()


external_materialization_schedule = ScheduleDefinition(
    job=materialize_all_external_assets,
    cron_schedule="0 * * * *",  # Every hour; adjust as needed
    name="external_materialization_hourly"
)