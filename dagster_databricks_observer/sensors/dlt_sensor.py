import os, json
from dagster import RunRequest, SensorEvaluationContext, sensor, AssetMaterialization, SensorEvaluationContext, SensorResult
from dagster_databricks_observer.pipeline_discovery import get_all_dlt_pipelines
from ..pipeline_discovery import get_latest_pipeline_status

TERMINAL_STATE = "COMPLETED"

@sensor(minimum_interval_seconds=60)
def dlt_pipeline_update_sensor(context: SensorEvaluationContext) -> SensorResult:
    materializations = []
    try:
        last_state_map = json.loads(context.cursor) if context.cursor else {}
    except Exception:
        last_state_map = {}

    for name, (pipeline_id, asset_key) in get_all_dlt_pipelines().items():
        latest_run = get_latest_pipeline_status(pipeline_id)
        if not latest_run:
            continue

        update_id = latest_run.get("update_id")
        state = latest_run.get("state")
        key = f"{pipeline_id}:{update_id}"
        last_recorded_state = last_state_map.get(key)

        # Only emit if this is the first time we've seen COMPLETED
        if state == TERMINAL_STATE and last_recorded_state != TERMINAL_STATE:
            url = f"{os.getenv('DATABRICKS_HOSTNAME')}/pipelines/{pipeline_id}/latest"

            materializations.append(AssetMaterialization(
                asset_key=asset_key,
                metadata={
                    "update_id": update_id,
                    "state": state,
                    "pipeline_name": name,
                    "Databricks Pipeline": url
                },
                description=f"DLT pipeline '{name}' completed"
            ))

            last_state_map[key] = TERMINAL_STATE

    return SensorResult(
        asset_events=materializations,
        cursor=json.dumps(last_state_map)
    )


