import os, re
import requests
import time
from dagster import AssetKey

def sanitize_name(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", name)

def get_all_dlt_pipelines():
    """
    Returns a dictionary of pipeline_name -> (pipeline_id, AssetKey)
    """
    url = f"{os.getenv('DATABRICKS_HOSTNAME')}/api/2.0/pipelines"
    headers = {"Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    pipelines = resp.json().get("statuses", [])

    pipeline_map = {}
    for p in pipelines:
        name = p["name"]
        pipeline_id = p.get("pipeline_id")
        if pipeline_id:
            asset_key = AssetKey(["dlt_pipeline", sanitize_name(name)])
            pipeline_map[name] = (pipeline_id, asset_key)

    return pipeline_map


def get_latest_pipeline_status(pipeline_id):
    """
    Returns metadata from the most recent pipeline update for a given DLT pipeline.
    """
    url = f"{os.getenv('DATABRICKS_HOSTNAME')}/api/2.0/pipelines/{pipeline_id}/updates"
    headers = {"Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}"}
    params = {"max_results": 1}

    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    updates = resp.json().get("updates", [])
    return updates[0] if updates else None


def trigger_dlt_pipeline_and_wait(pipeline_id, timeout_seconds=600, context=None):
    """
    Triggers a Databricks DLT pipeline and polls for completion using the updates/{update_id} endpoint.
    """
    base_url = os.getenv("DATABRICKS_HOSTNAME")
    token = os.getenv("DATABRICKS_TOKEN")
    headers = {"Authorization": f"Bearer {token}"}

    # Step 1: Trigger the pipeline
    trigger_url = f"{base_url}/api/2.0/pipelines/{pipeline_id}/updates"
    resp = requests.post(trigger_url, headers=headers)
    resp.raise_for_status()
    update_id = resp.json()["update_id"]

    # Step 2: Poll for completion
    status_url = f"{base_url}/api/2.0/pipelines/{pipeline_id}/updates/{update_id}"
    start_time = time.time()
    poll_interval = 5  # seconds

    while time.time() - start_time < timeout_seconds:
        status_resp = requests.get(status_url, headers=headers)
        status_resp.raise_for_status()
        data = status_resp.json()

        update_info = data.get("update", {})
        life_cycle_state = update_info.get("state")

        if context:
            context.log.info(f"Polling update... life_cycle_state={life_cycle_state}")

        if life_cycle_state in {"COMPLETED", "FAILED", "CANCELED"}:
            return life_cycle_state, update_id

        time.sleep(poll_interval)

    raise TimeoutError(f"Pipeline {pipeline_id} did not complete within {timeout_seconds} seconds.")