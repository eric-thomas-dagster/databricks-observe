import os
import requests
import time

DATABRICKS_HOST = os.getenv("DATABRICKS_HOSTNAME")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


def get_all_jobs():
    host = os.getenv("DATABRICKS_HOSTNAME")
    token = os.getenv("DATABRICKS_TOKEN")

    response = requests.get(
        f"{host}/api/2.1/jobs/list",
        headers={"Authorization": f"Bearer {token}"}
    )
    response.raise_for_status()
    data = response.json()
    return data.get("jobs", [])


def trigger_job_and_wait(job_id: str, timeout_seconds: int = 600, context=None) -> tuple[str, str]:
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}"
    }

    # Kick off job run
    trigger_url = f"{DATABRICKS_HOST}/api/2.1/jobs/run-now"
    resp = requests.post(trigger_url, headers=headers, json={"job_id": job_id})
    resp.raise_for_status()
    run_id = resp.json()["run_id"]

    if context:
        context.log.info(f"Triggered job {job_id} with run_id {run_id}")

    # Poll job status
    status_url = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get"
    start_time = time.time()
    while True:
        poll = requests.get(status_url, headers=headers, params={"run_id": run_id})
        poll.raise_for_status()
        status = poll.json()["state"]["life_cycle_state"]

        if status in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}:
            result_state = poll.json()["state"].get("result_state", "UNKNOWN")
            if context:
                context.log.info(f"Job {job_id} finished with status: {result_state}")
            return str(run_id), result_state

        if time.time() - start_time > timeout_seconds:
            if context:
                context.log.warn(f"Timeout reached for job {job_id}")
            return str(run_id), "TIMEOUT"

        time.sleep(5)

def get_job_run_output(run_id: int):
    url = f"{os.getenv('DATABRICKS_HOSTNAME')}/api/2.1/jobs/runs/get-output"
    headers = {
        "Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}",
        "Content-Type": "application/json"
    }
    resp = requests.get(url, headers=headers, params={"run_id": run_id})
    resp.raise_for_status()
    return resp.json()

def get_latest_job_run(job_id):
    url = f"{os.getenv('DATABRICKS_HOSTNAME')}/api/2.1/jobs/runs/list"
    headers = {"Authorization": f"Bearer {os.getenv('DATABRICKS_TOKEN')}"}
    params = {"job_id": job_id, "limit": 1}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    runs = resp.json().get("runs", [])
    return runs[0] if runs else None