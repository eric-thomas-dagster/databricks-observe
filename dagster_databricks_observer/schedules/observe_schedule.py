from dagster import define_asset_job, ScheduleDefinition

observe_dlt_job = define_asset_job(
    name="observe_dlt_job",
    selection="*"
)

observe_schedule = ScheduleDefinition(
    job=observe_dlt_job,
    cron_schedule="*/15 * * * *"  # Every 15 minutes
)