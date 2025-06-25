from dagster import Definitions
from .schedules.observe_schedule import observe_dlt_job, observe_schedule
from .sensors.dlt_sensor import dlt_pipeline_update_sensor
from .assets.dlt_assets import get_materializable_assets, get_external_asset_specs_with_lineage, external_materialization_schedule
from .assets.customer_assets import us_customers
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#lineage_assets, lineage_keys = get_lineage_aware_assets()
#observable_assets = get_observable_assets(exclude_keys=lineage_keys)
#assets = lineage_assets + observable_assets + get_materializable_assets()


defs = Definitions(
    assets=[*get_materializable_assets(), *get_external_asset_specs_with_lineage(), us_customers],
    sensors=[dlt_pipeline_update_sensor],
    schedules=[external_materialization_schedule],
)
