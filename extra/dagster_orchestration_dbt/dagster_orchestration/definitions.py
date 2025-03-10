from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_modules,
)
from dagster_duckdb_pandas import DuckDBPandasIOManager

from dagster_orchestration.assets import duck
from .assets import pipelineone, pipelinetwo


duck_assets = load_assets_from_modules([duck])
pipelineone_assets = load_assets_from_modules([pipelineone])
dbt_assets = load_assets_from_modules([pipelinetwo])

# define the job that will materialize the assets
pandas_job = define_asset_job("pandas_job", selection=["pandas_releases","summary_statistics"])

pipelineone_job = define_asset_job(
    name="pipeline_one_job",
    selection=["pipeline_one_asset_a","pipeline_one_asset_b","pipeline_one_asset_c"]
    #selection=AssetSelection.all()
)

pipelinetwo_job = define_asset_job(
    name="dbt_pipeline",
    selection=["pipeline_dbt_seed","pipeline_dbt_run","pipeline_dbt_test"]
)

# a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
pandas_schedule = ScheduleDefinition(
    job=pandas_job, cron_schedule="0 0 * * *"  # every day at midnight
)
pipelineone_schedule = ScheduleDefinition(
    job=pipelineone_job, cron_schedule="0 0 * * *"  # every day at midnight
)

pipelinetwo_schedule = ScheduleDefinition(
    job=pipelinetwo_job, cron_schedule="0 0 * * *"  # every day at midnight
)


database_io_manager = DuckDBPandasIOManager(database="analytics.pandas_releases")

defs = Definitions(
    assets=[*duck_assets,*pipelineone_assets,*dbt_assets],
    schedules=[pandas_schedule,pipelineone_schedule,pipelinetwo_schedule],
    resources={
        "io_manager": database_io_manager,
    },
)
