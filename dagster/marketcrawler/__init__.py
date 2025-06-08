import dagster as dg
from .resources import Database, Api
from .assets import (
    all_items,
    available_price_data,
    recent_price_data,
    generate_plotly_dashboard,
)

run_pipeline_job = dg.define_asset_job(
    name="complete_pipeline_job",
    selection=[
        "all_items",
        "available_price_data",
        "recent_price_data",
        "generate_plotly_dashboard",
    ],
)

crawl_job = dg.define_asset_job(
    name="crawling_recent_data_job",
    selection=[
        "all_items",
        "available_price_data",
        "recent_price_data",
    ],
)

daily_schedule = dg.ScheduleDefinition(
    job=crawl_job,
    cron_schedule="0 * * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[
        all_items,
        available_price_data,
        recent_price_data,
        generate_plotly_dashboard,
    ],
    jobs=[
        run_pipeline_job,
    ],
    schedules=[
        daily_schedule,
    ],
    resources={
        "database": Database,
        "api": Api,
    },
)
