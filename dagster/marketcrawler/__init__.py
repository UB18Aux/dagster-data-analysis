import dagster as dg
from .resources import Database, Api
from .assets import (
    all_items,
    available_price_data,
    recent_price_data,
    generate_plotly_dashboard,
)


run_pipeline = dg.define_asset_job(
    name="price_data_pipeline",
    selection=[
        "all_items",
        "available_price_data",
        "recent_price_data",
        "generate_plotly_dashboard",
    ],
)
defs = dg.Definitions(
    assets=[
        all_items,
        available_price_data,
        recent_price_data,
        generate_plotly_dashboard,
    ],
    jobs=[
        run_pipeline,
    ],
    resources={
        "database": Database,
        "api": Api,
    },
)
