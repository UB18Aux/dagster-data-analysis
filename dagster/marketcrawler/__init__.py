from dagster import Definitions
from .resources import Database
from .assets import (
    all_items,
    available_price_data,
    recent_price_data,
    generate_plotly_dashboard,
)

defs = Definitions(
    assets=[
        all_items,
        available_price_data,
        recent_price_data,
        generate_plotly_dashboard,
    ],
    resources={
        "database": Database,
    },
)
