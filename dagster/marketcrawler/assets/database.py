import dagster as dg
import pandas as pd
from marketcrawler.resources import DatabaseResource


@dg.asset(
    kinds={"postgres", "python"},
    group_name="Database",
    description="Returns a dataframe with all available items.",
)
def all_items(
    context: dg.AssetExecutionContext, database: DatabaseResource
) -> pd.DataFrame:
    """Returns all items that we have (item_id, name, type)"""
    with database.get_connection() as conn:
        try:
            df = pd.read_sql("SELECT item_id, name, type FROM items", conn)
            context.add_output_metadata(
                {
                    "num_records": dg.MetadataValue.int(len(df)),
                    "columns": dg.MetadataValue.text(str(list(df.columns))),
                    "preview": dg.MetadataValue.md(df.head().to_markdown()),
                }
            )
            return df
        except Exception as e:
            raise dg.Failure(f"Excpetion while getting items from database: {str(e)}")


@dg.asset(
    kinds={"postgres", "python"},
    group_name="Database",
    description="Returns a dataframe with all available price entries.",
)
def available_price_data(
    context: dg.AssetExecutionContext, database: DatabaseResource
) -> pd.DataFrame:
    """Returns all price data we have (item_id, volume, price, timestamp)"""
    with database.get_connection() as conn:
        try:
            df = pd.read_sql(
                "SELECT item_id, volume, price, timestamp FROM price_data", conn
            )
            context.add_output_metadata(
                {
                    "num_records": dg.MetadataValue.int(len(df)),
                    "columns": dg.MetadataValue.text(str(list(df.columns))),
                    "preview": dg.MetadataValue.md(df.head().to_markdown()),
                }
            )
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df
        except Exception as e:
            raise dg.Failure(
                f"Excpetion while getting price data from database: {str(e)}"
            )
