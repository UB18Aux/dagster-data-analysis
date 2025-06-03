import dagster as dg
import pandas as pd
from marketcrawler.resources import DatabaseResource
from datetime import datetime, timedelta, timezone
import requests as req


def determine_missing_combinations(
    all_items: pd.DataFrame,
    available_price_data: pd.DataFrame,
    dt_now: datetime,
    dt_past: datetime,
) -> pd.DataFrame:
    """Determines all (item_id, day, hour) combinations between dt_past and dt_now that are missing in the pric edata."""
    # Set day_of_year and hour for the data we have
    available_price_data["day_of_year"] = available_price_data["timestamp"].dt.dayofyear
    available_price_data["hour"] = available_price_data["timestamp"].dt.hour

    # Generate all tuples (day, hour) for the given timestamp
    all_datetimes = pd.date_range(start=dt_past, end=dt_now, freq="H")
    all_days = all_datetimes.dayofyear
    all_hours = all_datetimes.hour

    time_combinations = pd.DataFrame(
        {"day_of_year": all_days, "hour": all_hours}
    ).drop_duplicates()

    # Merge the two dataframes to create (item_id, day_of_year, hour) triples
    all_combinations = all_items[["item_id"]].merge(time_combinations, how="cross")

    existing_combinations = available_price_data[
        ["item_id", "day_of_year", "hour"]
    ].drop_duplicates()

    # Remove all combinations that we already have
    missing_combinations = (
        all_combinations.merge(
            existing_combinations,
            on=["item_id", "day_of_year", "hour"],
            how="left",
            indicator=True,
        )
        .query('_merge == "left_only"')
        .drop("_merge", axis=1)
    )
    return missing_combinations


def crawl_missing_price_data(
    context: dg.AssetExecutionContext,
    missing_combinations: pd.DataFrame,
) -> list[dict]:
    """Crawls all price and volume points for missing pairs of (item_id, day, hour)."""

    current_year = datetime.now().year
    new_entries = []
    total_entries = len(missing_combinations)
    tried_entries = 0

    for row in missing_combinations.itertuples():
        item_id = row.item_id
        day_of_year = row.day_of_year
        hour = row.hour
        # Datetime needed for the API call
        dt = datetime(current_year, 1, 1) + timedelta(days=day_of_year - 1, hours=hour)
        try:
            # Call the api
            response = req.get(
                f"http://price-api:8000/price",
                params={
                    "item_id": item_id,
                    "time": dt.isoformat(),
                },
            )
            # Various checks to make we successfully got data
            response.raise_for_status()
            result = response.json()
            if not result.get("success", False):
                raise Exception(
                    f"Unsuccessful query: {result.get('error','(No error returned)')}."
                )
            if not "price" in result or not "volume" in result:
                raise Exception("Missing price or volume field in response.")

            # Extract data
            price = result.get("price")
            volume = result.get("volume")
            new_entries.append(
                {
                    "item_id": item_id,
                    "volume": volume,
                    "price": price,
                    "timestamp": dt,
                }
            )
            context.log.debug(
                f"Successfuly crawled item: {item_id}, Time: {dt.isoformat()}."
            )
        except Exception as e:
            context.log.warning(
                f"Failed to crawl item_id: {item_id}, Time: {dt.isoformat()}. Reason: {str(e)}"
            )

        # Return a status uptadte at least every 5% percent of progress
        tried_entries += 1
        if tried_entries % max(1, total_entries // 20) == 0:
            context.log_event(
                dg.AssetObservation(
                    asset_key=context.asset_key,
                    metadata={
                        "successful": len(new_entries),
                        "failed": tried_entries - len(new_entries),
                        "total": total_entries,
                        "progress": tried_entries / total_entries,
                    },
                )
            )
    return new_entries


@dg.asset(
    kinds={"json", "postgres", "python"},
    group_name="Crawler",
    description="Returns price data up to the last 10 days. If missing, the data will be crawled and inserted into the database.",
    deps=["all_items", "available_price_data"],
)
def recent_price_data(
    context: dg.AssetExecutionContext,
    all_items: pd.DataFrame,
    available_price_data: pd.DataFrame,
    database: DatabaseResource,
) -> pd.DataFrame:
    """Returns recent price data for the past 10 days. Determines what is missing, crawls it, stores it in the database."""

    # The daterange we want to return price data for
    dt_past = datetime.now(timezone.utc) - timedelta(days=10)
    dt_now = datetime.now(timezone.utc)

    # We don't need to consider data older than 10 days
    available_price_data = available_price_data[
        available_price_data["timestamp"] >= dt_past
    ]

    # Which combinations do we lack
    missing_combinations = determine_missing_combinations(
        all_items,
        available_price_data,
        dt_now,
        dt_past,
    )

    if len(missing_combinations) > 0:
        # We need to crawl some data
        context.log.info(
            f"Found {len(missing_combinations)} missing combinations. Starting to crawl"
        )
        new_entries = crawl_missing_price_data(context, missing_combinations)

        # Insert the new data
        context.log.info(
            f"Found {len(new_entries)} price points. Inserting now",
        )
        data_tuples = [
            (entry["item_id"], entry["volume"], entry["price"], entry["timestamp"])
            for entry in new_entries
        ]
        try:
            with database.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        "INSERT INTO price_data (item_id, volume, price, timestamp) VALUES (%s, %s, %s, %s)",
                        data_tuples,
                    )
                conn.commit()

        except Exception as e:
            raise dg.Failure(
                f"Excpetion while inserting price data into database: {str(e)}"
            )
        try:
            # Read the new data from table
            with database.get_connection() as conn:
                df = pd.read_sql(
                    "SELECT item_id, volume, price, timestamp FROM price_data", conn
                )
        except Exception as e:
            raise dg.Failure(
                f"Excpetion while getting price data from database: {str(e)}"
            )
        # Filter out old data
        df = df[df["timestamp"] >= dt_past]
    else:
        # We can use what we got as input
        df = available_price_data

    # Sanitize and return the data
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    context.add_output_metadata(
        {
            "num_records": dg.MetadataValue.int(len(df)),
            "columns": dg.MetadataValue.text(str(list(df.columns))),
            "preview": dg.MetadataValue.md(df.head().to_markdown()),
        }
    )

    return df
