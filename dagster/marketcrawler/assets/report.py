import dagster as dg
import pandas as pd
import plotly.subplots as subplt
import plotly.graph_objects as go
import os
import plotly.express as px


@dg.asset(
    kinds={"python", "plotly"},
    group_name="Report",
    description="Generates a report of the last 10 days.",
    deps=["recent_price_data", "all_items"],
)
def generate_plotly_dashboard(
    context: dg.AssetExecutionContext,
    recent_price_data: pd.DataFrame,
    all_items: pd.DataFrame,
):
    """Generates a plotly dashboard for the price data of the last 10 days, and stores it in the dashboard-server's folder."""
    df_with_items = recent_price_data.merge(all_items, on="item_id", how="left")
    df_with_items = df_with_items.sort_values(["name", "timestamp"])

    fig = subplt.make_subplots(
        rows=2,
        cols=1,
        subplot_titles=("Prices Over Time", "Volumes Over Time"),
        vertical_spacing=0.15,
    )

    unique_items = all_items["name"].unique()

    colors = px.colors.qualitative.Plotly

    for i, item_name in enumerate(unique_items):
        item_data = df_with_items[df_with_items["name"] == item_name]
        color = colors[i % len(colors)]

        fig.add_trace(
            go.Scatter(
                x=item_data["timestamp"],
                y=item_data["price"],
                mode="lines",
                name=f"{item_name}",
                legendgroup=f"item_{i}",
                line=dict(color=color),
                marker=dict(color=color),
                showlegend=True,
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=item_data["timestamp"],
                y=item_data["volume"],
                mode="lines",
                name=f"{item_name}",
                legendgroup=f"item_{i}",
                line=dict(color=color),
                marker=dict(color=color),
                showlegend=False,
            ),
            row=2,
            col=1,
        )

    fig.update_layout(
        height=800,
        title_text="Price Data Dashboard",
        hovermode="x unified",
    )

    fig.update_xaxes(title_text="Time", row=1, col=1)
    fig.update_xaxes(title_text="Time", row=2, col=1)

    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    dashboard_name = f"dashboard_{context.run_id}.html"
    html_path = os.path.join("/app", "dashboards", dashboard_name)
    os.makedirs(os.path.dirname(html_path), exist_ok=True)

    fig.write_html(html_path)

    url = f"http://localhost/dashboards/{dashboard_name}"
    context.add_output_metadata(
        {
            "dashboard": dg.MetadataValue.url(url),
        }
    )
