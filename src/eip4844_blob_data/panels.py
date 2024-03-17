import panel as pn
import polars as pl
from dataclasses import dataclass
from ethpandaops_python.client import Queries

pn.extension("plotly", template="material", sizing_mode="stretch_width")


@dataclass
class Panels:
    clickhouse_queries = Queries()

    def blob_propagation(self):
        # make the query
        blob_propagation_query = self.clickhouse_queries.blob_propagation(
            time="7", network="mainnet"
        )
        blob_prop_df: pl.DataFrame = pl.from_pandas(blob_propagation_query)

        # make the panel
        blob_prop_panel = (
            blob_prop_df.group_by("slot")
            .agg(
                pl.col("slot_start_date_time").first(),
                pl.col("propagation_slot_start_diff").mean().alias(
                    "prop_slot_time_diff_mean"),
                pl.col("propagation_slot_start_diff").min().alias(
                    "prop_slot_time_diff_min"),
                pl.col("propagation_slot_start_diff").max().alias(
                    "prop_slot_time_diff_max"),
            )
            .filter(pl.col("prop_slot_time_diff_max") < 15000)
            .sort(by="slot_start_date_time")
            .plot.line(
                x="slot_start_date_time",
                y=[
                    "prop_slot_time_diff_min",
                    "prop_slot_time_diff_mean",
                    "prop_slot_time_diff_max",
                ],
                xlabel="Date",
                ylabel="Time (ms)",
                line_width=2,
                alpha=0.8,
            )
        )

        return pn.Column(
            "# Mainnet Blob Sidecar Propagation Time",
            blob_prop_panel.opts(axiswise=True),
            sizing_mode="stretch_width",
        )

    def blob_in_mempool(self):
        # make the query
        blob_propagation_query = self.clickhouse_queries.mempool_transaction(
            time="7", network="mainnet"
        )
        blob_prop_df: pl.DataFrame = pl.from_pandas(blob_propagation_query)

        # make the panel
        blob_prop_panel = (
            blob_prop_df.group_by("slot")
            .agg(
                pl.col("slot_start_date_time").first(),
                pl.col("propagation_slot_start_diff").mean().alias(
                    "prop_slot_time_diff_mean"),
                pl.col("propagation_slot_start_diff").min().alias(
                    "prop_slot_time_diff_min"),
                pl.col("propagation_slot_start_diff").max().alias(
                    "prop_slot_time_diff_max"),
            )
            .filter(pl.col("prop_slot_time_diff_max") < 15000)
            .sort(by="slot_start_date_time")
            .plot.line(
                x="slot_start_date_time",
                y=[
                    "prop_slot_time_diff_min",
                    "prop_slot_time_diff_mean",
                    "prop_slot_time_diff_max",
                ],
                xlabel="Date",
                ylabel="Time (ms)",
                line_width=2,
                alpha=0.8,
            )
        )

        return pn.Column(
            "# Mainnet Blob Sidecar Propagation Time",
            blob_prop_panel,
            sizing_mode="stretch_width",
        )
