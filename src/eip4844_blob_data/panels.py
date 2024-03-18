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
        return (
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

    def blob_in_mempool(self):
        mempool_type3 = self.clickhouse_queries.mempool_transaction(
            "blobs", 7, "mainnet", 3)

        beacon_block_type3 = self.clickhouse_queries.canonical_beacon_block_execution_transaction(
            "blobs", 7, "mainnet", 3
        )

        # merge the queries together
        blob_mempool: pl.DataFrame = (
            pl.from_pandas(beacon_block_type3)
            .join(pl.from_pandas(mempool_type3), on="hash", how="inner", suffix="_mempool")
            .with_columns(
                [
                    # calculate blob time in mempool as difference between slot start date time and earliest time the blob was seen in the EL mempool
                    (pl.col("slot_start_date_time") - pl.col("earliest_event_date_time")).alias(
                        "blob_time_in_mempool"
                    ),
                ]
            )
            .with_columns(
                [
                    (pl.col("blob_time_in_mempool").dt.total_seconds() / 12)
                    .ceil()
                    .alias("blocks_in_mempool")
                ]
            )
            .with_columns(
                pl.col("blocks_in_mempool")
                .rolling_mean(window_size=32, min_periods=1)
                .alias("blocks_in_mempool_32_block_avg")
            )
        )

        return blob_mempool.sort(
            by="blob_time_in_mempool", descending=False
        ).plot.line(
            x="slot_start_date_time",
            y=["blocks_in_mempool", "blocks_in_mempool_32_block_avg"],
            title="Blob Censorship",
            xlabel="Date",
            ylabel="# of blocks",
        )
