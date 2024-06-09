import polars as pl
import panel as pn


def create_slot_inclusion_line_chart(df: pl.DataFrame, sequencers: list[str]):
    return (
        df.filter(pl.col("sequencer_names").is_in(sequencers))
        .select(
            "slot_time",
            "slot_inclusion_rate",
            "submission_count",
            "sequencer_names",
        )
        .with_columns(pl.lit(2).alias('2_slot_target_inclusion_rate'))
        .plot.line(
            x="slot_time",
            y=[
                "slot_inclusion_rate",
                "2_slot_target_inclusion_rate"
            ],
            by="sequencer_names",
            ylabel="Beacon Block Inclusion (block)",
            xlabel="Slot Date Time",
            title="Historical Slot Inclusion",
            width=800,
            height=375,
        )
    )


def create_priority_fee_chart(
    slot_gas_bidding_df: pl.DataFrame,
    slot_gas_groupby_df: pl.DataFrame,
    sequencers: list[str],
):
    # priority fee scatter plot
    priority_fee_premium_chart = (
        slot_gas_bidding_df.filter(pl.col("sequencer_names").is_in(sequencers))
        .filter(pl.col("slot_inclusion_rate") < 100)
        .sort(by="slot_inclusion_rate")
        .plot.scatter(
            x="slot_inclusion_rate",
            y="priority_fee_bid_percent_premium",
            width=800,
            height=375,
            legend="top_left",
        )
    )

    line_chart_bid_premium = (
        slot_gas_groupby_df.filter(pl.col("sequencer_names").is_in(sequencers))
        .filter(pl.col("slot_inclusion_rate") < 100)
        .rename(
            {
                "priority_fee_bid_percent_premium": "priority fee bid premium (%)"
            }
        )
        .plot.line(
            x="slot_inclusion_rate",
            y=["priority fee bid premium (%)"],
            ylabel="priority fee bid premium (%, gwei)",
            xlabel="slot inclusion rate",
            title="priority fee bid premium over base fee",
            color="g",
            legend="top_left",
        )
    )

    return priority_fee_premium_chart * line_chart_bid_premium


def get_slot_inclusion_table(df: pl.DataFrame, sequencers: list[str]):
    slot_df = (df.filter(pl.col("sequencer_names").is_in(sequencers)).sort(
        by='slot_inclusion_rate', descending=True)
        .select(
            'slot', 'block_number', 'slot_time', 'sequencer_names',
            'hash', 'fill_percentage', 'submission_count', 'slot_inclusion_rate', 'blob_hashes_length',
            'base_tx_fee_eth', 'priority_tx_fee_eth', 'base_fee_per_gas', 'priority_fee_gas', 'total_tx_fee_eth', 'priority_fee_bid_percent_premium'
    )
        .unique()
    )
    return pn.widgets.Tabulator(
        slot_df.to_pandas(),
        layout='fit_data_table'
        # layout='fit_columns'
    )
