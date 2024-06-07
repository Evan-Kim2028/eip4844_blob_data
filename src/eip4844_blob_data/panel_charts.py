import polars as pl


def create_slot_inclusion_line_chart(df: pl.DataFrame, sequencers: list[str]):
    return (
        df.filter(pl.col("sequencer_names").is_in(sequencers))
        .select(
            "slot_time",
            "slot_inclusion_rate",
            "slot_inclusion_rate_50_blob_avg",
            "2_slot_target_inclusion_rate",
            "submission_count",
            "sequencer_names",
        )
        .plot.line(
            x="slot_time",
            y=[
                "slot_inclusion_rate",
                # "slot_inclusion_rate_50_blob_avg",
                "2_slot_target_inclusion_rate",
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
        .filter(pl.col("slot_inclusion_rate") < 40)
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
        .rename(
            {
                "priority_fee_bid_percent_premium": "priority fee bid premium (%)",
                "effective_gas_price_gwei": "block gas price (gwei)",
            }
        )
        .plot.line(
            x="slot_inclusion_rate",
            y=["priority fee bid premium (%)"],
            ylabel="priority fee bid premium (%, gwei)",
            xlabel="slot inclusion rate",
            title="priority fee bid premium effect on slot inclusion rate",
            color="g",
            legend="top_left",
        )
    )

    return priority_fee_premium_chart * line_chart_bid_premium
