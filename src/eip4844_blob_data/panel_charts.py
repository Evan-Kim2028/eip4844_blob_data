import polars as pl
import panel as pn

# start dashboard


def start_interactive_panel(filtered_data_dict, sequencer_names_list):
    multi_select = pn.widgets.MultiSelect(
        name="Sequencers",
        size=10,
        options=sequencer_names_list,
        value=sequencer_names_list,
    )

    # initial chart and table data
    slot_inclusion_line_chart = create_slot_inclusion_line_chart(
        filtered_data_dict["slot_inclusion_df"], sequencer_names_list
    )

    priority_fee_chart = create_priority_fee_chart(
        # add in filter to remove outliers and make chart look better
        filtered_data_dict["slot_inclusion_df"].filter(
            pl.col('slot_inclusion_rate') < 50),
        filtered_data_dict["slot_gas_groupby_df"].filter(
            pl.col('slot_inclusion_rate') < 50),
        sequencer_names_list,
    )

    fee_breakdown_line_chart = filtered_data_dict["slot_inclusion_df"].sort(by='slot_time').plot.scatter(
        x='slot_time', y=['base_fee_per_gas', 'priority_fee_gas'], groupby='sequencer_names', s=1,
        xlabel='datetime', ylabel='gas (gwei)', title='Base Fee vs Priority Fee (gwei)',
        # need `slot_time` so that it doesn't share the same y-axis.
        shared_axes=False
    )

    sequencer_macro_blob_table: pl.DataFrame = (
        filtered_data_dict['slot_inclusion_df'].drop_nulls().unique().group_by(
            'sequencer_names').agg(
            pl.col('fill_percentage').mean().alias('avg_fill_percentage'),
            pl.col('submission_count').mean().alias(
                'avg_submission_count'),
            pl.col('slot_inclusion_rate').mean().round(
                3).alias('avg_slot_inclusion_rate'),
            pl.col('blob_hashes_length').mean().alias(
                'avg_blob_hashes_length'),
            pl.len().alias('tx_count'),
            pl.col('blob_hashes_length').sum().alias('blob_count'),
            pl.col('base_tx_fee_eth').sum().round(
                3).alias('total_base_fees_eth'),
            pl.col('priority_tx_fee_eth').sum().round(
                3).alias('total_priority_fees_eth'),
            pl.col('total_tx_fee_eth').sum().round(3).alias('total_eth_fees'),
            pl.col('priority_fee_gas').mean().round(
                3).alias('avg_priority_fee_bid'),
        ).rename({'sequencer_names': 'rollup', 'avg_blob_hashes_length': 'avg_blobs_in_tx'}))

    slot_inclusion_table_tabulator = get_slot_inclusion_table(
        filtered_data_dict["slot_inclusion_df"], sequencer_names_list)

    filename, button = slot_inclusion_table_tabulator.download_menu(
        text_kwargs={'name': 'Enter filename', 'value': 'default.csv'},
        button_kwargs={'name': 'Download data'},
    )

    entire_panel = pn.Column(
        pn.Row(
            pn.pane.Markdown(
                """
            # EIP-4844 Slot Inclusion Dashboard

            ## About
            This dashboard shows detailed analytics for blob inclusion rates as well as the efficiency of using EIP-1559 priority fees
            as a bidding mechanism for faster slot inclusion. This dashboard is made using [Xatu Data](https://github.com/ethpandaops/xatu-data?tab=readme-ov-file) for EL mempool and Beacon chain data and [Hypersync](https://github.com/enviodev/hypersync-client-python) 
            for transaction gas data for the [EIP-4844 data challenge](https://esp.ethereum.foundation/data-challenge-4844).
            """
            ),
            multi_select,
            styles=dict(background="WhiteSmoke"),
        ),
        pn.pane.Markdown(
            """
            ## 7 Day Historical Slot Inclusion
            When a transaction is resubmitted with updated gas parameters, the transaction hash changes. For example take this blob reference hash - 0x01c738cf37c911334c771f2295c060e5bd7d084f347e4334863336724934c59a. 
            On [etherscan](https://etherscan.io/tx/0x763d823c0f933c4d2eb84406b37aa2649753f2f563fa3ee6d27251c6a52a8d69) we can see that the transaction was replaced by the user. We can see on Ethernow that the transaction contains 
            the same blob reference hash in both the [original tx](https://www.ethernow.xyz/tx/0x763d823c0f933c4d2eb84406b37aa2649753f2f563fa3ee6d27251c6a52a8d69?batchIndex=1) and the [resubmitted tx](https://www.ethernow.xyz/tx/0x5a4094662bd05ff3639a8979927ab527e007a6925387951a9c1b3d2958b13a86?batchIndex=1).
            
            We can measure the total time that a blob hash sat in the mempool by subtracting the original tx was first seen from the slot time, when it eventually is finalized on the beacon chain. 
            In this particular example, the total time that the blob sat in the mempool was not from 18:56:27 to 18:57:11 (4 slots), but really 18:54:29 to 18:57:11 (14 slots)
            """
        ),
        pn.Row(
            slot_inclusion_line_chart.opts(axiswise=True),
            priority_fee_chart.opts(legend_position="left", show_legend=True),
            styles=dict(background="WhiteSmoke"),

        ),
        pn.Row(
            pn.pane.Markdown(
                """
            ## Slot Inclusion Rates
            **Slot Inclusion Rate** - The slot inclusion rate indicates the number of slots required for a blob to be included in the beacon chain, 
            with a higher rate signifying a longer inclusion time. The accompanying time-series chart tracks this metric from initial mempool 
            appearance to final beacon block inclusion. A 50 blob slot inclusion average is taken to smooth out the performance. 
            The target slot inclusion rate is 2. 
                """
            ),
            pn.pane.Markdown(
                """
            ## EIP-1559 Priority Fee Premium Correlation with Slot Rates
            The scatterplot illustrates the relationship between the EIP-1559 priority fee bid premiums and slot inclusion rates. The scatterplot points
            are individual blob bid datapoints and the line is a median bid premium. A higher priority fee bid premium tends to coincide 
            with longer slot inclusion times. This unexpected twist underscores the value of efficient slot utilization. The data indicates a trend 
            where higher bid premiums are associated with longer slot inclusion times, suggesting that as the time for a blob to be included 
            in the beacon chain increases, so does the priority fee bid premium. This behavior comes from the fact that if a blob sits in the 
            mempool for too long, then it is resubmitted with a higher priority fee. 
            """
            ),
            styles=dict(background="WhiteSmoke")
        ),
        pn.Row(
            pn.pane.Markdown(
                """
                # Blob Transaction Data (Past 7 days):
                
                This table provides detailed information on various metrics related to traditional transaction hashes that carry blob hashes. The metrics include:

                Blob Fill Percentage: Indicates the percentage of the transaction space filled by blobs.
                Transaction Resubmission Count: The number of times a transaction has been resubmitted with the same blobs.
                Number of Blobs in a Transaction: The count of blobs contained within a single transaction.
                ETH Priority Fees: The priority fees associated with each transaction in ETH.
                Additional Metrics: Various other relevant metrics, such as fees and timings.
                """
            ),
            pn.Column(
                pn.pane.Markdown("""
                                 #### **Bid Competitiveness**: the amount of priority fees being paid by the rollup compared to the block base fee.
                                 """
                                 ),
                fee_breakdown_line_chart.opts(axiswise=True),
                styles=dict(background="WhiteSmoke")
            ),
            styles=dict(background="WhiteSmoke"),
        ),
        pn.Row(
            pn.widgets.Tabulator(
                sequencer_macro_blob_table.to_pandas(), layout='fit_data'
            ),
            # avg_slot_inclusion_scatterplot,
            styles=dict(background="WhiteSmoke")
        ),
        pn.Column(
            pn.pane.Markdown(
                """
                # Slot Inclusion Data Table
                The table shows raw data that the dashboard was built on
                """
            ),
            pn.Column(filename, button),
            slot_inclusion_table_tabulator,
            styles=dict(background="WhiteSmoke")
        )
    )

    def update_bar_chart(event):
        """
        Use this to update charts based on sequencer name user selection
        """
        entire_panel[2][0].object = create_slot_inclusion_line_chart(
            filtered_data_dict["slot_inclusion_df"],
            sequencers=multi_select.value,
        )

        entire_panel[2][1].object = create_priority_fee_chart(
            filtered_data_dict["slot_inclusion_df"],
            filtered_data_dict["slot_gas_groupby_df"],
            sequencers=multi_select.value,
        )

        # I don't thnk this currently works right now
        entire_panel[4][1].object = filtered_data_dict["slot_inclusion_df"].sort(by='slot_time').plot.scatter(
            x='slot_time', y=['base_fee_per_gas', 'priority_fee_gas'], groupby='sequencer_names', s=1,
            xlabel='datetime', ylabel='gas (gwei)', title='Base Fee vs Priority Fee (gwei)',
            # need `slot_time` so that it doesn't share the same y-axis.
            shared_axes=False
        )

    multi_select.param.watch(update_bar_chart, "value")

    return entire_panel


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
            width=900,
            height=375,
        )
    )


def filter_data_seq(
    sequencers: list[str], slot_inclusion_joined_df: pl.DataFrame, cached_data: dict[str, pl.DataFrame]
) -> dict[str: pl.DataFrame]:
    """
    This function filters a dataframe and returns updated chart data, based on the input of the dashboard user.
    """

    # slot inclusion
    slot_inclusion_df = (
        slot_inclusion_joined_df.filter(
            pl.col("sequencer_names").is_in(sequencers))
        .filter(pl.col('meta_network_name') == 'mainnet')
        .unique()
        .sort(by="slot")
    )

    # gas bidding scatterplot median
    slot_gas_groupby_df = (
        slot_inclusion_df.group_by("slot_inclusion_rate", "sequencer_names")
        .agg(
            pl.col("priority_fee_bid_percent_premium").median(),
            pl.col("base_fee_per_gas").mean(),
        )
        .sort(by="slot_inclusion_rate")
        .drop_nulls()
    )

    return {
        # time series
        "slot_inclusion_df": slot_inclusion_df,
        "slot_gas_groupby_df": slot_gas_groupby_df,
    }


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
            width=900,
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


def fee_breakdown_line(df: pl.DataFrame, sequencers: list[str]):
    fee_breakdown_line = (
        df.filter(pl.col("sequencer_names").is_in(sequencers))
        .plot.line(x='slot_time', y=['base_tx_fee_eth', 'priority_tx_fee_eth'], by='sequencer_names', width=900,
                   height=375, xlabel='Time', ylabel='Fee Breakdown (in ETH)', title='Fee Breakdown')
    )

    return fee_breakdown_line.opts(axiswise=True)


def get_slot_inclusion_table(df: pl.DataFrame, sequencers: list[str]):
    slot_df = (df.filter(pl.col("sequencer_names").is_in(sequencers)).sort(
        by='slot_inclusion_rate', descending=True)
    )
    return pn.widgets.Tabulator(
        slot_df.to_pandas(),
        layout='fit_data_table',
        pagination='local'
        # layout='fit_columns'
    )
