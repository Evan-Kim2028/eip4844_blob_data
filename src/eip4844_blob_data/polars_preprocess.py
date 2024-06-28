import polars as pl


def hex_to_readable_string(hex_str):
    """
    Function to convert hex string to a readable string using 'latin-1'
    """
    try:
        return bytes.fromhex(hex_str[2:]).decode('latin-1')
    except Exception as e:
        return str(e)


def create_slot_inclusion_df(cached_data: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    `slot_inclusion` returns the slot, slot inclusion time, and slot start time for the last `time` days.

    This query calculates slot inclusion data - such as average slot inclusion time and number of blob submissions

    Returns a pl.DataFrame
    """

    blob_mempool_table: pl.DataFrame = (
        cached_data["mempool_df"]
        .rename({"blob_hashes": "versioned_hash"})
        .sort(by="event_date_time")
        .group_by(
            (
                pl.col("versioned_hash").cast(pl.List(pl.Categorical)),
                "nonce",
            )
        )
        .agg(
            [
                # min/max datetime
                pl.col("event_date_time").min().alias(
                    "event_date_time_min"),
                pl.col("event_date_time").max().alias(
                    "event_date_time_max"),
                # blob sidecar data
                pl.col("blob_hashes_length").mean().alias(
                    "blob_hashes_length"),
                pl.col("blob_sidecars_size").mean().alias(
                    "blob_sidecars_size"),
                # blob utilization data
                pl.col("fill_percentage").mean().alias("fill_percentage"),
                pl.col("blob_gas").mean(),
                pl.col("blob_gas_fee_cap").mean(),
                pl.col("gas_price").mean(),
                pl.col("gas_tip_cap").mean(),
                pl.col("gas_fee_cap").mean(),
                # tx info
                pl.col("hash").last(),
                pl.col("from").last(),
                pl.col("to").last(),
            ]
        )
        .with_columns(
            # count number of times a versioned hash gets resubmitted under a new transaction hash
            pl.len().over("versioned_hash").alias("submission_count")
        )
        .sort(by="submission_count")
    )

    canonical_sidecar_df: pl.DataFrame = cached_data["canonical_beacon_blob_sidecar_df"].drop(
        "blob_index")

    return (
        (
            # .explode() separates all blob versioned hashes from a list[str] to single str rows
            blob_mempool_table.explode("versioned_hash")
            .with_columns(pl.col("versioned_hash").cast(pl.String))
        )
        .join(canonical_sidecar_df, on="versioned_hash", how="left")
        .unique()
        .with_columns(
            # divide by 1000 to convert from ms to s
            ((pl.col("slot_start_date_time") - pl.col("event_date_time_min")) / 1000)
            .alias("beacon_inclusion_time")
            .cast(pl.Float64),
        )
        .with_columns(
            # divide by 12 to get beacon slot number
            (pl.col("beacon_inclusion_time") / 12)
            .abs()
            .ceil()
            .alias("num_slot_inclusion")
        )
        .sort(by="slot_start_date_time")
        .with_columns(
            # calculate rolling average
            pl.col("num_slot_inclusion")
            .rolling_mean(50)
            .alias("rolling_num_slot_inclusion_50"),
            # add base inclusion target
            pl.lit(2).alias("base_line_2_slots"),
        )

        # rename columns for niceness
        .rename(
            {
                "slot_start_date_time": "slot_time",
                "num_slot_inclusion": "slot_inclusion_rate",
                "rolling_num_slot_inclusion_50": "slot_inclusion_rate_50_blob_avg",
                "base_line_2_slots": "2_slot_target_inclusion_rate",
            }
        )
        .drop_nulls()
        # filter for mainnet data only, there seems to be a bug that shows holesky data as well (6/7/24)
        .filter(pl.col("meta_network_name") == "mainnet")
        # adding filter because outliers mess up the graph
        .filter(pl.col("slot_inclusion_rate") < 200)
        .select(
            'versioned_hash',
            'nonce',
            'event_date_time_min',
            'event_date_time_max',
            'blob_hashes_length',
            'blob_sidecars_size',
            'fill_percentage',
            'blob_gas',
            'blob_gas_fee_cap',
            'gas_price',
            'gas_tip_cap',
            'gas_fee_cap',
            'hash',
            'from',
            'to',
            'submission_count',
            'slot',
            'slot_time',
            'block_root',
            'kzg_commitment',
            'meta_network_name',
            'blob_size',
            'blob_empty_size',
            'beacon_inclusion_time',
            'slot_inclusion_rate',
            'slot_inclusion_rate_50_blob_avg',
            '2_slot_target_inclusion_rate',
        )
    )


def create_slot_gas_bidding_df(cached_data: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    This function calculates gas bidding data for blob
    """
    slot_inclusion_df: pl.DataFrame = create_slot_inclusion_df(cached_data)

    # print(f"slot inclusion df columns: {slot_inclusion_df.columns}")
    # slot inclusion df columns: ["versioned_hash", "nonce", "event_date_time_min", "event_date_time_max", "blob_hashes_length",
    # "blob_sidecars_size", "fill_percentage", "blob_gas", "blob_gas_fee_cap", "gas_price", "gas_tip_cap",
    # "gas_fee_cap", "hash", "from", "to", "submission_count", "slot", "slot_time", "block_root",
    # "kzg_commitment", "meta_network_name", "blob_size", "blob_empty_size", "beacon_inclusion_time", "slot_inclusion_rate",
    # "slot_inclusion_rate_50_blob_avg", "2_slot_target_inclusion_rate"]

    joined_df = (
        slot_inclusion_df
        .join(
            cached_data["txs"], on="hash", how="left"
        )
        .with_columns(
            (pl.col("base_fee_per_gas") * pl.col("gas_used")).alias("base_tx_fee_eth"),
            (pl.col("effective_gas_price") - pl.col("base_fee_per_gas")).alias(
                "priority_fee_gas"
            ),
            ((pl.col("max_priority_fee_per_gas") / pl.col("effective_gas_price")
              ).round(3)).alias("priority_fee_bid_percent_premium")
        )
        .with_columns(
            # have to perform priority fee calculation in this column
            (((pl.col("effective_gas_price") - pl.col("base_fee_per_gas"))
             * pl.col("gas_used")) / 10**18).alias("priority_tx_fee_eth"),
            # unit calculations for gwei and eth values
            (pl.col("base_tx_fee_eth") / 10**18).alias("base_tx_fee_eth"),
            (pl.col("priority_fee_gas") / 10**9).alias("priority_fee_gas"),
            (pl.col("base_fee_per_gas") / 10**9).alias("base_fee_per_gas")
        )
        .with_columns(
            (pl.col("base_tx_fee_eth") + \
             pl.col("priority_tx_fee_eth")).alias("total_tx_fee_eth"),
        )
        # label builder data
        .with_columns(pl.col('extra_data').map_elements(hex_to_readable_string, return_dtype=pl.Utf8).alias('builder_label'))
        .with_columns(
            pl.when(pl.col("builder_label").str.contains("geth"))
            .then(pl.lit("vanilla_builder_geth"))
            .otherwise(pl.col("builder_label"))
            .alias("builder_label")
        )
        .with_columns(
            pl.when(pl.col("builder_label").str.contains("reth"))
            .then(pl.lit("vanilla_builder_reth"))
            .otherwise(pl.col("builder_label"))
            .alias("builder_label")
        )
        .with_columns(
            pl.when(pl.col("builder_label").str.contains("rsync"))
            .then(pl.lit("rsync_builder"))
            .otherwise(pl.col("builder_label"))
            .alias("builder_label")
        )
        .select(
            "block_number",
            "extra_data",
            "builder_label",
            "base_tx_fee_eth",
            "priority_tx_fee_eth",
            "total_tx_fee_eth",
            "base_fee_per_gas",
            "priority_fee_gas",
            "meta_network_name",
            "priority_fee_bid_percent_premium",
            "slot_inclusion_rate",
            "submission_count",
            "hash",
            "from",
        )
        .unique()
        .sort(by="block_number")
        .drop_nulls()
        # filter for mainnet data only, there seems to be a bug that shows holesky data as well (6/7/24)
        .filter(pl.col("meta_network_name") == "mainnet")
    )

    return joined_df


def create_bid_premium_df(cached_data: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """
    Groupby on slot inclusion rate to get median priority fee bid percent premium and mean effective gas price
    """
    slot_gas_bidding_df = create_slot_gas_bidding_df(cached_data)

    return (slot_gas_bidding_df.group_by("slot_inclusion_rate")
            .agg(
        pl.col("priority_fee_bid_percent_premium").median(),
        pl.col("effective_gas_price_gwei").mean(),
    )
        .sort(by="slot_inclusion_rate")
        .drop_nulls()
        # adding filter because outliers mess up the graph
        .filter(pl.col("slot_inclusion_rate") < 50)
    )


def create_blob_block_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Groupby on block number to get blob data per block.
    """

    return (df.drop_nulls().unique().group_by('block_number', 'sequencer_names').agg(
        pl.col('slot_time').first(),
        pl.col('extra_data').first(),
        pl.col('base_tx_fee_eth').sum().alias('base_fees_per_block_eth'),
        pl.col('priority_tx_fee_eth').sum().alias(
            'priority_fees_per_block_eth'),
        pl.col('total_tx_fee_eth').sum().alias('total_tx_fees_per_block_eth'),
        pl.col('slot_inclusion_rate').mean().alias(
            'avg_slot_inclusion_rate_per_block'),
        pl.col('priority_fee_gas').mean().alias(
            'avg_priority_fee_gas_per_block_gwei'),
        pl.col('base_fee_per_gas').mean(),
        pl.col('blob_hashes_length').sum().alias('blobs_per_block'),
    ).sort(by='block_number'))


def create_block_agg_df(blob_block_df: pl.DataFrame) -> pl.DataFrame:
    """
    makes an aggregation on top of `create_blob_block_df`
    """
    return blob_block_df.group_by('slot_time').agg(
        pl.col('block_number').first().alias('block_number'),
        pl.col('extra_data').first(),
        pl.col('base_fees_per_block_eth').sum().alias(
            'base_fees_per_block_eth'),
        pl.col('priority_fees_per_block_eth').sum().alias(
            'priority_fees_per_block_eth'),
        pl.col('total_tx_fees_per_block_eth').sum().alias(
            'total_tx_fees_per_block_eth'),
        pl.col('avg_slot_inclusion_rate_per_block').mean().alias(
            'avg_slot_inclusion_rate_per_block'),
        pl.col('avg_priority_fee_gas_per_block_gwei').mean().alias(
            'avg_priority_fee_gas_per_block_gwei'),
        pl.col('base_fee_per_gas').mean(),
        pl.col('blobs_per_block').sum().alias('blobs_per_block'),
    ).sort(by='block_number')
