from query_loader import render_sql
from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table import expressions as E
from pyflink.table.window import Tumble

def setup_tables(t_env: TableEnvironment):
    """Defines the Kafka source and Filesystem sink tables."""
    t_env.execute_sql(render_sql('tbl_impressions'))
    t_env.execute_sql(render_sql('tbl_clicks'))
    t_env.execute_sql(render_sql('tbl_ctr'))
    

def compute_ctr(t_env: TableEnvironment):
    impressions = t_env.from_path("impressions")
    clicks = t_env.from_path("clicks")

    # MUST use the new 'event_time' column for all time operations
    aliased_impressions = impressions.select(
        impressions.impr_id,
        impressions.campaign_id.alias("impr_campaign_id"),
        impressions.event_time.alias("impr_ts")
    )
    # MUST use the new 'event_time' column here as well
    aliased_clicks = clicks.select(
        clicks.click_id,
        clicks.impr_id.alias("click_impr_id"),
        clicks.event_time.alias("click_ts")
    )

    joined_stream = aliased_impressions.join(aliased_clicks).where(
        (aliased_impressions.impr_id == aliased_clicks.click_impr_id) &
        (aliased_clicks.click_ts.between(
            aliased_impressions.impr_ts,
            aliased_impressions.impr_ts + E.lit(15).seconds
        ))
    )

    windowed_stream = joined_stream.window(
        Tumble.over(E.lit(30).seconds).on(joined_stream.impr_ts).alias("w")
    )

    # Group by window and campaign, then aggregate using aliased columns
    ctr_results = windowed_stream.group_by(
        E.col("w"), joined_stream.impr_campaign_id
    ).select(
        joined_stream.impr_campaign_id.alias("campaign_id"),
        E.col("w").start.alias("window_start"),
        E.col("w").end.alias("window_end"),
        joined_stream.impr_id.count.distinct.alias("impressions"),
        joined_stream.click_id.count.distinct.alias("clicks")
    )

    # Calculate CTR and execute the insert operation
    ctr_results.select(
        ctr_results.campaign_id,
        ctr_results.window_start,
        ctr_results.window_end,
        ctr_results.impressions,
        ctr_results.clicks,
        (ctr_results.clicks.cast(DataTypes.DOUBLE()) / ctr_results.impressions.cast(DataTypes.DOUBLE())).alias("ctr")
    ).execute_insert("ctr_by_campaign").wait()


def main():
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    t_env.get_config().get_configuration().set_string("execution.checkpointing.interval", "30s")

    setup_tables(t_env=t_env)
    compute_ctr(t_env=t_env)

if __name__ == "__main__":
    main()