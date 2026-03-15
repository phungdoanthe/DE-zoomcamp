from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def create_processed_events_sink_postgres(t_env):
    table_name = 'session_processed_trips'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            pickup_location_id INTEGER,
            num_trips BIGINT,
            PRIMARY KEY (window_start, window_end, pickup_location_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_events_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            trip_distance DOUBLE,
            total_amount DOUBLE,
            pickup_datetime VARCHAR,
            event_timestamp AS TO_TIMESTAMP(pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda:29092',
            'topic' = 'green_trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def log_processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        source_table = create_events_source_kafka(t_env)
        sink_table = create_processed_events_sink_postgres(t_env)

        t_env.execute_sql(
            f"""
            INSERT INTO {sink_table}
            SELECT
                window_start,
                window_end,
                pickup_location_id,
                COUNT(*) AS num_trips
            FROM TABLE(
                SESSION(
                    TABLE {source_table} PARTITION BY pickup_location_id,
                    DESCRIPTOR(event_timestamp),
                    INTERVAL '5' MINUTES
                )
            )
            GROUP BY window_start, window_end, pickup_location_id
            """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))

if __name__ == '__main__':
    log_processing()