from pyflink.table import EnvironmentSettings, TableEnvironment

# batch 환경
batch_settings = EnvironmentSettings.new_instance() \
    .in_batch_mode().build()

batch_table_env = TableEnvironment.create(batch_settings)

# Stream 환경
stream_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()

stream_table_env = TableEnvironment.create(stream_settings)