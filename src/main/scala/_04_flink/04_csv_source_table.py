from pyflink.table import (
    EnvironmentSettings, TableEnvironment, DataTypes,
    CsvTableSource
)

settings = EnvironmentSettings.new_instance() \
    .in_batch_mode().build()
table_env = TableEnvironment.create(settings)

field_names = ["framework", "chapter"]
field_types = [DataTypes.STRING(), DataTypes.BIGINT()]

source = CsvTableSource(
    "./sample.csv",
    field_names,
    field_types,
    ignore_first_line=False
)

table_env.register_table_source("chapters", source)
table = table_env.from_path("chapters")

print(table.to_pandas())
