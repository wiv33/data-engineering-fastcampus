from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(environment_settings=env_settings)

# hard coding
col_names = ["id", "language"]
d = [
    (1, "php"),
    (2, "python"),
    (3, "c++"),
    (4, "java"),
]
t1 = t_env.from_elements(d, col_names)
t2 = t_env.from_elements(d, col_names)

# p로 시작하는 value
table = t1.where(t1.language.like("p%")).union_all(t2)
print(table.explain())
