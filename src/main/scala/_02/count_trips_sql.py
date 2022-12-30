from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("tip_count_sql").getOrCreate()
# row 데이터 생성
import pandas as pd
import os

# !pip install pyarrow fastparquet

target_directory = '/Users/auto/github/data-engineering-fastcampus/data'
target_data = 'fhvhv_tripdata_2020-03'
if not os.path.isfile(f'{target_directory}/{target_data}.csv'):
    # 시간이 오래 걸리는 변환 작업을 거르기 위한 로직
    df = pd.read_parquet('%s/%s.parquet' % (target_directory, target_data))
    df.to_csv(f'{target_directory}/{target_data}.csv')

data = spark.read.csv(f'{target_directory}/{target_data}.csv', header=True)

data.show()
data.createOrReplaceTempView("mobility_data")
spark.sql("select * from mobility_data").limit(5).show()
# date time을 날짜와 시간으로 분리
spark.sql('select pickup_date, count(*) as trips '
          'from (select split(pickup_datetime, " ")[0] as pickup_date '
          'from mobility_data ) '
          'group by pickup_date').show()
