# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame 
from pyspark.sql.functions import col 

# COMMAND ----------

def map_to_spark_type(data_type):
    if 'int' in data_type:
        return IntegerType()
    elif 'float' in data_type:
        return DoubleType()
    elif 'bool' in data_type:
        return BooleanType()
    elif 'datetime' in data_type:
        return TimestampType()
    else:
        return StringType()

# COMMAND ----------

def generate_schema_string(df, struct_type_name):

    if not isinstance(df, DataFrame):
        raise TypeError("The 'df' parameter must be a DataFrame or Dataset.")

    if df.columns:
        columns = df.columns
    else:
        columns = [f"col_{i + 1}" for i in range(len(df.columns))]

    schema_string = f"{struct_type_name} = StructType(["
    for col, dtype in zip(columns, df.dtypes):
        spark_data_type = map_to_spark_type(dtype[1])
        schema_string += f"\n    StructField('{col}', {spark_data_type}, True),"

    schema_string = schema_string.rstrip(',') + '\n])'

    return schema_string           

# COMMAND ----------

spark = SparkSession.builder.appName("ASG").getOrCreate()

# COMMAND ----------

df = spark.createDataFrame([(1, 'John', True, 100.5, '2022-01-01'),
                            (2, 'Jane', False, 200.0, '2022-02-01')],
                           ['id', 'name', 'status', 'amount', 'timestamp'])

# COMMAND ----------

generated_schema_string = generate_schema_string(df , 'MyCustomSchema')

# COMMAND ----------

print('Generated Schema String:')
print(generated_schema_string)

# COMMAND ----------

MyCustomSchema = StructType([
    StructField('id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('status', BooleanType(), True),
    StructField('amount', StringType(), True),
    StructField('timestamp', StringType(), True)
])

# COMMAND ----------

df_no_column_names = spark.createDataFrame([(1, 'John', True, 100.5, '2022-01-01'),
                                            (2, 'Jane', False, 200.0, '2022-02-01')])

# COMMAND ----------

generated_schema_string = generate_schema_string(df_no_column_names , 'MyCustomShema')
print('Generated Schema without column name:')
print(generated_schema_string)


# COMMAND ----------


