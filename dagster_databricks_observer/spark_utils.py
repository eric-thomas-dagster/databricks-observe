from databricks.connect import DatabricksSession

def get_spark_session():
    return DatabricksSession.builder.getOrCreate()