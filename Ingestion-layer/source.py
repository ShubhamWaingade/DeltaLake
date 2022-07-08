from pyspark.sql import SparkSession
from pyspark.sql import DataFrame


class Sources:

    def get_from_csv(spark:SparkSession, path: str) -> DataFrame:
        return (spark.read.format('csv').options(header=True).load(path))

    def get_from_multiple_csv(spark:SparkSession, path: str) -> DataFrame:
        return (spark.read.format("csv").
                option("recursiveFileLookup", "true").
                option("header", "true").load(path))


