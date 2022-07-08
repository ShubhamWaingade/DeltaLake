import findspark
findspark.init()
from pyspark.sql import *
import logging
from source import Sources


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


try:

    spark = SparkSession.builder.master("local").appName("Delta").getOrCreate()
    path = "data/regions.csv"
    path2 = "data"
    df = Sources.get_from_csv(spark=spark, path=path)
    df.show()
    logger.info("Dataframe imported!")

    logger.info("Now loading multiple files recursively")
    df2 = Sources.get_from_multiple_csv(spark, path2)
    df2.show()
    logger.info("Second dataframe created!")
except Exception as e:
    logger.info(e)

