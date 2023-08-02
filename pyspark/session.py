from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

app = 'pysparkPandas'

conf = (SparkConf().setAppName(app)
                    .setMaster('local[1]')
                    .set('spark.logConf', 'true'))

sparkSession = SparkSession.builder.appName(app).config(conf=conf).getOrCreate()