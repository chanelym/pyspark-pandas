import sys
sys.path.insert(0, '')

from session import sparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

df1 = sparkSession.read.format("parquet").load("data/Clientes.parquet")
df1.show()