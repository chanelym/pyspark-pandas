import sys
sys.path.insert(0, '')

from session import sparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import expr
from pyspark.sql.functions import sum as _sum
from pyspark.sql import functions as Func

def load_dataframe(filename):
    dfParquet= sparkSession.read.format("parquet").options(header="true").load(filename)
    return dfParquet

clientes = load_dataframe("data/Atividades/Clientes.parquet")
vendas = load_dataframe("data/Atividades/Vendas.parquet")

# clientes.show()
# vendas.show()

# First consult on Clients
# clientes.select("Cliente", "Estado", "Status").show()

# Second consult on Clients
# clientes.select("Cliente", "Status").where((Func.col("Status") == "Platinum") | (Func.col("Status") == "Gold")).show()

# Third consult joining Clients and Sales
vendas.join(clientes,vendas.ClienteID == clientes.ClienteID) \
    .groupBy(clientes.Status) \
    .agg(_sum("Total")) \
    .orderBy(Func.col("sum(Total)").desc()).show()