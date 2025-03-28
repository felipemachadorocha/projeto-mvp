# Databricks notebook source
# MAGIC %md
# MAGIC ### **O objetivo do projeto é analisar o comportamento dos clientes em um APP de passagens rodoviárias. As perguntas a serem respondidas são as seguintes:**
# MAGIC
# MAGIC - Qual foi o número de novos cadastros por mês e ano?
# MAGIC - Qual a faixa etária dos clientes que se cadastram por grupo de idade?
# MAGIC - Quantos desses novos clientes realizaram a compra de uma ou mais passagens?
# MAGIC - Quantos clientes realizaram compra na plataforma?
# MAGIC - Quantos clientes realizaram apenas uma compra na plataforma?
# MAGIC - Quantos clientes realizaram mais de uma compra na plataforma?
# MAGIC - Quantos clientes não realizaram nenhum tipo de compra na plataforma?
# MAGIC - Qual foi a taxa de cancelamento de passagens?
# MAGIC - Quais são os destinos mais comprados?
# MAGIC - Qual o tempo médio entre o cadastro e a primeira compra?

# COMMAND ----------

# Importanto Bibliotecas
from pyspark.sql.types  import *
from pyspark.sql.window import Window
from pyspark.sql.functions import * 

# COMMAND ----------

# MAGIC %md
# MAGIC ####Criando a pasta no diretório e carregando os arquivos.

# COMMAND ----------

# Criando uma pasta para armazenar os dados que estão em CSV
dbutils.fs.mkdirs('FileStore/tables/arquivos_mvp')

# COMMAND ----------

# Após a realização do carregamento dos arquivos no DBFS, verificar se os arquivos foram carregados corretamente.
display(dbutils.fs.ls('FileStore/tables/arquivos_mvp'))

# COMMAND ----------

# Carregando o arquivo CSV de Compras do DBFS
dbfs_compras = '/FileStore/tables/arquivos_mvp/Compras.csv'

# Lendo o arquivo CSV para um DataFrame
df_compras = spark.read.csv(dbfs_compras, header=True, inferSchema=True)

# Exibindo o DataFrame
display(df_compras)

# COMMAND ----------

# Carregando o arquivo CSV de Clientes do DBFS
dbfs_clientes = '/FileStore/tables/arquivos_mvp/Clientes.csv'

# Lendo o arquivo CSV para um DataFrame
df_clientes = spark.read.csv(dbfs_clientes, header=True, inferSchema=True)

# Exibindo o DataFrame
display(df_clientes)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verificando e tratando o arquivo Clientes

# COMMAND ----------

# Verificando o tipo de arquivo de df_clientes
df_clientes.printSchema()

# COMMAND ----------

# Verificando valores nulos na tabela clientes:
for coluna in df_clientes.columns:
    print(coluna, df_clientes.filter(col(coluna).isNull()).count())

# COMMAND ----------

# Lógica para nova coluna 'GrupoIdade'
df_clientes = df_clientes.withColumn(
    "GrupoIdade",
    when(col("Idade") <= 19, "Até 19 anos")
    .when((col("Idade") >= 20) & (col("Idade") <= 59), "Entre 20 e 59 anos")
    .when((col("Idade") >= 60), "60+")
    .otherwise("Não Classificado")
)

# Exibe o DataFrame com a nova coluna
display(df_clientes)

# COMMAND ----------

# Tratando para que quando na tabela df_clientes na coluna "Idade" constar um valor nulo preencher com "Não Informado"

df_clientes = df_clientes.withColumn(
    "Idade",
    when(col("Idade").isNull(), "Não Informado").otherwise(col("Idade"))
)

# COMMAND ----------

# Código para verificação do tratamento realizado
display(df_clientes.filter(col("Idade") == "Não Informado"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Verificando e tratando o arquivo Compras

# COMMAND ----------

# Verificando o tipo de arquivo de df_compras:
df_compras.printSchema()

# COMMAND ----------

# Verificando valores nulos na tabela compras:
for coluna in df_compras.columns:
    print(coluna, df_compras.filter(col(coluna).isNull()).count())

# COMMAND ----------

# Tratando para que quando na tabela df_compras na coluna "Destino" constar um valor nulo preencher com "Não Informado"

df_compras = df_compras.withColumn(
    "Destino",
    when(col("Destino").isNull(), "Não Informado").otherwise(col("Destino"))
)

# COMMAND ----------

# Código para verificação do tratamento realizado
display(df_compras.filter(col("Destino") == "Não Informado"))

# COMMAND ----------

#Primeira Camada - Incluindo nova coluna para contagem de compras Aprovadas e Canceladas

# 1. Agrupando por ClienteId, Localizador, TipoCompra e Destino para contar as compras por status
df_contagem_localizadores = df_compras.groupBy("ClienteId", "Localizador", "TipoCompra","Destino") \
    .agg(
        sum(when(col("StatusVenda") == "SALE_APPROVED", 1).otherwise(0)).alias("ContagemAprovadosLocalizador"),
        sum(when(col("StatusVenda") == "SALE_CANCELED", 1).otherwise(0)).alias("ContagemCanceladosLocalizador")
    )

# 2. Definindo a janela para ordenar as compras por VendaId
window_spec = Window.partitionBy("ClienteId").orderBy("VendaId")

# 3. Realizando um join com o DataFrame original para incluir as colunas calculdas
df_compras_join = df_compras.join(
    df_contagem_localizadores, ["ClienteId", "Localizador", "TipoCompra","Destino"], "left"
)

# 4. Definindo a janela para identificar a primeira ocorrência de cada Localizador 
window_spec_localizador = Window.partitionBy("ClienteId", "Localizador", "TipoCompra", "Destino").orderBy("VendaId")

# 5. Criando as colunas com lógica para contar quantas passagens o cliente comprou em uma única compra
df_compras_contagem = df_compras_join.withColumn(
    "PrimeiraOcorrenciaLocalizador",
    row_number().over(window_spec_localizador)
).withColumn(
    "NumeroComprasAprovadas",
    when(col("PrimeiraOcorrenciaLocalizador") == 1, col("ContagemAprovadosLocalizador")).otherwise(0)
).withColumn(
    "NumeroVendasCanceladas",
    when(col("PrimeiraOcorrenciaLocalizador") == 1, -col("ContagemCanceladosLocalizador")).otherwise(0)
).drop("ContagemAprovadosLocalizador", "ContagemCanceladosLocalizador", "PrimeiraOcorrenciaLocalizador")

# Para visualizar o DataFrame com as novas colunas:
display(df_compras_contagem)

# COMMAND ----------

#Criando tabela virtual temporada do SQL 
df_compras_contagem.createOrReplaceTempView("compras_contagem")

# COMMAND ----------

# DBTITLE 1,Script para visualizar a inclusão das novas Colunas
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   ClienteId,
# MAGIC   sum(NumeroComprasAprovadas) ComprasAprovadas,
# MAGIC   sum(NumeroVendasCanceladas) ComprasCanceladas
# MAGIC FROM 
# MAGIC   compras_contagem
# MAGIC GROUP BY
# MAGIC   ClienteId

# COMMAND ----------

#Segunda Camada - Inclusão de nova coluna para verificar identifcar a primeira compra

# Define uma janela particionada por ClienteId e ordenada por DataCompra
window_spec_cliente = Window.partitionBy("ClienteId").orderBy("DataCompra")

# Adiciona uma coluna para numerar as compras de cada cliente por ordem de data
df_compras_numerada = df_compras_contagem.withColumn(
    "NumeroCompraCliente",
    row_number().over(window_spec_cliente)
)

# Filtra a primeira compra de cada cliente
df_primeira_compra = df_compras_numerada.filter(col("NumeroCompraCliente") == 1)

# Define uma janela particionada por ClienteId
window_spec_cliente_agrupado = Window.partitionBy("ClienteId")

# Definir os localizadores da primeira compra em uma lista
df_primeira_compra_agrupada = df_primeira_compra.withColumn(
    "LocalizadoresPrimeiraCompra",
    collect_list("Localizador").over(window_spec_cliente_agrupado)
)

# Seleciona apenas as colunas ClienteId e LocalizadoresPrimeiraCompra
df_primeira_compra_localizadores = df_primeira_compra_agrupada.select("ClienteId", "LocalizadoresPrimeiraCompra")

# Junta o DataFrame original com os localizadores da primeira compra
df_compras_com_primeira = df_compras_numerada.join(df_primeira_compra_localizadores, "ClienteId", "left")

# Cria a coluna 'PrimeiraCompra' usando array_contains
df_compras_primeira_compra = df_compras_com_primeira.withColumn(
    "PrimeiraCompra",
    array_contains(col("LocalizadoresPrimeiraCompra"), col("Localizador"))
).drop("LocalizadoresPrimeiraCompra","NumeroCompraCliente") 

#Para visualizar o DataFrame com a nova coluna:
display(df_compras_primeira_compra)

# COMMAND ----------

#Criando tabela virtual temporaria do SQL 
df_compras_primeira_compra.createOrReplaceTempView("compras_primeira_compra")

# COMMAND ----------

# DBTITLE 1,Script para visualizar a inclusão da nova Coluna
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ClienteId,
# MAGIC   DataCompra,
# MAGIC   PrimeiraCompra
# MAGIC FROM
# MAGIC   compras_primeira_compra
# MAGIC ORDER BY
# MAGIC   ClienteId,
# MAGIC   DataCompra

# COMMAND ----------

#Teceira Camada - Inclusão da Coluna com o total de compras realizada pelo cliente

# Contando os localizadores com StatusVenda 'SALE_APPROVED' e agrupar por ClienteId e Localizador
df_aprovados = df_compras_primeira_compra.filter(col("StatusVenda") == "SALE_APPROVED") \
    .groupBy("ClienteId", "Localizador") \
    .agg(count("*").alias("CountAprovados"))

# Contando os localizadores com StatusVenda 'SALE_CANCELED' e agrupar por ClienteId e Localizador
df_cancelados = df_compras_primeira_compra.filter(col("StatusVenda") == "SALE_CANCELED") \
    .groupBy("ClienteId", "Localizador") \
    .agg(count("*").alias("CountCancelados"))

# Realizando um join entre os dataframes de aprovados e cancelados
df_combinado = df_aprovados.join(df_cancelados, ["ClienteId", "Localizador"], "full") \
    .fillna(0)

# Realizando a contagem dos localizadores
df_contagem_ajustada_localizador = df_combinado.withColumn(
    "ContagemLocalizador",
    when(col("CountAprovados") > col("CountCancelados"), 1)
    .otherwise(0)
)

# Realizando a soma dos localizadores distintos e agrupando por ClienteId 
df_contagem = df_contagem_ajustada_localizador.groupBy("ClienteId") \
    .agg(count("*").alias("TotalLocalizadores"),  # Contagem total de localizadores (para referência)
         sum("ContagemLocalizador").alias("TotalCompras"))

# Realizando um join para incluir a nova coluna
df_compras_total = df_compras_primeira_compra.join(df_contagem.select("ClienteId", "TotalCompras"), "ClienteId", "left")

# Para visualizar o DataFrame com a nova coluna:
display(df_compras_total)

# COMMAND ----------

#Criando tabela virtual temporaria do SQL 
df_compras_total.createOrReplaceTempView("compras_total")

# COMMAND ----------

# DBTITLE 1,Script para visualizar a inclusão da nova Coluna
# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   ClienteId,
# MAGIC   TotalCompras
# MAGIC FROM
# MAGIC   compras_total

# COMMAND ----------

#Quarta Camada - Somente realizando o agrupamento e plotando apenas as colunas necessárias

# Realiza o tratamento para agrupar os totais de compras aprovadas e canceladas
df_compras_agg = df_compras_total.groupBy(
    "ClienteId",
    "Localizador",
    "DataCompra",
    "Destino",
    "MesAnoCompra",
    "TipoCompra",
    "PrimeiraCompra",
    "TotalCompras",
).agg(
    sum("NumeroComprasAprovadas").alias("NumeroComprasAprovadas"),
    sum("NumeroVendasCanceladas").alias("NumeroVendasCanceladas"),
)

# Exibindo o schema
display(df_compras_agg)

# COMMAND ----------

# Quinta Camada - Criando uma nova coluna para classificar as compras de acordo com as regras aplicadas abaixo

# Cria a nova coluna 'CompraAux' somando as colunas existentes
df_compras_aux = df_compras_agg.withColumn("CompraAux", col("NumeroComprasAprovadas") + col("NumeroVendasCanceladas"))

# Cria a nova coluna 'StatusCompra' com base na condição, utilizando o DataFrame 'df_compras_aux'
df_compras_aux = df_compras_aux.withColumn(
    "StatusCompra",
    when(col("CompraAux") > 0, 1).otherwise(0)
).drop("CompraAux")

df_compras_final = df_compras_aux.withColumn(
    "ClassificacaoCompra",
    when((col("TotalCompras") == 0) & (col("StatusCompra") == 0), "Sem Compra")
    .when((col("TotalCompras") == 1) & (col("StatusCompra") == 1) & (col("PrimeiraCompra") == True), "Primeira Compra(Única)")
    .when((col("TotalCompras") == 1) & (col("StatusCompra") == 0) & (col("PrimeiraCompra") == True), "Primeira Compra(Cancelada)")
    .when((col("TotalCompras") == 1) & (col("StatusCompra") == 0) & (col("PrimeiraCompra") == False), "Compra Cancelada(Recompra)")
    .when((col("TotalCompras") == 1) & (col("StatusCompra") == 1) & (col("PrimeiraCompra") == False), "Compra Única(Recompra)")
    .when((col("TotalCompras") > 1) & (col("StatusCompra") == 0) & (col("PrimeiraCompra") == False), "Compra Cancelada")
    .when((col("TotalCompras") > 1) & (col("StatusCompra") == 0) & (col("PrimeiraCompra") == True), "Primeira Compra(Cancelada)")
    .when((col("TotalCompras") > 1) & (col("StatusCompra") == 1) & (col("PrimeiraCompra") == True), "Primeira Compra")
    .when((col("TotalCompras") > 1) & (col("StatusCompra") == 1) & (col("PrimeiraCompra") == False), "Recompra")
).drop("StatusCompra")

# Para visualizar o DataFrame com a nova coluna:
display(df_compras_final)

# COMMAND ----------

# DBTITLE 1,Criando o DataBase
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS mvp

# COMMAND ----------

# DBTITLE 1,Criando Tabela Compras
# MAGIC %sql
# MAGIC USE mvp;
# MAGIC CREATE TABLE IF NOT EXISTS compras(
# MAGIC ClienteId integer,
# MAGIC Localizador string,
# MAGIC DataCompra date,
# MAGIC Destino string,
# MAGIC MesAnoCompra string,
# MAGIC TipoCompra string,
# MAGIC PrimeiraCompra boolean,
# MAGIC TotalCompras long,
# MAGIC NumeroComprasAprovadas long,
# MAGIC NumeroVendasCanceladas long,
# MAGIC ClassificacaoCompra string
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Realizando ingestão dos dados da tabela Compras
database_name = "mvp"
table_name = "compras"
df_compras_final.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Criando Tabela Clientes
# MAGIC %sql
# MAGIC USE mvp;
# MAGIC CREATE TABLE IF NOT EXISTS clientes(
# MAGIC ClienteId integer,
# MAGIC DataCadastro date,
# MAGIC MesAnoCadastro string,
# MAGIC Idade string,
# MAGIC GrupoIdade string
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Realizando ingestão dos dados da tabela Clientes
database_name = "mvp"
table_name = "clientes"
df_clientes.write.format("delta").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")

# COMMAND ----------

# DBTITLE 1,Script para realizar as análises
# MAGIC %sql
# MAGIC SELECT * FROM mvp.clientes

# COMMAND ----------

# DBTITLE 1,Script para realizar as análises
# MAGIC %sql
# MAGIC SELECT
# MAGIC   cp.ClienteId,
# MAGIC   cp.Localizador,
# MAGIC   cp.Destino,
# MAGIC   cp.MesAnoCompra,
# MAGIC   cp.TipoCompra,
# MAGIC   cl.GrupoIdade,
# MAGIC   cp.NumeroComprasAprovadas ComprasAporvadas,
# MAGIC   cp.NumeroVendasCanceladas ComprasCanceladas,
# MAGIC   cp.ClassificacaoCompra
# MAGIC FROM 
# MAGIC   mvp.compras cp
# MAGIC   LEFT JOIN mvp.clientes cl ON (cp.ClienteId = cl.ClienteId)
# MAGIC WHERE 
# MAGIC   year(cp.DataCompra) = '2024'

# COMMAND ----------


