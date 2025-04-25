# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 002-Configs
# MAGIC
# MAGIC Responsável por carregar variáveis de ambiente e segredos necessários para a execução dos notebooks seguintes.
# MAGIC
# MAGIC Este notebook realiza:
# MAGIC - Leitura do ambiente de execução (`environment`)
# MAGIC - Identificação do Data Lake (`abfss_data_lake`)
# MAGIC - Recuperação segura de token via Databricks Secrets
# MAGIC
# MAGIC Recomenda-se executá-lo antes de qualquer notebook que dependa dessas configurações.

# COMMAND ----------

# Environment variables
var_environment = os.getenv('environment')
var_data_lake_name = os.getenv('abfss_data_lake')
var_secret_datalake = dbutils.secrets.get(scope="databricks-scope", key="secret-databricks-token")

# Validation prints
print(f"Environment: {var_environment}")
print(f"Data Lake Name: {var_data_lake_name}")
print("Secret Data Lake retrieved successfully.")

# Environment variables
import os
var_workflow_url = os.getenv('workflow_url')
var_databricks_url = os.getenv('databricks_url')
var_dev = f'abfss://landing@adlsproj96dev.dfs.core.windows.net'
var_landing = f'abfss://landing@{var_data_lake_name}.dfs.core.windows.net'
var_bronze = f'abfss://bronze@{var_data_lake_name}.dfs.core.windows.net'
var_silver = f'abfss://silver@{var_data_lake_name}.dfs.core.windows.net'
var_gold = f'abfss://gold@{var_data_lake_name}.dfs.core.windows.net'
var_controller = f'abfss://controller@{var_data_lake_name}.dfs.core.windows.net'
var_secret_datalake = dbutils.secrets.get(scope="databricks-scope", key="secret-databricks-token")
var_bronze_schema = 'bronze'
var_silver_schema = 'silver'
var_gold_schema = 'gold'

# Print statements for validation
print(f"Environment: {var_environment}")
print(f"Data Lake Name: {var_data_lake_name}")
print(f"Workflow URL: {var_workflow_url}")
print(f"Databricks URL: {var_databricks_url}")
print(f"Dev: {var_dev}")
print(f"Landing: {var_landing}")
print(f"Bronze: {var_bronze}")
print(f"Silver: {var_silver}")
print(f"Gold: {var_gold}")
print(f"Controller: {var_controller}")
print(f"Secret Data Lake: {var_secret_datalake}")
print(f"Bronze Schema: {var_bronze_schema}")
print(f"Silver Schema: {var_silver_schema}")
print(f"Gold Schema: {var_gold_schema}")