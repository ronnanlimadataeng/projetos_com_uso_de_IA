# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 003-AuthenticationKeys
# MAGIC
# MAGIC Define e organiza variáveis de autenticação para os serviços de Visão Computacional utilizados no pipeline de classificação de imagens.
# MAGIC
# MAGIC Este notebook realiza:
# MAGIC - Definição dos endpoints de treino e predição do serviço de Visão
# MAGIC - Atribuição de chaves de acesso e identificadores do projeto
# MAGIC - Organização das variáveis em nomes mais legíveis para uso posterior
# MAGIC
# MAGIC ⚠️ Nenhuma credencial é exposta diretamente. As chaves e endpoints devem estar devidamente configurados no ambiente seguro do Databricks.

# COMMAND ----------

# retrieve environment variables
VISION_TRAINING_ENDPOINT = 'xxxxxxxxxxxxxx' # https://eastus.api.cognitive.microsoft.com/
VISION_TRAINING_KEY = 'xxxxxxxxxxxxxx'

VISION_PREDICTION_ENDPOINT = 'xxxxxxxxxxxxxx'
VISION_PREDICTION_KEY = 'xxxxxxxxxxxxxx'

VISION_PREDICTION_RESOURCE_ID = 'xxxxxxxxxxxxxx'

PROJECT_ID = '25267a8f-b944-4f91-ab86-851ec513136f'
INTERATION_NAME = 'ml_classification_proj96'

# COMMAND ----------

# retrieve environment variables
training_endpoint = VISION_TRAINING_ENDPOINT
training_key = VISION_TRAINING_KEY
prediction_endpoint = VISION_PREDICTION_ENDPOINT
prediction_key = VISION_PREDICTION_KEY
prediction_resource_id = VISION_PREDICTION_RESOURCE_ID
project_id = PROJECT_ID
publish_iteration_name = INTERATION_NAME