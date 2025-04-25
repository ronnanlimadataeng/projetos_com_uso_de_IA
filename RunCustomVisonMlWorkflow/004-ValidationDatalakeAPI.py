# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 004-ValidationDatalakeAPI
# MAGIC
# MAGIC Valida a autenticação e conectividade com o serviço Azure Custom Vision, garantindo que as credenciais, endpoints e projetos estejam acessíveis antes da execução principal do pipeline.
# MAGIC
# MAGIC Este notebook realiza:
# MAGIC - Instalação do pacote `azure-cognitiveservices-vision-customvision`
# MAGIC - Inicialização dos clientes de treino e predição via API Keys
# MAGIC - Validação do acesso à API de treinamento listando projetos disponíveis
# MAGIC - Validação da API de predição classificando uma imagem de teste
# MAGIC
# MAGIC ⚠️ É necessário instalar os pacotes que do custom vision antes de executar autenticações ou libs.

# COMMAND ----------

# MAGIC %run /Workspace/Users/ronnan_ok@hotmail.com/CustomVisionProject/002-Configs

# COMMAND ----------

# MAGIC %run /Workspace/Users/ronnan_ok@hotmail.com/CustomVisionProject/003-AutenticationKeys

# COMMAND ----------

# MAGIC %pip install azure-cognitiveservices-vision-customvision

# COMMAND ----------

from azure.cognitiveservices.vision.customvision.training import CustomVisionTrainingClient
from azure.cognitiveservices.vision.customvision.prediction import CustomVisionPredictionClient
from azure.cognitiveservices.vision.customvision.training.models import ImageFileCreateBatch, ImageFileCreateEntry, Region
from msrest.authentication import ApiKeyCredentials
from azure.core.credentials import AzureKeyCredential
from io import BytesIO
import os, time, uuid
import requests
from datetime import datetime

# COMMAND ----------

credentials = ApiKeyCredentials(in_headers={"Training-key": training_key})
trainer = CustomVisionTrainingClient(training_endpoint, credentials)
prediction_credentials = ApiKeyCredentials(in_headers={"Prediction-key": prediction_key})
predictor = CustomVisionPredictionClient(prediction_endpoint, prediction_credentials)

# COMMAND ----------

try:
    training_credentials = ApiKeyCredentials(in_headers={"Training-key": training_key})
    trainer = CustomVisionTrainingClient(training_endpoint, training_credentials)

    # Testa listando projetos
    projects = trainer.get_projects()
    print(f"✅ Training API OK - {len(projects)} projeto(s) encontrados.")
except Exception as e:
    print(f"❌ Erro ao acessar a Training API: {str(e)}")

# COMMAND ----------

try:
    # === Autenticação no Custom Vision ===
    prediction_credentials = ApiKeyCredentials(in_headers={"Prediction-key": prediction_key})
    predictor = CustomVisionPredictionClient(prediction_endpoint, prediction_credentials)

    # URL da imagem para teste
    image_url = 'https://upload.wikimedia.org/wikipedia/commons/6/65/Fiat_Uno_Sting_-_top_view.jpg'
    
    result = predictor.classify_image_url(
        project_id,
        publish_iteration_name,
        image_url
    )

    print("✅ Prediction API OK")
    for prediction in result.predictions:
        print(f"{prediction.tag_name}: {prediction.probability:.2%}")

except Exception as e:
    print(f"❌ Erro ao acessar a Prediction API: {str(e)}")