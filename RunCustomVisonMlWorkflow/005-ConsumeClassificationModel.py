# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 005-ConsumeClassificationModel
# MAGIC
# MAGIC Executa a classificação de imagens utilizando o modelo publicado no Azure Custom Vision e grava os resultados no Data Lake como tabela Delta.
# MAGIC
# MAGIC Este notebook realiza:
# MAGIC - Leitura de imagens armazenadas no Azure Data Lake (ADLS) via ABFSS
# MAGIC - Classificação das imagens com base em um modelo Custom Vision (Carro vs. Moto)
# MAGIC - Extração das probabilidades por classe
# MAGIC - Determinação automática da classe predita com base na maior probabilidade
# MAGIC - Criação de um DataFrame com os resultados da inferência
# MAGIC - Escrita dos dados em uma tabela Delta:
# MAGIC   - Se a tabela `dev.bronze.predict_table_model` existir, os resultados são adicionados (`append`)
# MAGIC   - Caso contrário, a tabela é criada com os dados (`overwrite`)

# COMMAND ----------

# MAGIC %run /Workspace/Users/ronnan_ok@hotmail.com/CustomVisionProject/004-ValidationDatalakeAPI

# COMMAND ----------

df_path_geral = dbutils.fs.ls("abfss://sandbox@adlsproj96dev.dfs.core.windows.net/")
df_path_imagens = dbutils.fs.ls("abfss://sandbox@adlsproj96dev.dfs.core.windows.net/imagens/")

df_path_geral = spark.createDataFrame(df_path_geral)
df_path_imagens = spark.createDataFrame(df_path_imagens)

display(df_path_geral)
display(df_path_imagens)

# COMMAND ----------

from PIL import Image
import io

# === Autenticação no Custom Vision ===
prediction_credentials = ApiKeyCredentials(in_headers={"Prediction-key": prediction_key})
predictor = CustomVisionPredictionClient(prediction_endpoint, prediction_credentials)
# === Caminho ABFSS no ADLS ===
image_paths = df_path_imagens.select("path").collect()

results_list = []

for row in image_paths:
    image_path = row["path"]
    # Convertemos o ABFSS em um caminho DBFS temporário
    # Extrair probabilidades de previsão
    # Obter data de processamento atual
    # Determine classification
    # Adicionar resultados à lista
    local_temp_path = "/tmp/imagem_temp.jpeg"
    dbutils.fs.cp(image_path, f"file:{local_temp_path}")

    with open(local_temp_path, "rb") as image_contents:
        image = Image.open(image_contents)
        image = image.resize((800, 800))  # Resize the image to 800x800(alterado 24/04)
        image_byte_array = io.BytesIO()
        image.save(image_byte_array, format='JPEG')
        image_byte_array = image_byte_array.getvalue()

        results = predictor.classify_image(
            project_id, publish_iteration_name, image_byte_array)
        carros_percent = round(next((p.probability for p in results.predictions if p.tag_name == "Carros"), 0) * 100, 2)
        motos_percent = round(next((p.probability for p in results.predictions if p.tag_name == "Motos"), 0) * 100, 2)
        data_processamento = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        classificacao = "carro" if carros_percent > motos_percent else "moto"
        results_list.append((image_path, carros_percent, motos_percent, classificacao, data_processamento))

df_results = spark.createDataFrame(
    results_list,
    ["fonte_imagem", "percentual_carro", "percentual_moto", "classificacao", "data_processamento"]
)

# Escrever os resultados na tabela especificada
if spark.catalog.tableExists("dev.bronze.predict_table_model"):
    df_results.write.format("delta").mode("append").saveAsTable("dev.bronze.predict_table_model")
else:
    df_results.write.format("delta").mode("overwrite").saveAsTable("dev.bronze.predict_table_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from dev.bronze.predict_table_model

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count_carros from dev.bronze.predict_table_model where classificacao = 'carro'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as count_carros from dev.bronze.predict_table_model where classificacao = 'moto'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # 1. Seguradora – Classificação de itens para cotação automática
# MAGIC Contexto de negócio:
# MAGIC Seguradoras modernas oferecem seguro para diversos bens: veículos, bicicletas, eletrônicos, eletrodomésticos. Quando o cliente envia uma foto para cotação, é preciso identificar corretamente o tipo de item.
# MAGIC
# MAGIC Como o código ajuda:
# MAGIC
# MAGIC Classifica se a imagem é de um celular, bicicleta, notebook, TV, etc.
# MAGIC
# MAGIC Valida se o item enviado bate com o que foi declarado.
# MAGIC
# MAGIC Evita fraudes, acelera o processo de cotação e torna a jornada do cliente mais fluida.
# MAGIC
# MAGIC Pode ser integrado em aplicativos mobile ou portais web.
# MAGIC
# MAGIC Escala para milhares de cotações simultâneas, sem depender de atendimento humano.
# MAGIC
# MAGIC # 2. Indústria – Inspeção visual de produtos na linha de produção
# MAGIC Contexto de negócio:
# MAGIC Empresas que produzem diferentes itens (ex: garrafas, caixas, peças, embalagens) precisam verificar visualmente se cada item está sendo classificado e rotulado corretamente.
# MAGIC
# MAGIC Como o código ajuda:
# MAGIC
# MAGIC O modelo identifica visualmente o tipo de produto fabricado (ex: "garrafa de vidro", "garrafa de plástico", "embalagem danificada").
# MAGIC
# MAGIC Compara com o que deveria estar sendo produzido naquele momento.
# MAGIC
# MAGIC Garante controle de qualidade e rastreabilidade.
# MAGIC
# MAGIC Automatiza o processo de inspeção e reduz retrabalho.
# MAGIC
# MAGIC Escala para centenas de milhares de itens/dia, com execução em tempo real.
# MAGIC
# MAGIC # 3. E-commerce – Classificação de produtos por imagem para marketplaces
# MAGIC Contexto de negócio:
# MAGIC Marketplaces recebem fotos de produtos dos vendedores que, muitas vezes, não categorizam corretamente os itens, o que prejudica buscas e vendas.
# MAGIC
# MAGIC Como o código ajuda:
# MAGIC
# MAGIC Classifica automaticamente as fotos recebidas: "roupa", "calçado", "móvel", "eletrodoméstico", etc.
# MAGIC
# MAGIC Sugere a categoria correta e até a ficha técnica (versão avançada).
# MAGIC
# MAGIC Padroniza a base de produtos, melhora a experiência do usuário e a performance de busca.
# MAGIC
# MAGIC Pode rodar em lote ou no momento do upload de imagens.
# MAGIC
# MAGIC Escala para milhões de anúncios novos por mês.