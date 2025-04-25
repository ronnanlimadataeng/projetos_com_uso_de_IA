# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook: 001-RunCustomVisionMlWorkflow
# MAGIC
# MAGIC Este notebook orquestra a execução sequencial de notebooks do projeto `CustomVisionProject`, com controle de timeout e logs básicos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Executar notebooks em ordem, garantindo que cada etapa (configuração, autenticação, validação e inferência) ocorra de forma controlada.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Pré-requisitos
# MAGIC
# MAGIC - Notebooks esperados:
# MAGIC   - `002-Configs`
# MAGIC   - `003-AutenticationKeys`
# MAGIC   - `004-ValidationDatalakeAPI`
# MAGIC   - `005-ConsumeClassificationModel`
# MAGIC - Permissão para executar `dbutils.notebook.run`
# MAGIC - Secrets, tokens e endpoints configurados previamente
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Estrutura do Código
# MAGIC
# MAGIC ### 1. Importação de configurações
# MAGIC ```python
# MAGIC %run /Workspace/Users/<seu-usuario>/CustomVisionProject/002-Configs

# COMMAND ----------

# MAGIC %run /Workspace/Users/ronnan_ok@hotmail.com/CustomVisionProject/002-Configs

# COMMAND ----------

def run_notebook(notebook_name):
    dbutils.notebook.run(notebook_name, 0)

# COMMAND ----------

job_list = [
    "002-Configs"
    ,"003-AutenticationKeys"
    ,"004-ValidationDatalakeAPI"
    ,"005-ConsumeClassificationModel"
]

# COMMAND ----------

from threading import Thread
from queue import Queue
import time

q = Queue()

worker_count = 1
timeout = 600  # 10 minutes in seconds

for job in job_list:
    print(f'Job: {job}')
    q.put(job)

def run_tasks(function, q):
    while not q.empty():
        value = q.get()
        print(f'Task: {value}')
        start_time = time.time()
        try:
            run_notebook(value)
        except Exception as e:
            error_msg = f"Error running notebook {value}: {e}"
            print(error_msg)
        finally:
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout:
                timeout_msg = f"Task {value} exceeded timeout of {timeout} seconds"
                print(timeout_msg)
        q.task_done()

for i in range(worker_count):
    t = Thread(target=run_tasks, args=(run_notebook, q))
    t.daemon = True
    t.start()

q.join()

msg_fim = f'Todos os jobs foram executados.'
print(msg_fim)