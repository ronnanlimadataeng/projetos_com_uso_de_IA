# Databricks notebook source
# MAGIC %run ../0_Config/0-Init

# COMMAND ----------

var_worflow = 'CustomVisonMlWorkflow'

# COMMAND ----------

import requests
import json

Authorization = f"Bearer {var_secret_datalake}"

parametros = {
    "name": var_worflow #Verifica se o job existe e colocar o nome
}
headers ={
'Authorization': Authorization
,"Content-Type" : "application/json"
}
url = f"{var_databricks_url}/api/2.2/jobs/list"
resp = requests.get(url, params=parametros, headers=headers)

if resp.status_code == 200:
    jobs = resp.json().get("jobs", [])
    for job in jobs:
        if job["settings"]["name"] == var_worflow: #Verifica se o job existe e colocar o nome
            job_id = job["job_id"]
            break

print(resp.status_code)
print(resp.text)
print(f"Job ID: {job_id}")

# COMMAND ----------

import requests
import json

Authorization = f"Bearer {var_secret_datalake}"

parametros = {
    "job_id" :  job_id
}
headers ={
'Authorization': Authorization
,"Content-Type" : "application/json"
}
url = f"{var_databricks_url}/api/2.1/jobs/run-now"
resp = requests.post(url, json=parametros, headers=headers)

print(resp.status_code)
print(resp.text)