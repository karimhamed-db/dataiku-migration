# Databricks notebook source
# MAGIC %md
# MAGIC ### Installing Dataiku Clients

# COMMAND ----------

# MAGIC %pip install dataiku-api-client

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sometimes an extra internal Dataiku client may be required

# COMMAND ----------

# MAGIC %pip install https://<dataiku-host>/public/packages/dataiku-internal-client.tar.gz

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing Required Dependencies

# COMMAND ----------

# MAGIC %pip install sqlglot

# COMMAND ----------

# MAGIC %pip install databricks-sdk

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loading Configurations

# COMMAND ----------

# Dataiku Configuration Details
project_name = ""
zone_name = ""
# Entry point recipes to start the migration from
entry_recipe_names = []
dataiku_uri = ""
dataiku_token = ""

# COMMAND ----------

# Databricks Configuration Details
dbx_uri = ""
dbx_output_dir = ""
dbx_token = ""
local_output_dir = f"/tmp/{project_name}/{zone_name}"

# COMMAND ----------

snowflake_connection = {
  "url": "",
  "user": ""
  "password": "",
  "role": "",
  "warehouse": ""
}
