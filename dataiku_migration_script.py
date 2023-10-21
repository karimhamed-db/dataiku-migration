# Databricks notebook source
# MAGIC %run ./config_properties

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from dataiku_helper import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Imports & Notebook parameters

# COMMAND ----------

import dataiku
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.workspace.api import WorkspaceApi

import os
import base64

# COMMAND ----------

# MAGIC %md
# MAGIC ### Init connections & Project setup
# MAGIC

# COMMAND ----------

dbx_api_client = ApiClient(
  host=dbx_uri,
  token=dbx_token
)

dbx_ws_api = WorkspaceApi(dbx_api_client)

# COMMAND ----------

dataiku.set_remote_dss(
  dataiku_uri,
  dataiku_token)

dss_project = dataiku.api_client().get_project(project_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create global parameters in configuration notebook

# COMMAND ----------

config_local_output_path = local_output_dir + "/config.py"
config_dbx_import_path = dbx_output_dir + "/config"

variables = dss_project.get_variables()['standard']
create_config_notebook(variables, config_local_output_path)
import_notebook_to_dbx(dbx_ws_api, config_local_output_path, config_dbx_import_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create pivot notebook

# COMMAND ----------

pivot_file_path = f"transformations/pivot"
pivot_local_output_path = os.path.join(local_output_dir, pivot_file_path)
pivot_dbx_import_path = os.path.join(dbx_output_dir, pivot_file_path)

create_pivot_function_notebook(pivot_local_output_path)
import_notebook_to_dbx(dbx_ws_api, pivot_local_output_path, pivot_dbx_import_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Traverse through the Flow to create recipe notebooks

# COMMAND ----------

recipes_list = [{"recipe":dss_project.get_recipe(recipe_name), "source_snowflake": False, "source_uploaded": False, "upstream_recipes": {}} for recipe_name in entry_recipe_names]
recipes_map = {recipe["recipe"].name: recipe for recipe in recipes_list}
snowflake_source_datasets = {}
uploaded_source_datasets = {}

# COMMAND ----------

for recipe_obj in recipes_list:
  recipe = recipe_obj["recipe"]
  recipe_source_datasets = get_recipe_source_datasets(recipe, dss_project)
  
  if len(recipe_source_datasets["Snowflake"]):
    recipe_obj["source_snowflake"] = True
    snowflake_source_datasets = {**snowflake_source_datasets, **recipe_source_datasets["Snowflake"]}
  
  if len(recipe_source_datasets["UploadedFiles"]):
    recipe_obj["source_uploaded"] = True
    uploaded_source_datasets = {**uploaded_source_datasets, **recipe_source_datasets["UploadedFiles"]}

  managed_dataset_upstream_recipes = get_upstream_recipes_for_managed_dataset(recipe, dss_project)
  for upstream_recipe in managed_dataset_upstream_recipes:
    if upstream_recipe.name in recipes_map:
      upstream_recipe_obj = recipes_map[upstream_recipe.name]
    else:
      upstream_recipe_obj = {"recipe":upstream_recipe, "source_snowflake": False, "source_uploaded": False, "upstream_recipes": {}}
      recipes_map[upstream_recipe.name] = upstream_recipe_obj
      recipes_list.append(upstream_recipe_obj)
    recipe_obj["upstream_recipes"].setdefault(upstream_recipe.name, upstream_recipe_obj)

  recipe_output_path = f"transformations/{recipe.name}"
  local_output_path = os.path.join(local_output_dir, recipe_output_path)
  dbx_import_path = os.path.join(dbx_output_dir, recipe_output_path)
  
  if is_sql_recipe(recipe):
    create_notebook_from_recipe(recipe, variables, local_output_path)
  elif is_python_recipe(recipe):
    create_pyspark_notebook_from_recipe(recipe, local_output_path)
  elif is_pivot_recipe(recipe):
    create_pivot_notebook_from_recipe(recipe, dss_project, local_output_path)
  else:
    continue
  import_notebook_to_dbx(dbx_ws_api, local_output_path, dbx_import_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Source Datasets

# COMMAND ----------

source_datasets_base_dir = "datasets"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Snowflake source datasets notebook

# COMMAND ----------

snowflake_source_datasets_output_path = os.path.join(source_datasets_base_dir, "snowflake_source_datasets")
snowflake_source_datasets_local_output_path = os.path.join(local_output_dir, snowflake_source_datasets_output_path)
snowflake_source_datasets_dbx_import_path = os.path.join(dbx_output_dir, snowflake_source_datasets_output_path)

create_snowflake_source_dataset_notebook(list(snowflake_source_datasets.values()), snowflake_connection, snowflake_source_datasets_local_output_path)
import_notebook_to_dbx(dbx_ws_api, snowflake_source_datasets_local_output_path, snowflake_source_datasets_dbx_import_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Uploaded File source datasets notebook

# COMMAND ----------

uploaded_source_datasets_output_path = os.path.join(source_datasets_base_dir, "uploaded_source_datasets")
uploaded_source_datasets_local_output_path = os.path.join(local_output_dir, uploaded_source_datasets_output_path)
uploaded_source_datasets_dbx_import_path = os.path.join(dbx_output_dir, uploaded_source_datasets_output_path)

prefix = "file:/Workspace"
file_format = 'xlsx'
uploaded_datasets_paths = [{
  "path": prefix + os.path.join(dbx_output_dir, source_datasets_base_dir, "files", dataset["dataset_name"])+"."+file_format,
  "table_name": dataset["table_name"]
  } 
  for dataset in uploaded_source_datasets.values()
]
create_uploaded_source_dataset_notebook(uploaded_datasets_paths, uploaded_source_datasets_local_output_path)
import_notebook_to_dbx(dbx_ws_api, uploaded_source_datasets_local_output_path, uploaded_source_datasets_dbx_import_path)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### Create Workflow from the Flow Zone

# COMMAND ----------

import os
import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

w = WorkspaceClient()

all_recipe_tasks = []
# Adding the Source Dataset Registrartion
all_recipe_tasks.append({
                  "task_key": "REGISTER_SNOWFLAKE_SOURCE_DATASETS",
                  "depends_on": [],
                  "notebook_task": {
                      "notebook_path": snowflake_source_datasets_dbx_import_path,
                      "source": "WORKSPACE"
                  },
                  "job_cluster_key": "one_worker",
                  "timeout_seconds": 0,
                  "description": "Register the source Datasets from Snowflake"
              })
all_recipe_tasks.append({
                  "task_key": "REGISTER_UPLOADED_SOURCE_DATASETS",
                  "depends_on": [],
                  "notebook_task": {
                      "notebook_path": uploaded_source_datasets_dbx_import_path,
                      "source": "WORKSPACE"
                  },
                  "job_cluster_key": "one_worker",
                  "timeout_seconds": 0,
                  "description": "Register the source Datasets from uploaded files"
              })

for recipe_obj in recipes_list:
  recipe = recipe_obj["recipe"]
  recipe_output_path = f"transformations/{recipe.name}"
  local_output_path = os.path.join(local_output_dir, recipe_output_path)
  dbx_import_path = os.path.join(dbx_output_dir, recipe_output_path)
  dependencies = []
  
  # Adding Source Datasets Registration depedency for all recipes
  if recipe_obj["source_snowflake"]:
    dependencies.append({"task_key": "REGISTER_SNOWFLAKE_SOURCE_DATASETS"})
  
  recipe_dependencies = recipe_obj['upstream_recipes'].values()

  for dependency in recipe_dependencies:
    # if the the upstream recipe is for uploading file, connect this recipe to the upload files notebook
    if dependency["source_uploaded"]:
      dependencies.append({"task_key": "REGISTER_UPLOADED_SOURCE_DATASETS"})
    else:
      dependencies.append({"task_key": dependency['recipe'].name})

  if not (len(recipe_dependencies) or recipe_obj["source_snowflake"] or recipe_obj["source_uploaded"]):
    dependencies.append({"task_key": "REGISTER_SNOWFLAKE_SOURCE_DATASETS"})
    dependencies.append({"task_key": "REGISTER_UPLOADED_SOURCE_DATASETS"})

  if not recipe_obj["source_uploaded"]:
    all_recipe_tasks.append({
                  "task_key": recipe.name,
                  "depends_on": dependencies,
                  "notebook_task": {
                      "notebook_path": dbx_import_path,
                      "source": "WORKSPACE"
                  },
                  "job_cluster_key": "one_worker",
                  "timeout_seconds": 0,
                  "description": recipe.name
              })

# COMMAND ----------

print("Creating the final job")

job_cluster_dicts = [
    {
        "job_cluster_key": "one_worker",
        "new_cluster": {
            "spark_version": "12.2.x-scala2.12",
            "spark_conf": {
                "spark.databricks.delta.preview.enabled": "true",
                "spark.sql.caseSensitive": "true"
            },
            "node_type_id": "c4.2xlarge",
            "custom_tags": {"ResourceClass": "SingleNode"},
            "data_security_mode": "SINGLE_USER",
            "runtime_engine": "STANDARD",
            "aws_attributes":{
              "ebs_volume_type":"GENERAL_PURPOSE_SSD",
              "ebs_volume_count": 1,
              "ebs_volume_size": 100
            },
            ## Seems theres a bug that won't allow for single node clusters
            "num_workers": 1,
        },
    }
]
job_clusters = map(jobs.JobCluster.from_dict, job_cluster_dicts)
tasks = map(jobs.Task.from_dict, all_recipe_tasks)

created_job = w.jobs.create(name=zone_name, tasks=tasks, job_clusters=job_clusters)
print(f"Final Job {created_job}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### End
