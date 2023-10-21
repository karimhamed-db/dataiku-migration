import os
import re
import sqlglot

def mkdir_local(path):
  if not os.path.exists(path):
    os.makedirs(path, exist_ok=True)

def write_to_local_path(payload, output_path):
  mkdir_local(os.path.dirname(output_path))
  with open(output_path, 'w') as fh:
    fh.write(payload)

def get_zone(dss_project, zone_name):
  dss_flow = dss_project.get_flow()
  dss_zone = None
  for zone in dss_flow.list_zones():
    if zone.name == zone_name:
      dss_zone = zone
      break

  if not dss_zone:
    raise Exception(f"Zone {zone_name} not found")
  
  return dss_zone

def create_notebook_from_recipe(recipe, variables, output_path):
  config_payload = _add_parameter_payload(variables)
  recipe_payload = recipe.get_settings().get_payload()
  converted_query = convert_snowflake_to_databricks_query(recipe_payload)
  cleaned_query = _clean_query(converted_query)

  output_name = f"{recipe.project_key}_{recipe.get_settings().get_flat_output_refs()[0]}"

  payload = f"""
%run ../config
# COMMAND ----------\n
{config_payload}
# COMMAND ----------\n
def {recipe.name.lower()}():
  spark.sql(f\"\"\"\n{cleaned_query}\n\"\"\").createOrReplaceGlobalTempView("{output_name}")
# COMMAND ----------\n
{recipe.name.lower()}()
"""
  write_to_local_path(payload, output_path)
    
def _clean_query(payload):
  cleaned_payload = payload.replace("\"", "").replace("$", "")
  return cleaned_payload
    
def _add_parameter_payload(variables):
  payload = "(\n"
  indent = "    "
  for key in variables.keys():
    payload += f"{indent}{key},\n"
  payload += ") = set_up_variables(dbutils, spark)"
  return payload
    
def create_config_notebook(variables, output_path):
  payload = "def set_up_variables(dbutils, spark):\n"
  #setting up the widgets
  for key, value in variables.items():
    if(isinstance(value, str)):
      value = value.replace("::timestamp", "")
      payload += f"    dbutils.widgets.text(\"{key}\", \"{value}\")\n"
    else:
      payload += f"    dbutils.widgets.text(\"{key}\", \"{value}\")\n"
  
  #parsing the widgets
  payload += "\n"
  for key in variables.keys():
    payload += f"    {key} = dbutils.widgets.get(\"{key}\")\n"
    
  #returning the tuple
  payload += "\n"
  payload += "    return ("
  for key in variables.keys():
    payload += f"{key},"
  payload += ")\n"
  
  write_to_local_path(payload, output_path)

def import_notebook_to_dbx(dbx_ws_api, local_path, dbx_import_path, overwrite=True):
  dbx_ws_api.mkdirs(os.path.dirname(dbx_import_path))
  dbx_ws_api.import_workspace(local_path,
                              dbx_import_path,
                              language="PYTHON",
                              fmt="auto",
                              is_overwrite=overwrite
                             )

def is_sync_recipe(recipe):
  return recipe.get_settings().type == 'sync'

def is_sql_recipe(recipe):
  return recipe.get_settings().type == 'sql_query'

def is_python_recipe(recipe):
  return recipe.get_settings().type == 'python'

def is_pivot_recipe(recipe):
  return recipe.get_settings().type == 'pivot'

def convert_snowflake_to_databricks_query(query):
  parsed_query = sqlglot.parse_one(query, read="snowflake")

  tables_to_skip = [cte.alias for cte in parsed_query.find_all(sqlglot.exp.CTE)]

  for table in parsed_query.find_all(sqlglot.exp.Table):
    if table.name not in tables_to_skip:
      new_table_alias = f"AS {table.alias}" if table.alias else ""
      new_table_name = f"global_temp.{table.name} {new_table_alias}"
      table.replace(sqlglot.exp.Table(this=new_table_name))

  transpiled_query = parsed_query.sql(dialect="databricks", pretty=True)
  results = re.compile(re.escape("DATEDIFF(days"), re.IGNORECASE).sub("DATEDIFF(day", transpiled_query)
  results = re.compile(re.escape("DATEADD(days"), re.IGNORECASE).sub("DATEADD(day", transpiled_query)
  
  return results

def get_recipe_source_datasets(recipe, dss_project):
  datasets = {"UploadedFiles":{}, "Snowflake": {}}
  recipe_settings = recipe.get_settings()
  input_datasets = recipe_settings.get_flat_input_refs()
  
  for dataset_name in input_datasets:
    dataset = dss_project.get_dataset(dataset_name).get_settings().get_raw()
    if not dataset["managed"]:
      if dataset["type"] == "UploadedFiles" and is_sync_recipe(recipe):
        dataset_name = recipe_settings.get_flat_output_refs()[0]
        datasets[dataset["type"]][dataset_name] = {"dataset_name": dataset_name, "table_name": f"{recipe.project_key}_{dataset_name}"}
      elif dataset["type"] == "Snowflake":
        datasets[dataset["type"]][dataset["name"]] = {"database_name":dataset["params"]["catalog"], "schema_name":dataset["params"]["schema"], "table_name":dataset["params"]["table"]}
  
  return datasets

def get_upstream_recipes_for_managed_dataset(recipe, dss_project):
  input_datasets = recipe.get_settings().get_flat_input_refs()
  upstream_recipes = []
  
  for dataset_name in input_datasets:
    dataset = dss_project.get_dataset(dataset_name)
    dataset_settings = dataset.get_settings().get_raw()
    if dataset_settings["managed"]:
      upstream_recipes += [dss_project.get_recipe(usage['objectId']) for usage in dataset.get_usages() if usage['type'] == 'RECIPE_OUTPUT']

  return upstream_recipes

def create_snowflake_source_dataset_notebook(dataset_list, snowflake_connection, output_path):
  payload = f"""
def read_snowflake_table(database_name, schema_name, table_name, snowflake_connection):
  snowflake_connection_options = {{
    "sfUrl": snowflake_connection["url"],
    "sfUser": dbutils.secrets.get(snowflake_connection["user"]["secret_scope"], snowflake_connection["user"]["secret_key"]),
    "sfPassword": dbutils.secrets.get(snowflake_connection["password"]["secret_scope"], snowflake_connection["password"]["secret_key"]),
    "sfRole": snowflake_connection["role"],
    "sfWarehouse": snowflake_connection["warehouse"],
  }}

  return (spark.read
    .format("snowflake")
    .options(**snowflake_connection_options)
    .option("sfDatabase", database_name)
    .option("sfSchema", schema_name)
    .option("dbtable", table_name)
    .load()
  ).createOrReplaceGlobalTempView(table_name)
# COMMAND ----------\n
dataset_list = {dataset_list}
# COMMAND ----------\n
snowflake_connection = {snowflake_connection}
# COMMAND ----------\n
for dataset in dataset_list:
  read_snowflake_table(dataset["database_name"], dataset["schema_name"], dataset["table_name"], snowflake_connection)
"""

  write_to_local_path(payload, output_path)

def get_runnable_recipes(items):
  res = []
  for sub in items:
    if sub['type']=='RUNNABLE_RECIPE':
      obj = {'name': sub['ref'], 'type': sub['type'], 'pre': sub['predecessors'], 'sucs': sub['successors']}
      res.append(obj)
  return(res)

def create_pyspark_notebook_from_recipe(recipe, output_path):
  recipe_payload = recipe.get_settings().get_payload()

  payload = f"""
  {recipe_payload}
  """
  write_to_local_path(payload, output_path)


def create_uploaded_source_dataset_notebook(dataset_list, output_path):
  payload = f"""
%pip install openpyxl
# COMMAND ----------\n
import pyspark.pandas as pypd
# COMMAND ----------\n
def read_excel_file(dataset):
  return (
    pypd
      .read_excel(dataset["path"])
      .to_spark()
  ).createOrReplaceGlobalTempView(dataset["table_name"].upper())
# COMMAND ----------\n
dataset_list = {dataset_list}
# COMMAND ----------\n
for dataset in dataset_list:
  read_excel_file(dataset)
"""

  write_to_local_path(payload, output_path)

def create_pivot_function_notebook(output_path):
  payload = f"""
  from pyspark.sql.functions import expr
  
  def pivot_table(df, identifiers, pivot_column, aggs):
    
    expr_list = [expr(f"{{fn['agg']}}({{fn['col']}}) AS {{fn['col']}}_{{fn['agg']}}") for fn in aggs if fn['col'] != '*']
    expr_list += [expr(f"{{fn['agg']}}({{fn['col']}})") for fn in aggs if fn['col'] == '*']
    
    df = (df
      .groupBy(identifiers)
      .pivot(pivot_column)
      .agg(*expr_list)
    )

    if len(expr_list) == 1:
      for column in df.columns:
        if column not in identifiers+[pivot_column]:
          agg = aggs[0]
          if agg['col'] != '*':
            df = df.withColumnRenamed(column, f"{{column}}_{{agg['col']}}_{{agg['agg']}}")
          else: 
            df = df.withColumnRenamed(column, f"{{column}}_{{agg['agg']}}")
    
    return df
  """
  write_to_local_path(payload, output_path)

def create_pivot_notebook_from_recipe(recipe, dss_project, output_path):
  pivot_payload = recipe.get_settings().get_json_payload()
  identifiers = pivot_payload['explicitIdentifiers']
  
  recipe_input_name = recipe.get_settings().get_flat_input_refs()[0]
  input_dataset = dss_project.get_dataset(recipe_input_name).get_settings().get_raw()
  
  input_table_name = ""
  if input_dataset["managed"]:
    input_table_name += f"{recipe.project_key}_"
  input_table_name += recipe_input_name
  
  output_table_name = f"{recipe.project_key}_" + recipe.get_settings().get_flat_output_refs()[0]

  payload = f"""
%run ./pivot
# COMMAND ----------\n
identifiers = {identifiers}
# COMMAND ----------\n
input_table_df = spark.sql(f'select * from global_temp.{input_table_name}')
"""

  for pivot_details in pivot_payload['pivots']:
    aggs = [{"agg": vc["$agg"], "col":vc["column"]} for vc in pivot_details['valueColumns']]
    if not len(aggs):
      aggs = [{"agg": "count", "col": "*"}]

    payload += f"""
# COMMAND ----------\n
pivot_column = '{pivot_details['keyColumns'][0]}'
aggs = {aggs}
df = pivot_table(input_table_df, identifiers, pivot_column, aggs)
"""
  payload += f"""
# COMMAND ----------\n
df.createOrReplaceGlobalTempView('{output_table_name}')
"""

  write_to_local_path(payload, output_path)
