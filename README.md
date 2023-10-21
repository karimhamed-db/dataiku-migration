# Dataiku Migration

This repo contain all scripts and utility functions for migrating a zone from Dataiku to Databricks

## Description

The script migrate a Dataiku project zone into a Databricks Job. It traces the dependencies of the recipes in a zone and creates the same dependencies graph in a Databricks job. The script also manages to trace dependencies across zone, if any.

The scripts requires at least one recipe name to start from, from where it will trace the dependencies from. It goes from right to left (i.e. from leaf to root).

The script currently supports the following Dataiku recipes
- SQL
- Python
- Pivot
- Sync


The script will create a notebook for each SQL and Python recipe with the name of the original Dataiku recipe. For SQL recipes, it will wrap the query in a python function and it will create a `global` view from it. This is so that views can be accessed across notebooks when running it as a Databricks Job. 

**NOTE:** The current version of the script was built for a specific use case to support migrating Snowflake SQL quereis into Databricks compatible queries. It uses SQLGlot to support in the transpiling and conversion of the SQL queries. It will create two extra notebooks to load sources tables from Snowflake. It will also create another notebook for excel files manually uploaded. Currently this is reflected as `sync` recipes in Dataiku

## How to use

- Fill the `config_properties.py` with the required values

```python
# Dataiku Configuration Details
project_name = ""
zone_name = ""
# Entry point recipes to start the migration from
entry_recipe_names = []
dataiku_uri = ""
dataiku_token = ""


# Databricks Configuration Details
dbx_uri = ""
dbx_output_dir = ""
dbx_token = ""
```

- Update the job configuration in the `dataiku_migration_script.py` with the required cluster configuration
- Run the `dataiku_migration_script.py` from top to bottom in a Databricks envrionment.