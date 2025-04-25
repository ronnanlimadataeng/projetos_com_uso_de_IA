# Databricks notebook source
from pyspark.sql import Row

name_project = 'proj96'
name_metastore = 'adlsproj96uceus'

print(name_project)
print(name_metastore)

# COMMAND ----------

data = [
    Row(type=0, tipo='catalog, external location, grants', catalog_name='prd', location_name='prd_catalog', schema_name='', url_str=f'abfss://metastore@{name_metastore}.dfs.core.windows.net/uc-metastore-{name_project}-eus/prd', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='prd', location_name='prd_bronze', schema_name='bronze', url_str=f'abfss://bronze@adls{name_project}prd.dfs.core.windows.net/', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='prd', location_name='prd_controller', schema_name='controller', url_str=f'abfss://controller@adls{name_project}prd.dfs.core.windows.net/', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='prd', location_name='prd_gold', schema_name='gold', url_str=f'abfss://gold@adls{name_project}prd.dfs.core.windows.net/', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='prd', location_name='prd_landing', schema_name='landing', url_str=f'abfss://landing@adls{name_project}prd.dfs.core.windows.net/', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='prd', location_name='prd_sandbox', schema_name='sandbox', url_str=f'abfss://sandbox@adls{name_project}prd.dfs.core.windows.net/', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='prd', location_name='prd_silver', schema_name='silver', url_str=f'abfss://silver@adls{name_project}prd.dfs.core.windows.net/', credential_name='prd_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=0, tipo='catalog, external location, grants', catalog_name='qa', location_name='qa_catalog', schema_name='', url_str=f'abfss://metastore@{name_metastore}.dfs.core.windows.net/uc-metastore-{name_project}-eus/qa', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='qa', location_name='qa_bronze', schema_name='bronze', url_str=f'abfss://bronze@adls{name_project}qa.dfs.core.windows.net/', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='qa', location_name='qa_controller', schema_name='controller', url_str=f'abfss://controller@adls{name_project}qa.dfs.core.windows.net/', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='qa', location_name='qa_gold', schema_name='gold', url_str=f'abfss://gold@adls{name_project}qa.dfs.core.windows.net/', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='qa', location_name='qa_landing', schema_name='landing', url_str=f'abfss://landing@adls{name_project}qa.dfs.core.windows.net/', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='qa', location_name='qa_sandbox', schema_name='sandbox', url_str=f'abfss://sandbox@adls{name_project}qa.dfs.core.windows.net/', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='qa', location_name='qa_silver', schema_name='silver', url_str=f'abfss://silver@adls{name_project}qa.dfs.core.windows.net/', credential_name='qa_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=0, tipo='catalog, external location, grants', catalog_name='dev', location_name='dev_catalog', schema_name='', url_str=f'abfss://metastore@{name_metastore}.dfs.core.windows.net/uc-metastore-{name_project}-eus/dev', credential_name='dev_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='dev', location_name='dev_bronze', schema_name='bronze', url_str=f'abfss://bronze@adls{name_project}dev.dfs.core.windows.net/', credential_name='dev_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='dev', location_name='dev_controller', schema_name='controller', url_str=f'abfss://controller@adls{name_project}dev.dfs.core.windows.net/', credential_name='dev_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='dev', location_name='dev_gold', schema_name='gold', url_str=f'abfss://gold@adls{name_project}dev.dfs.core.windows.net/', credential_name='dev_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='dev', location_name='dev_landing', schema_name='landing', url_str=f'abfss://landing@adls{name_project}dev.dfs.core.windows.net/', credential_name='dev_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='dev', location_name='dev_sandbox', schema_name='sandbox', url_str=f'abfss://sandbox@adls{name_project}dev.dfs.core.windows.net/', credential_name='dev_credentials', root='GROUP_DATA_OWNERS'),
    Row(type=1, tipo='schema, external location, grants', catalog_name='dev', location_name='dev_silver', schema_name='silver', url_str=f'abfss://silver@adls{name_project}dev.dfs.core.windows.net/', credential_name='dev_credentials', root='GROUP_DATA_OWNERS')
]

df = spark.createDataFrame(data)
df.createOrReplaceTempView("view_catalog_external_location_grants")

display(df)

# adlsproj96uceusprd.dfs.core.windows.net

# COMMAND ----------

data = df.collect()

execution_status = []

for row in data:
    type = row.type
    catalog_name = row.catalog_name
    location_name = row.location_name
    schema_name = row.schema_name
    url_str = row.url_str
    credential_name = row.credential_name
    root = row.root

    try:
        # CREATE EXTERNAL LOCATION
        spark.sql(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS {location_name}
            URL '{url_str}'
            WITH (STORAGE CREDENTIAL {credential_name})
        """)
        execution_status.append(f"CREATE EXTERNAL LOCATION {location_name}: SUCCESS")

        spark.sql(f'''
        ALTER EXTERNAL LOCATION {location_name} OWNER TO {root}
        ''')
        execution_status.append(f"ALTER EXTERNAL LOCATION {location_name} OWNER TO {root}: SUCCESS")

        # GRANT ALL PRIVILEGES
        spark.sql(f'''
        GRANT ALL PRIVILEGES ON EXTERNAL LOCATION {location_name} TO {root}
        ''')
        execution_status.append(f"GRANT ALL PRIVILEGES ON EXTERNAL LOCATION {location_name} TO {root}: SUCCESS")

        # GRANT MANAGE
        spark.sql(f'''
        GRANT MANAGE ON EXTERNAL LOCATION {location_name} TO {root}
        ''')
        execution_status.append(f"GRANT MANAGE ON EXTERNAL LOCATION {location_name} TO {root}: SUCCESS")

        #Type = 0 = Catalogo
        if type == 0:
            spark.sql(f"""
                CREATE CATALOG IF NOT EXISTS {catalog_name}
                    MANAGED LOCATION '{url_str}'
            """)
            execution_status.append(f"CREATE CATALOG {catalog_name}: SUCCESS")

            spark.sql(f"""
                GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO {root}
            """)
            execution_status.append(f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO {root}: SUCCESS")

            spark.sql(f"""
                GRANT EXTERNAL USE SCHEMA ON CATALOG {catalog_name} TO {root}
            """)
            execution_status.append(f"GRANT EXTERNAL USE SCHEMA ON CATALOG {catalog_name} TO {root}: SUCCESS")

            spark.sql(f"""
                GRANT MANAGE ON CATALOG {catalog_name} TO {root}
            """)
            execution_status.append(f"GRANT MANAGE ON CATALOG {catalog_name} TO {root}: SUCCESS")

        #Type = 1 = Schema
        if type == 1:
            spark.sql(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}
                MANAGED LOCATION '{url_str}'
            """)
            execution_status.append(f"CREATE SCHEMA {catalog_name}.{schema_name}: SUCCESS")

            spark.sql(f'''
            ALTER SCHEMA {catalog_name}.{schema_name} OWNER TO {root}
            ''')
            execution_status.append(f"ALTER SCHEMA {catalog_name}.{schema_name} OWNER TO {root}: SUCCESS")

    except Exception as e:
        execution_status.append(f"Error processing {location_name}: {str(e)}")

display(execution_status)