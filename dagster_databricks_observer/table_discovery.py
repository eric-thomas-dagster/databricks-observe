import os
import databricks.sql

def get_sql_connection():
    return databricks.sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOSTNAME"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
        http_path=os.getenv("DATABRICKS_SQL_HTTP_PATH"),
    )

def get_all_tables():
    conn = get_sql_connection()
    cursor = conn.cursor()
    discovered = []

    # Step 1: Try SHOW CATALOGS — this works in Unity Catalog
    try:
        cursor.execute("SHOW CATALOGS")
        catalogs = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"SHOW CATALOGS failed: {e}")
        catalogs = ["hive_metastore"]  # fallback to default if no UC

    for catalog in catalogs:
        # Step 2: Try SHOW SCHEMAS in catalog
        try:
            cursor.execute(f"SHOW SCHEMAS IN {catalog}")
            schemas = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"Skipping catalog '{catalog}': {e}")
            continue

        for schema in schemas:
            # Step 3: Try SHOW TABLES in catalog.schema
            try:
                cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
                for row in cursor.fetchall():
                    # row[1] is table name in Databricks SHOW TABLES format
                    table_name = row[1]
                    discovered.append((catalog, schema, table_name))
            except Exception as e:
                print(f"Skipping {catalog}.{schema}: {e}")
                continue

    return discovered