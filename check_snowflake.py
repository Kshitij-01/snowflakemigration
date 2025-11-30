#!/usr/bin/env python
"""Check the data in Snowflake after migration."""

import snowflake.connector
from config import load_credentials


def main():
    creds = load_credentials()

    print("Connecting to Snowflake...")
    print(f"  Account: {creds.get('SNOWFLAKE_ACCOUNT')}")
    print(f"  User: {creds.get('SNOWFLAKE_USER')}")
    print(f"  Database: {creds.get('SNOWFLAKE_DATABASE')}")

    conn = snowflake.connector.connect(
        account=creds.get("SNOWFLAKE_ACCOUNT"),
        user=creds.get("SNOWFLAKE_USER"),
        password=creds.get("SNOWFLAKE_PASSWORD"),
        warehouse=creds.get("SNOWFLAKE_WAREHOUSE"),
        database=creds.get("SNOWFLAKE_DATABASE"),
    )

    cursor = conn.cursor()

    try:
        # List schemas
        print("\n" + "=" * 60)
        print("SCHEMAS IN DATABASE")
        print("=" * 60)
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        migration_schemas = []
        for schema in schemas:
            schema_name = schema[1]
            print(f"  - {schema_name}")
            if schema_name not in ("INFORMATION_SCHEMA", "PUBLIC"):
                migration_schemas.append(schema_name)

        # Check each migration schema
        for schema_name in migration_schemas:
            print("\n" + "=" * 60)
            print(f"SCHEMA: {schema_name}")
            print("=" * 60)

            # Tables
            cursor.execute(f"SHOW TABLES IN SCHEMA {schema_name}")
            tables = cursor.fetchall()
            table_names = [t[1] for t in tables]
            print(f"Tables: {table_names}")

            # Row counts
            print("\nRow counts:")
            for table_name in table_names:
                try:
                    cursor.execute(f'SELECT COUNT(*) FROM {schema_name}.{table_name}')
                    count = cursor.fetchone()[0]
                    print(f"  {table_name}: {count} rows")
                except Exception as e:
                    print(f"  {table_name}: ERROR - {e}")

            # Sample data
            print("\nSample data (first 2 rows per table):")
            for table_name in table_names:
                print(f"\n--- {table_name} ---")
                try:
                    cursor.execute(f'SELECT * FROM {schema_name}.{table_name} LIMIT 2')
                    columns = [desc[0] for desc in cursor.description]
                    print(f"Columns: {columns}")
                    rows = cursor.fetchall()
                    for row in rows:
                        print(f"  {row}")
                except Exception as e:
                    print(f"  ERROR: {e}")

    finally:
        cursor.close()
        conn.close()

    print("\n" + "=" * 60)
    print("SNOWFLAKE CHECK COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
