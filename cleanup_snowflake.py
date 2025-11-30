#!/usr/bin/env python
"""Clean up Snowflake schemas before re-running migration."""

import snowflake.connector
from config import load_credentials


def main():
    creds = load_credentials()

    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=creds.get("SNOWFLAKE_ACCOUNT"),
        user=creds.get("SNOWFLAKE_USER"),
        password=creds.get("SNOWFLAKE_PASSWORD"),
        warehouse=creds.get("SNOWFLAKE_WAREHOUSE"),
        database=creds.get("SNOWFLAKE_DATABASE"),
    )

    cursor = conn.cursor()

    try:
        # List current schemas
        print("\nCurrent schemas:")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        for schema in schemas:
            print(f"  - {schema[1]}")

        # Drop the migration schemas (both uppercase and lowercase versions)
        schemas_to_drop = [
            "PG_MIGRATION",
            "pg_migration", 
            "ECOMMERCE",
            "ecommerce",
            "COMPANY",
            "company",
            "DEMO_USER",
            "demo_user",
        ]

        print("\nDropping migration schemas...")
        for schema_name in schemas_to_drop:
            try:
                # Use quotes for case-sensitive drop
                cursor.execute(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
                print(f"  Dropped: {schema_name}")
            except Exception as e:
                print(f"  {schema_name}: {e}")

        # Verify cleanup
        print("\nRemaining schemas:")
        cursor.execute("SHOW SCHEMAS")
        schemas = cursor.fetchall()
        for schema in schemas:
            print(f"  - {schema[1]}")

        print("\nCleanup complete!")

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    main()

