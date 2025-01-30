def csv_to_snowflake_etl():
    # Import necessary libraries
    from pyspark.sql import SparkSession
    import pandas as pd
    import os
    import requests
    from io import StringIO
    import tempfile

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("CSV to Snowflake ETL") \
        .master("local[*]") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.executor.cores", "2") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.network.timeout", "300s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Define Snowflake options
    snowflake_options = {
        "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.getenv('SNOWFLAKE_USER'),
        "sfPassword": os.getenv('SNOWFLAKE_PASSWORD'),
        "sfDatabase": os.getenv('SNOWFLAKE_DATABASE'),
        "sfSchema": os.getenv('SNOWFLAKE_SCHEMA'),
        "sfRole": os.getenv('SNOWFLAKE_ROLE')
    }

    # Function to load a CSV file and write it to Snowflake
    def load_and_write_csv_to_snowflake(snowflake_options: dict):
        github_url = "https://github.com/Michelle-Zhaomc/repo_webapp/raw/refs/heads/main/data/fleet_service_data.csv"  # Replace with the actual raw URL

        # Step 1: Download the CSV file from GitHub
        response = requests.get(github_url)
        if response.status_code == 200:
            print("File downloaded successfully!")
        else:
            raise Exception(f"Failed to download file from GitHub. Status code: {response.status_code}")

        # Step 2: Save the CSV content to a temporary file
        csv_content = response.content.decode('utf-8')
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_csv_file:
            temp_csv_file.write(csv_content.encode('utf-8'))
            temp_csv_path = temp_csv_file.name

        # Step 3: Read the CSV file into a Spark DataFrame directly
        spark_df = spark.read.csv(temp_csv_path, header=True, inferSchema=True)

        # Rename columns to replace spaces with underscores
        for col in spark_df.columns:
            spark_df = spark_df.withColumnRenamed(col, col.replace(' ', '_'))

        # Define Snowflake table name
        table_name = "fleet_service_data"  # You can customize this as needed
        spark_df.show()
        print(snowflake_options, table_name)

        # Write data to Snowflake
        spark_df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", table_name) \
            .mode("overwrite") \
            .save()

        print(f"Data written to Snowflake table '{table_name}'")

    # Load and write the CSV data to Snowflake
    load_and_write_csv_to_snowflake(snowflake_options)

    spark.stop()

# from dotenv import load_dotenv
# load_dotenv()
# csv_to_snowflake_etl()