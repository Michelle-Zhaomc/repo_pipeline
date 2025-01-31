def excel_to_snowflake_etl(): 
    
    # Import necessary libraries
    from pyspark.sql import SparkSession
    import pandas as pd
    import os
    import requests
    from io import BytesIO
    from openpyxl import load_workbook

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Snowflake to PostgreSQL") \
        .master("local[*]") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3,com.crealytics:spark-excel_2.12:0.13.5") \
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

    def load_and_write_excel_to_snowflake(snowflake_options: dict):
        github_url = "https://github.com/Michelle-Zhaomc/repo_webapp/raw/refs/heads/main/data/fleet_service_data.xlsx"

        # Step 1: Download the Excel file
        response = requests.get(github_url)
        if response.status_code == 200:
            print("File downloaded successfully!")
        else:
            raise Exception(f"Failed to download file from GitHub. Status code: {response.status_code}")

        # Step 2: Read the Excel file using Pandas (strip styles)
        excel_file = BytesIO(response.content)
        wb = load_workbook(excel_file, data_only=True)  # Ignore styles by setting `data_only=True`

        # Step 3: Save the cleaned file
        cleaned_file_path = "c:\cleaned_fleet_service_data.xlsx"
        wb.save(cleaned_file_path)
        print(f"Saved cleaned Excel file: {cleaned_file_path}")

        # Step 4: Use cleaned file in Pandas
        df = pd.read_excel(cleaned_file_path, engine="openpyxl")
        print(df.head())  # Verify it loads correctly

        # Step 3: Load cleaned file into Spark
        spark_df = spark.read.format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .option("dataAddress", "'Sheet1'!A1") \
            .option("maxRowsInMemory", 20000) \
            .load(cleaned_file_path)  # ✅ Load the cleaned file

        # Rename columns (replace spaces with underscores)
        for col in spark_df.columns:
            spark_df = spark_df.withColumnRenamed(col, col.replace(' ', '_'))

        # Show loaded DataFrame
        print(f"Loaded data with {spark_df.count()} rows")
        spark_df.show()

        # Define Snowflake table name
        table_name = "fleet_service_data"

        # Write data to Snowflake
        spark_df.write \
            .format("snowflake") \
            .options(**snowflake_options) \
            .option("dbtable", table_name) \
            .mode("overwrite") \
            .save()

        print(f"Data successfully written to Snowflake table: {table_name}")

    # Run the function
    load_and_write_excel_to_snowflake(snowflake_options)


    # Load and write the AdventureWorks data from an Excel file to Snowflake
    # excel_file_path = excel_path  # Use raw string
    load_and_write_excel_to_snowflake(snowflake_options)

    spark.stop()


# excel_to_snowflake_etl()

