def excel_to_snowflake_etl():
    # Import necessary libraries
    from pyspark.sql import SparkSession
    import pandas as pd
    import os
    from io import BytesIO

    # Create a Spark session
    spark = SparkSession.builder \
        .appName("Excel to Snowflake ETL") \
        .master("local[*]") \
        .config("spark.jars.packages", "net.snowflake:spark-snowflake_2.12:2.10.0-spark_3.2,net.snowflake:snowflake-jdbc:3.13.3,com.crealytics:spark-excel_2.12:0.13.5") \
        .config("spark.driver.memory", "4g") \
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

    # Define local Excel file path
    local_file_path = r"C:\Users\13693\Documents\MyApp\webapp_demo\Promotion_data.xlsx"

    # Read the Excel file using Pandas
    excel_file = pd.ExcelFile(local_file_path)
    sheet_names = excel_file.sheet_names

    # Handle the case where there's only one sheet
    if len(sheet_names) == 1:
        sheet_name = sheet_names[0]
        print(f"Single sheet found: {sheet_name}")
    else:
        raise Exception("Multiple sheets detected; only one sheet is supported in this implementation.")

    # Read the Excel sheet into a Spark DataFrame
    print(f"Loading sheet: {sheet_name}")
    spark_df = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dataAddress", f"{sheet_name}!A1") \
        .option("maxRowsInMemory", 20000) \
        .load(local_file_path)

    # Clean column names
    for col in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(col, col.replace(' ', '_'))

    # Show DataFrame
    print(f"Loaded sheet '{sheet_name}' with {spark_df.count()} rows.")
    spark_df.show()

    # Define Snowflake table name based on sheet name
    table_name = sheet_name.replace(" ", "_")

    # Write data to Snowflake
    spark_df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table_name) \
        .mode("overwrite") \
        .save()

    print(f"Data successfully written to Snowflake table '{table_name}'.")

    # Stop Spark session
    spark.stop()


# Call the ETL function
excel_to_snowflake_etl()
