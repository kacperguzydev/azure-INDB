import logging
import os
from azure.functions.decorators import FunctionApp
import azure.functions as func
from etl import process_imdb_data  # Ensure etl.py is in the same folder or update the import path accordingly

# Import the Cosmos DB client and helper
from azure.cosmos import CosmosClient, PartitionKey

app = FunctionApp()

# Set your Cosmos DB Emulator endpoint and primary key
COSMOS_ENDPOINT = "https://localhost:8081/"
COSMOS_KEY = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="  # Replace with your actual key
DATABASE_NAME = "IMDBDatabase"
CONTAINER_NAME = "Movies"


def upsert_data_into_cosmos(df):
    # Create a Cosmos DB client
    client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    # Create database if it does not exist
    database = client.create_database_if_not_exists(id=DATABASE_NAME)
    # Create container if it does not exist, using "/id" as the partition key
    container = database.create_container_if_not_exists(
        id=CONTAINER_NAME,
        partition_key=PartitionKey(path="/id"),
        offer_throughput=400
    )

    # Iterate over the DataFrame rows and insert each as a document
    inserted_count = 0
    for _, row in df.iterrows():
        document = row.to_dict()
        container.upsert_item(document)
        inserted_count += 1
    return inserted_count


@app.function_name(name="IngestIMDBData")
@app.route(route="IngestIMDBData", methods=["GET", "POST"], auth_level=func.AuthLevel.ANONYMOUS)
def ingest_imdb_data(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("IngestIMDBData function triggered.")

    try:
        # Path to your CSV file
        file_path = "data/IMDB Top 250 Movies.csv"
        # Process the CSV using your ETL script
        df_processed = process_imdb_data(file_path)
        # Insert processed data into Cosmos DB
        count = upsert_data_into_cosmos(df_processed)
        # Optionally, return the first 5 rows as JSON along with a message
        result_json = df_processed.head().to_json(orient="records")
        message = f"Inserted {count} records into Cosmos DB.\nFirst 5 records: {result_json}"
        return func.HttpResponse(message, status_code=200, mimetype="application/json")
    except Exception as e:
        logging.error(f"Error processing data: {e}")
        return func.HttpResponse("Failed to process data", status_code=500)
