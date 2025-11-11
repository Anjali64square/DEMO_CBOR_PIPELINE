import jaydebeapi
import jpype
import jpype.imports
import os
import json
from jpype.types import *
import boto3
import zipfile
import io
from jaydebeapi import Error
import iSeries  # ✅ Keep this reference — will handle MySQL logic later

# Initialize S3 client (retained)
s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    - Connects to MySQL (through iSeries module)
    - Executes a SQL query
    - Returns the results in JSON format for API consumption
    """
    print("DEBUG: Lambda handler invoked")
    print("Received event:", event)
    print("Received event (JSON):", json.dumps(event))

    # -------------------------------
    # ✅ Get companyContract from API, body, or environment
    # -------------------------------
    behv_id = None

    # Case 1: GET request with query parameter
    if event.get("queryStringParameters"):
        behv_id = event["queryStringParameters"].get("companyContract")
        print(f"DEBUG: companyContract from queryStringParameters = {behv_id}")

    # Case 2: POST request with JSON body
    if not behv_id and event.get("body"):
        try:
            body = json.loads(event["body"])
            behv_id = body.get("companyContract")
            print(f"DEBUG: companyContract from body = {behv_id}")
        except Exception as e:
            import traceback
            traceback.print_exc()
            print("ERROR: Failed to parse body:", str(e))

    # Case 3: Fallback to environment variable
    if not behv_id:
        behv_id = os.getenv("companyContract")
        print(f"DEBUG: companyContract from environment = {behv_id}")

    if not behv_id:
        print("ERROR: companyContract is missing")
        return {
            "statusCode": 400,
            "body": json.dumps({
                "error": "Missing 'companyContract'. Pass it as query parameter, in body, or environment variable."
            })
        }

    # -----------------------------------------
    # ✅ Define SQL Query (now simple employees query)
    # -----------------------------------------
    query = "SELECT * FROM employees"
    print(f"DEBUG: SQL query constructed:\n{query}")

    # -------------------------------
    # ✅ Execute Query and Return JSON
    # -------------------------------
    try:
        print(f"DEBUG: Executing query for companyContract = {behv_id}")
        results = iSeries.execute_query(query)  # ✅ iSeries will connect to MySQL using JayDeBeApi
        print("DEBUG: Query execution completed")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({
                "results": results,
                "original": {"companyContract": behv_id}
            })
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("ERROR: Exception in Lambda handler:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
