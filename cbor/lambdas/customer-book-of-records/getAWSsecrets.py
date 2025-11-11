import boto3
import os

# -------------------------------
# ✅ Custom error messages for validation
# -------------------------------
custom_errors = {
    'INVALID_ARN': 'Expected the first parameter to be an ARN of a resource in Secrets Manager'
}

# -------------------------------
# ✅ Function to retrieve secret value from AWS Secrets Manager
# -------------------------------


def getSecretValue(secret_arn, region=os.getenv("AWS_REGION")):
    print("INFO: getSecretValue called")

    # Print the input parameters for debugging
    print(f"DEBUG: Received secret_arn = {secret_arn}")
    print(f"DEBUG: AWS_REGION from environment = {region}")

    # -------------------------------
    # ✅ Validate that the input is a proper Secrets Manager ARN
    # -------------------------------
    if not isinstance(secret_arn, str) or not secret_arn.startswith("arn:aws:secretsmanager"):
        print("ERROR: Invalid ARN format")
        raise ValueError(custom_errors['INVALID_ARN'])

    try:
        # -------------------------------
        # ✅ Create a Secrets Manager client for the specified region
        # -------------------------------
        print("DEBUG: Creating Secrets Manager client")
        client = boto3.client('secretsmanager', region_name=region)

        # -------------------------------
        # ✅ Retrieve the secret value using the ARN
        # -------------------------------
        print(f"DEBUG: Calling get_secret_value for ARN: {secret_arn}")
        response = client.get_secret_value(SecretId=secret_arn)
        print("DEBUG: Secrets Manager response received")

        # -------------------------------
        # ✅ Return the secret string if available
        # -------------------------------
        if 'SecretString' in response:
            print("INFO: SecretString found in response")
            return response['SecretString']
        else:
            print("ERROR: SecretString not found in response")
            raise ValueError("Secret value not found in response.")

    except Exception as e:
        # -------------------------------
        # ✅ Log and re-raise any exceptions encountered
        # -------------------------------
        import traceback
        traceback.print_exc()
        print("ERROR: Exception while retrieving secret:", str(e))
