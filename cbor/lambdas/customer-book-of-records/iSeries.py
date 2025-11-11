import jaydebeapi
import jpype
import jpype.imports
import os
import json
from jpype.types import *
import getAWSsecrets


def execute_query(query):
    """
    Connects to MySQL using JDBC (jaydebeapi), executes the provided SQL query,
    and returns the results as a list of dictionaries.
    """

    print("DEBUG: Starting execute_query")

    # Secret name (MySQL credentials ARN from environment variable)
    mysql_secret_arn = os.getenv("MYSQL_SECRET_ARN")
    print(f"DEBUG: Environment variable MYSQL_SECRET_ARN = {mysql_secret_arn}")

    # Retrieve MySQL credentials from Secrets Manager
    secret_data = getAWSsecrets.getSecretValue(mysql_secret_arn)
    creds = json.loads(secret_data)
    print(f"DEBUG: MySQL credentials retrieved successfully")

    # JDBC driver class and connection URL
    driver = "com.mysql.cj.jdbc.Driver"
    url = f"jdbc:mysql://{creds['host']}:{creds['port']}/{creds['database']}"
    username = creds["user"]
    password = creds["password"]

    # Path to MySQL JDBC driver .jar
    jar_path = "/opt/python/mysql-connector-j-8.3.0.jar"
    jvm_path = "/opt/python/lib/server/libjvm.so"

    print(
        f"DEBUG: Checking JAR path exists: {jar_path} -> {os.path.exists(jar_path)}")
    print(
        f"DEBUG: Checking JVM path exists: {jvm_path} -> {os.path.exists(jvm_path)}")

    print("START: Connecting to MySQL")

    # Start JVM if not already started
    if not jpype.isJVMStarted():
        print("DEBUG: Starting JVM...")
        jpype.startJVM(jvmpath=jvm_path, classpath=[jar_path])
        jpype.java.lang.System.setProperty("java.awt.headless", "true")
        print("DEBUG: JVM started")

    # Establish JDBC connection
    try:
        print("DEBUG: Attempting JDBC connection...")
        conn = jaydebeapi.connect(driver, url, [username, password], jar_path)
        cursor = conn.cursor()
        print("END: Connection completed successfully")
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("ERROR: JDBC connection failed:", str(e))
        return []

    try:
        # Execute query
        print(f"DEBUG: Executing query:\n{query}")
        cursor.execute(query)

        # Fetch column headers
        headers = [str(col_desc[0]) for col_desc in cursor.description]
        print(f"DEBUG: Retrieved headers: {headers}")

        # Fetch all rows
        rows = cursor.fetchall()
        print(f"DEBUG: Retrieved {len(rows)} rows")

        # Convert results to list of dicts
        formatted_results = []
        for row in rows:
            row_dict = {}
            for header, value in zip(headers, row):
                row_dict[header] = value
            formatted_results.append(row_dict)

        print("Query executed successfully. Results:")
        for row in formatted_results:
            print(json.dumps(row, indent=2))

        return formatted_results

    except Exception as e:
        import traceback
        traceback.print_exc()
        print("ERROR: Exception during query execution:", str(e))
        return []

    finally:
        print("DEBUG: Closing JDBC resources")
        cursor.close()
        conn.close()
