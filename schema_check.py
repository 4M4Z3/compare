from google.cloud import bigquery
from google.oauth2 import service_account

def check_schema():
    """Check the schema of the GenCast BigQuery table."""
    try:
        # Initialize credentials
        credentials = service_account.Credentials.from_service_account_file(
            'service_acct.json',
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        
        # Create BigQuery client
        client = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )
        
        # Get table reference
        table_ref = client.dataset('weathernext_gen_forecasts', 'ultra-task-456813-d5').table('126478713_1_0')
        
        # Get table
        table = client.get_table(table_ref)
        
        # Print schema information
        print("\nTable Schema:")
        for field in table.schema:
            print(f"- {field.name} ({field.field_type})")
            
        # Get sample row
        query = """
        SELECT *
        FROM `ultra-task-456813-d5.weathernext_gen_forecasts.126478713_1_0`
        LIMIT 1
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        print("\nSample Row:")
        for row in results:
            for key, value in row.items():
                print(f"- {key}: {value}")
            
    except Exception as e:
        print(f"Error checking schema: {str(e)}")

if __name__ == "__main__":
    check_schema() 