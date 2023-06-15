import boto3

# Configuración de AWS
region_name = 'us-west-2'  # Reemplaza con tu región de AWS
output_bucket = 'nombre-del-bucket'  # Reemplaza con el nombre de tu bucket de salida en S3

# Crea una sesión de AWS
session = boto3.Session(region_name=region_name)

# Crea el cliente de AWS Athena
athena_client = session.client('athena')

# Configura la consulta
database = 'nombre-de-la-base-de-datos'  # Reemplaza con el nombre de tu base de datos en Athena
query_string = 'SELECT * FROM nombre-de-la-tabla LIMIT 10'  # Reemplaza con tu consulta SQL

# Ejecuta la consulta
response = athena_client.start_query_execution(
    QueryString=query_string,
    QueryExecutionContext={
        'Database': database
    },
    ResultConfiguration={
        'OutputLocation': f's3://{output_bucket}/athena-output/'
    }
)

# Obtiene el ID de la ejecución de la consulta
query_execution_id = response['QueryExecutionId']

# Espera hasta que la consulta termine de ejecutarse
athena_client.get_waiter('query_execution_complete').wait(
    QueryExecutionId=query_execution_id
)

# Obtiene los resultados de la consulta
result_response = athena_client.get_query_results(
    QueryExecutionId=query_execution_id
)

# Extrae los datos de los resultados
column_names = [col['Name'] for col in result_response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
rows = [tuple(data['VarCharValue'] for data in row['Data']) for row in result_response['ResultSet']['Rows'][1:]]

# Imprime los nombres de las columnas
print(column_names)

# Imprime las filas de datos
for row in rows:
    print(row)
