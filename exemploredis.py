import boto3

def get_cache_value(cluster_endpoint, key):
    try:
        # Conectar ao cluster do ElastiCache
        client = boto3.client('elasticache')
        
        # Consultar o valor associado à chave no cluster
        response = client.get_item(
            Endpoint=cluster_endpoint,
            Key={
                'key': {'S': key}
            }
        )
        
        # Verificar se a resposta contém dados
        if 'Item' in response:
            value = response['Item']['value']['S']
            return value
        else:
            return None
    
    except Exception as e:
        print(f"Erro ao consultar a chave '{key}': {e}")
        return None

# Substitua 'nome-do-cluster' pelo nome do cluster ElastiCache
cluster_endpoint = 'nome-do-cluster.abcxyz.clustercfg.use1.cache.amazonaws.com'
# Substitua 'chave-desejada' pela chave que você quer consultar
key_to_query = 'chave-desejada'

# Consultar o valor associado à chave no cluster do ElastiCache
result = get_cache_value(cluster_endpoint, key_to_query)
if result is not None:
    print(f"Valor associado à chave '{key_to_query}': {result}")
else:
    print(f"Chave '{key_to_query}' não encontrada.")
