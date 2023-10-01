import requests

# URL da API do ResourceManager do YARN
resource_manager_url = "http://<seu-resource-manager>:8088/ws/v1/cluster/apps"

# Faça uma chamada GET para obter informações sobre os aplicativos em execução
response = requests.get(resource_manager_url)

# Verifique se a solicitação foi bem-sucedida
if response.status_code == 200:
    # A resposta é em formato JSON
    data = response.json()
    
    # Itere sobre os aplicativos e imprima as informações desejadas
    for app in data['apps']['app']:
        print("ID do Aplicativo:", app['id'])
        print("Nome do Aplicativo:", app['name'])
        print("Estado do Aplicativo:", app['state'])
        print("Data e Hora de Início:", app['startedTime'])
        print("Usuário:", app['user'])
        print("\n")
else:
    print("Erro ao buscar informações do ResourceManager")

