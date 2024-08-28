# Databricks - MockarooAPI Integration

Este projeto demonstra a integração entre o Databricks e a API do Mockaroo para a geração de dados fictícios e ingestão desses dados em um Data Lake utilizando a arquitetura Medallion.


## Estrutura do Projeto
O projeto está organizado da seguinte forma:

```css
Databricks---MockarooAPI/
│
├── src/
|   |──raw/
|   |  ├── main.py
|   |  ├── fields.json
|   |
│   ├── bronze/
│   │   ├── ingestao.py
│   │   
│   ├── silverETL/
│   │   └── ingestao.py
|   |   └── query´s.sql
│   └── gold/
│   |    └── ingestion.py
|   |    └── query´s.sql
|   |
│   └──workflows/
|   |  └── main.py
|   |  └── Mockaroo-API.json
|   |
├── lib/
│   ├── connection.py
│   └── ETL.py
|   └── ingestors.py
|   └── mockaroo_api.py
|   └── pipeline.py
|   └── utils.py
│
├── .env
├── .gitignore
├── README.md
└── LICENSE
```

## Configuração

#### Pré-requisito:

    Certifique-se de ter as seguintes ferramentas instaladas:

  *  Databricks
  *  Python 3.8+


### Instalação:

##### Clone o repositório:
```bash
    git clone https://github.com/ViniciusOtoni/Databricks---MockarooAPI.git
    cd Databricks---MockarooAPI
```

##### Crie um ambiente virtual e instale as dependências:

```bash
    python -m venv venv
    source venv/Scripts/activate
    pip install -r requirements.txt
```
#####  Configure as variáveis de ambiente:
    Crie um arquivo .env na raiz do projeto e defina as seguintes variáveis:

```env
    MOCKAROO_API_KEY=
    DATABRICKS_TOKEN=
    DATABRICKS_HOST=
```
    MOCKAROO_API_KEY: Chave da API do Mockaroo.
    DATABRICKS_TOKEN: Token de autenticação para a API do Databricks.
    DATABRICKS_HOST: URL do seu workspace no Databricks.

### Utilização das Variáveis de Ambiente

As variáveis de ambiente são usadas para configurar as credenciais e endpoints necessários para as integrações com as APIs do Mockaroo e do Databricks. O arquivo .env é lido automaticamente pelo projeto, garantindo que informações sensíveis não fiquem expostas no código-fonte.

Para garantir que as variáveis de ambiente sejam carregadas corretamente, o projeto utiliza a biblioteca dotenv. Certifique-se de que todas as variáveis necessárias estejam definidas no arquivo .env antes de executar os scripts.

### Execução:

##### Definindo o Schema:

Primeiro, defina a estrutura dos dados a serem gerados pela API do Mockaroo no arquivo fields.json. Aqui está um exemplo de como o arquivo pode ser estruturado:
```json
    [
  {
    "name": "name",
    "type": "First Name"
  },
  {
    "name": "category",
    "type": "Custom List",
    "values": ["Action", "Adventure", "Strategy"]
  },
  {
    "name": "rating",
    "type": "Number",
    "min": 1,
    "max": 10
  }
]
```

##### Ingestão de dados para o Azure:

Execute o script main.py para gerar os dados e realizar a ingestão na camada bronze do Data Lake:
```bash
    python src/raw/main.py
```

##### Ingestão de dados para proxíma camada:
Execute o script ingestion.py para transformar os dados e passar para a próxima camada do Data Lake: (deve passar os parâmetros necessários nos Job´s para uma execução bem sucedida).

```bash
    python src/camada/ingestion.py
```


### Orquestração do Workflow:

No databricks realizei a cópia do arquivo JSON contendo as informações dos JOB´s e realize a orquestração de forma local:
```bash
    python src/workflows/main.py
```

Exemplo de arquivo JSON:

```json
{
  "job_id": 123,
  "new_settings": {
    "name": "Exemplo-API",
    "email_notifications": {
      "no_alert_for_skipped_runs": false
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
      {
        "task_key": "JobExemplo",
        "run_if": "ALL_SUCCESS",
        "notebook_task": {
          "notebook_path": "path",
          "base_parameters": {
            "param01": "valor01",
            "param02": "valor02",
            "param03": "valor03"
          },
          "source": "GIT"
        },
        "existing_cluster_id": "cluster-id",
        "timeout_seconds": 0,
        "email_notifications": {},
        "notification_settings": {
          "no_alert_for_skipped_runs": false,
          "no_alert_for_canceled_runs": false,
          "alert_on_last_attempt": false
        },
        "webhook_notifications": {}
    }],
    "git_source": {
      "git_url": "https://github.com/ViniciusOtoni/Databricks---MockarooAPI.git",
      "git_provider": "gitHub",
      "git_branch": "branch"
    },
    "queue": {
      "enabled": true
    },
    "run_as": {
      "user_name": "usuarioDatabricks"
    }
  }
}
```



