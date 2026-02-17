# 📊 ClickHouse Sync - Sistema de Integração e Análise de Dados

Este projeto é uma solução completa para integração e análise de dados usando **ClickHouse** em containers Docker, com uma classe Python customizada para gerenciamento de dados, ETL e execução de queries analíticas.

## 🎯 Objetivo do Projeto

O **ClickHouse Sync** foi desenvolvido para:
- ✅ Facilitar a integração de dados CSV com o banco de dados ClickHouse
- ✅ Fornecer uma interface Python robusta para operações de ETL
- ✅ Executar análises de dados em larga escala com alta performance
- ✅ Automatizar a criação e gerenciamento de databases e tabelas
- ✅ Demonstrar as capacidades analíticas do ClickHouse com dados reais

## 🏗️ Arquitetura do Sistema

```
┌─────────────────────────────────────────────────────────────┐
│                   CLICKHOUSE SYNC PROJECT                   │
├─────────────────────────────────────────────────────────────┤
│  📁 _docker/                                               │
│     ├── df_clickhouse          # Dockerfile ClickHouse     │
│     ├── cmd_build              # Script de build           │
│     └── cmd_network            # Script de network         │
│                                                            │
│  🐍 Python Scripts/                                        │
│     ├── clickhouse_sync.py     # Classe principal         │
│     ├── test_connection.py     # Teste de conexão         │
│     ├── load_csv_to_clickhouse.py # ETL de dados          │
│     └── test_queries.py        # Queries analíticas       │
│                                                            │
│  📊 Data Files/                                            │
│     └── clientes_fake.csv      # Dataset de exemplo       │
│                                                            │
│  ⚙️ Configuration/                                          │
│     ├── .env                   # Variáveis ambiente       │
│     ├── docker-compose.yml     # Orquestração containers  │
│     └── requirements.txt       # Dependências Python      │
└─────────────────────────────────────────────────────────────┘
```

## 📋 Pré-requisitos

### Sistema
- **Docker** 20.10+ e **Docker Compose** 2.0+
- **Python** 3.8+
- **pip** para instalação de pacotes Python

### Hardware Recomendado
- **RAM**: Mínimo 4GB (recomendado 8GB+)
- **Storage**: 10GB+ de espaço livre
- **CPU**: 2+ cores

## 📁 Estrutura do Projeto

```
ClickhouseSyncGit/
├── _docker/                    # 🐳 Configurações Docker
│   ├── df_clickhouse          # Dockerfile do ClickHouse
│   ├── cmd_build              # Script para build da imagem
│   └── cmd_network            # Script para criar network
│
├── 📄 Configuração Principal
│   ├── .env                   # Variáveis de ambiente
│   ├── pj_clickhouse_compose.yml # Configuração Docker Compose
│   └── requirements.txt       # Dependências Python
│
├── 🐍 Scripts Python
│   ├── clickhouse_sync.py     # Classe principal ClickHouseSync
│   ├── test_connection.py     # Testa conexão com banco
│   ├── load_csv_to_clickhouse.py # Carrega dados CSV
│   └── test_queries.py        # Executa queries analíticas
│
└── 📊 Dados
    └── clientes_fake.csv       # Dataset com 200 registros de clientes
```

## 🚀 Guia de Instalação e Execução

### Passo 1: Preparação do Ambiente

#### 1.1 Clone o repositório
```bash
git clone <seu-repositorio>
cd ClickhouseSyncGit
```

#### 1.2 Configure o arquivo .env
Crie ou ajuste o arquivo `.env` com suas configurações:

```env
# Image
CLICKHOUSE_IMAGE=img_clickhouse

# Container
CLICKHOUSE_CONTAINER_NAME=exemple_clickhouse
CLICKHOUSE_HOSTNAME=exemple_clickhouse

# Ports
CLICKHOUSE_HTTP_PORT=8123
CLICKHOUSE_TCP_PORT=9000

# Credentials
CLICKHOUSE_USER=clickhouse_admin
CLICKHOUSE_PASSWORD=JmQ2ktJIu68SaU77Ojml
CLICKHOUSE_DB=example_db

# Logging
LOG_MAX_SIZE=100m
LOG_MAX_FILE=5

TEST_CLICKHOUSE_HOST=localhost
```

### Passo 2: Configuração do Docker

#### 2.1 Crie a network Docker
```bash
# Windows
cd _docker
.\cmd_network

# Linux/Mac
bash _docker/cmd_network
```

#### 2.2 Build da imagem ClickHouse
```bash
# Windows
cd _docker
.\cmd_build

# Linux/Mac
bash _docker/cmd_build
```

#### 2.3 Inicie o container
```bash
# Volte para o diretório raiz
cd ..

# Inicie o container
docker-compose -f pj_clickhouse_compose.yml up -d
```

#### 2.4 Verifique o container
```bash
docker ps
docker-compose -f pj_clickhouse_compose.yml logs
```

### Passo 3: Configuração do Python

#### 3.1 Instale as dependências
```bash
pip install -r requirements.txt
```

**Dependências incluídas:**
- `clickhouse-driver==0.2.10` - Driver oficial ClickHouse
- `pandas==3.0.0` - Manipulação de dados
- `numpy==2.4.2` - Operações numéricas
- `python-dotenv==1.0.0` - Gerenciamento de variáveis ambiente
- `Faker==40.4.0` - Geração de dados fake

### Passo 4: Execução dos Scripts

#### 4.1 Teste a conexão
```bash
python test_connection.py
```

**Saída esperada:**
```
=== TESTE DE CONEXÃO COM CLICKHOUSE ===
Host: localhost
Porta: 9000
Usuário: clickhouse_admin
Database: example_db
--------------------------------------------------
Tentando estabelecer conexão...
Conexão estabelecida com sucesso!
Testando consulta simples...
Versão do ClickHouse: 24.8.x.x

Listando databases disponíveis:
      name
0  default
1  example_db
2  system

Testando queries básicas:
Versão do ClickHouse: 24.8.x.x
Data/Hora atual no servidor: 2026-02-17 15:30:45

✅ CONEXÃO TESTADA COM SUCESSO!
```

#### 4.2 Carregue os dados CSV
```bash
python load_csv_to_clickhouse.py
```

**Processo executado:**
1. 📁 Carrega o arquivo `clientes_fake.csv` (200 registros)
2. 🔌 Conecta ao ClickHouse
3. 🗄️ Cria database `exemplo_db`
4. 📋 Cria tabela `clientes` com schema automático
5. 📊 Insere dados em lotes de 1000 registros
6. ✅ Valida inserção e exibe estatísticas

**Saída esperada:**
```
=== CARREGAMENTO DE DADOS CSV PARA CLICKHOUSE ===
Arquivo CSV: clientes_fake.csv
Database: exemplo_db
Tabela: clientes
--------------------------------------------------
1. Carregando arquivo CSV...
✅ CSV carregado: 200 registros, 14 colunas
Colunas: ['id_cliente', 'nome', 'sexo', 'cpf', ...]

2. Conectando ao ClickHouse...
✅ Conexão estabelecida com sucesso!

3. Criando database 'exemplo_db'...
✅ Database 'exemplo_db' criado ou já existe

4. Criando tabela 'clientes' a partir do DataFrame...
✅ Tabela 'clientes' criada com sucesso!

5. Inserindo 200 registros na tabela...
Lote 1 inserido com sucesso.
✅ 200 registros inseridos com sucesso!

6. Verificando dados inseridos...
✅ Total de registros na tabela: 200

Estatísticas dos dados:
Total de clientes: 200
Sexos distintos: 2
Nascimento mais antigo: 1945-05-10
Nascimento mais recente: 2006-11-17
Renda média: R$ 12847.32
Clientes ativos: 102

🎉 CARREGAMENTO CONCLUÍDO COM SUCESSO!
```

#### 4.3 Execute análises de dados
```bash
python test_queries.py
```

**Análises executadas:**
- 📊 Contagem total de registros
- 👥 Distribuição por sexo
- 🗺️ Top 10 estados com mais clientes
- 📈 Estatísticas por faixa etária
- ✅ Clientes ativos vs inativos
- 📅 Cadastros por ano
- 💰 Top 10 clientes por renda
- 📧 Domínios de email mais comuns
- 📋 Resumo geral dos dados

## 🔧 Funcionalidades da Classe ClickHouseSync

### Gerenciamento de Conexão
```python
clickhouse = ClickhouseSync(host, port, user, password, database)
clickhouse.connect()
clickhouse.test_connection()
```

### Operações de Database
```python
clickhouse.create_database_if_not_exists("meu_db")
clickhouse.drop_database("db_temporario")
```

### Operações de Tabela
```python
# Criação automática a partir do DataFrame
clickhouse.create_table_from_df(db_name, table_name, df, datetime_nullable_cols)

# Inserção de dados em lotes
clickhouse.insert_df_in_batches_v3(db_name, table_name, df, batch_size=1000)
```

### Execução de Queries
```python
# Retorna DataFrame pandas
df_resultado = clickhouse.execute_query_to_df("SELECT * FROM tabela")

# Execução de comandos
clickhouse.execute_command("OPTIMIZE TABLE tabela FINAL")
```

### Recursos Avançados
- ✅ **Sanitização automática** de tipos de dados
- ✅ **Tratamento de valores nulos** (NaN/NaT/None)
- ✅ **Conversão inteligente** de tipos pandas → ClickHouse
- ✅ **Inserção em lotes** para performance otimizada
- ✅ **Criação automática** de schemas baseados em DataFrames

## 🎯 Casos de Uso

### 1. ETL de Dados
```python
# Carrega dados de múltiplas fontes
df_vendas = pd.read_csv("vendas.csv")
df_produtos = pd.read_excel("produtos.xlsx")

# Processa e combina dados
df_final = process_data(df_vendas, df_produtos)

# Carrega no ClickHouse
clickhouse.create_table_from_df("analytics", "vendas_produtos", df_final)
clickhouse.insert_df_in_batches_v3("analytics", "vendas_produtos", df_final)
```

### 2. Análise de Dados em Tempo Real
```python
# Query analítica complexa
query = """
SELECT 
    toMonth(data_venda) as mes,
    categoria,
    SUM(valor) as receita_total,
    COUNT(*) as qtd_vendas,
    AVG(valor) as ticket_medio
FROM analytics.vendas_produtos
WHERE data_venda >= today() - INTERVAL 30 DAY
GROUP BY mes, categoria
ORDER BY receita_total DESC
"""

df_resultado = clickhouse.execute_query_to_df(query)
```

### 3. Migração de Dados
```python
# Migra dados de PostgreSQL para ClickHouse
df_legacy = pd.read_sql("SELECT * FROM tabela_antiga", conexao_postgres)
clickhouse.create_table_from_df("novo_db", "tabela_migrada", df_legacy)
clickhouse.insert_df_in_batches_v3("novo_db", "tabela_migrada", df_legacy)
```

## 🌐 Interface Web do ClickHouse

Acesse a interface web para executar queries manualmente:
```
http://localhost:8123/play
```

**Credenciais:**
- **Usuário**: `clickhouse_admin`
- **Senha**: `JmQ2ktJIu68SaU77Ojml`

## 📊 Dataset de Exemplo

O projeto inclui um dataset com **200 registros de clientes** contendo:

| Campo | Tipo | Descrição |
|-------|------|-----------|
| `id_cliente` | Int32 | ID único do cliente |
| `nome` | String | Nome completo |
| `sexo` | String | M/F |
| `cpf` | String | CPF formatado |
| `data_nascimento` | Date | Data de nascimento |
| `email` | String | E-mail |
| `telefone` | String | Telefone formatado |
| `cep` | String | CEP |
| `logradouro` | String | Endereço |
| `numero` | Int32 | Número |
| `complemento` | String | Complemento |
| `bairro` | String | Bairro |
| `cidade` | String | Cidade |
| `estado` | String | Estado (sigla) |
| `pais` | String | País |
| `renda_mensal` | Float64 | Renda mensal |
| `data_cadastro` | Date | Data do cadastro |
| `ativo` | UInt8 | Status ativo (1/0) |

## 🔍 Comandos Úteis

### Docker
```bash
# Ver status dos containers
docker ps

# Ver logs em tempo real
docker-compose -f pj_clickhouse_compose.yml logs -f

# Parar containers
docker-compose -f pj_clickhouse_compose.yml down

# Reiniciar container
docker-compose -f pj_clickhouse_compose.yml restart

# Conectar ao container via terminal
docker exec -it exemple_clickhouse clickhouse-client -u clickhouse_admin --password JmQ2ktJIu68SaU77Ojml
```

### ClickHouse CLI
```sql
-- Ver databases
SHOW DATABASES;

-- Ver tabelas
SHOW TABLES FROM exemplo_db;

-- Descrever estrutura da tabela
DESCRIBE TABLE exemplo_db.clientes;

-- Estatísticas básicas
SELECT COUNT(*) FROM exemplo_db.clientes;

-- Otimizar tabela
OPTIMIZE TABLE exemplo_db.clientes FINAL;
```

## 🛠️ Solução de Problemas

### Container não inicia
```bash
# Verificar se as portas estão livres
netstat -tulpn | grep :8123
netstat -tulpn | grep :9000

# Verificar logs
docker-compose -f pj_clickhouse_compose.yml logs
```

### Erro de conexão Python
```bash
# Verificar se o container está rodando
docker ps

# Testar conectividade
telnet localhost 9000

# Verificar variáveis ambiente
cat .env
```

### Problemas de performance
```bash
# Verificar recursos do container
docker stats exemple_clickhouse

# Otimizar tabelas
docker exec -it exemple_clickhouse clickhouse-client -q "OPTIMIZE TABLE exemplo_db.clientes FINAL"
```

## 📈 Performance e Escalabilidade

### Benchmarks do Sistema
- ✅ **Inserção**: 10K+ registros/segundo
- ✅ **Queries analíticas**: Subsegundo para milhões de registros  
- ✅ **Compressão**: ~10x redução no tamanho dos dados
- ✅ **Concurrent queries**: Suporte a múltiplas conexões simultâneas

### Otimizações Implementadas
- 🚀 **Inserção em lotes** com batch_size configurável
- 🚀 **Schema automático** otimizado para ClickHouse
- 🚀 **Sanitização inteligente** de tipos de dados
- 🚀 **Gerenciamento de memória** eficiente

## 🔐 Segurança

### Configurações Aplicadas
- ✅ Usuário dedicado com senha forte
- ✅ Isolamento via Docker network
- ✅ Logs controlados e rotativos
- ✅ Variáveis ambiente seguras (.env)

### Boas Práticas
```bash
# Adicione .env ao .gitignore
echo ".env" >> .gitignore

# Use senhas fortes em produção
openssl rand -base64 32

# Limite acesso por IP se necessário
# Configure firewall adequadamente
```

## 🔄 Próximos Passos e Extensões

### Funcionalidades Futuras
- 📊 **Dashboard web** com Grafana/Streamlit
- 🔄 **ETL automatizado** com Apache Airflow
- 📡 **API REST** para consultas
- 🔔 **Monitoramento** e alertas
- 🌐 **Clustering** para alta disponibilidade

### Integrações Possíveis
- 📊 **Business Intelligence**: Metabase, Looker, Tableau
- 🔄 **ETL Tools**: Apache Airflow, Prefect, Dagster
- 📊 **Visualização**: Grafana, Plotly Dash, Streamlit
- 🗄️ **Fontes de dados**: PostgreSQL, MongoDB, APIs REST

## 📚 Referências e Documentação

### Documentação Oficial
- [ClickHouse Documentation](https://clickhouse.com/docs)
- [ClickHouse Python Driver](https://github.com/mymarilyn/clickhouse-driver)
- [Docker Compose](https://docs.docker.com/compose/)

### Recursos Adicionais
- [ClickHouse SQL Reference](https://clickhouse.com/docs/en/sql-reference/)
- [Performance Optimization](https://clickhouse.com/docs/en/operations/performance/)
- [Best Practices](https://clickhouse.com/docs/en/operations/tips/)

## 👥 Contribuição

Este projeto está aberto para contribuições! Areas de interesse:
- 🐛 Correção de bugs
- ⚡ Otimizações de performance  
- 📊 Novos tipos de análises
- 🔧 Melhorias na interface
- 📖 Documentação

## 📄 Licença

Este projeto está licenciado sob a [MIT License](LICENSE).

---

**Desenvolvido com ❤️ para demonstrar o poder do ClickHouse em análise de dados**

🚀 **Ready to analyze big data? Let's ClickHouse!** 🚀