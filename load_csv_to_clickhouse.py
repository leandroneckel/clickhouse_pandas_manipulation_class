#!/usr/bin/env python3
"""
Carregamento de dados CSV para ClickHouse
Este script lê um arquivo CSV com pandas, cria database e tabela, e insere os dados.
"""

import os
import pandas as pd
from dotenv import load_dotenv
from clickhouse_sync import ClickhouseSync

def main():
    """Carrega dados do CSV para o ClickHouse."""
    print("=== CARREGAMENTO DE DADOS CSV PARA CLICKHOUSE ===")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Configurações do banco
    host = os.getenv('TEST_CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_TCP_PORT', 9000))
    user = os.getenv('CLICKHOUSE_USER')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    database = os.getenv('CLICKHOUSE_DB')
    
    # Configurações dos dados
    csv_file = "clientes_fake.csv"
    db_name = "exemplo_db"
    table_name = "clientes"
    
    print(f"Arquivo CSV: {csv_file}")
    print(f"Database: {db_name}")
    print(f"Tabela: {table_name}")
    print("-" * 50)
    
    try:
        # 1. Carrega o CSV com pandas
        print("1. Carregando arquivo CSV...")
        if not os.path.exists(csv_file):
            print(f"❌ Arquivo {csv_file} não encontrado!")
            print("Execute o script gerar_dados_exemplo.py primeiro.")
            return 1
        
        # Lê o CSV (separado por ponto e vírgula conforme o arquivo de exemplo)
        df = pd.read_csv(csv_file, sep=';', encoding='utf-8-sig')
        print(f"✅ CSV carregado: {len(df)} registros, {len(df.columns)} colunas")
        print(f"Colunas: {list(df.columns)}")
        
        # Exibe informações sobre o DataFrame
        print("\nInformações do DataFrame:")
        print(df.info())
        
        print("\nPrimeiras 3 linhas:")
        print(df.head(3))
        
        # 2. Conecta ao ClickHouse
        print("\n2. Conectando ao ClickHouse...")
        clickhouse = ClickhouseSync(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        
        clickhouse.connect()
        print("✅ Conexão estabelecida com sucesso!")
        
        # 3. Cria o database
        print(f"\n3. Criando database '{db_name}'...")
        clickhouse.create_database_if_not_exists(db_name)
        print(f"✅ Database '{db_name}' criado ou já existe")
        
        # 4. Cria a tabela a partir do DataFrame
        print(f"\n4. Criando tabela '{table_name}' a partir do DataFrame...")
        
        # Define colunas de data que devem ser tratadas como DateTime nullable
        datetime_cols = ['data_nascimento', 'data_cadastro']
        
        clickhouse.create_table_from_df(
            db_name=db_name,
            table_name=table_name,
            df=df,
            datetime_nullable_cols=datetime_cols
        )
        print(f"✅ Tabela '{table_name}' criada com sucesso!")
        
        # 5. Insere os dados
        print(f"\n5. Inserindo {len(df)} registros na tabela...")
        clickhouse.insert_df_in_batches_v3(
            db_name=db_name,
            table_name=table_name,
            df=df,
            batch_size=1000,  # Processamento em lotes de 1000 registros
            datetime_strfmt="%Y-%m-%d %H:%M:%S"
        )
        print(f"✅ {len(df)} registros inseridos com sucesso!")
        
        # 6. Verifica os dados inseridos
        print(f"\n6. Verificando dados inseridos...")
        count_query = f"SELECT COUNT(*) as total FROM {db_name}.{table_name}"
        df_count = clickhouse.execute_query_to_df(count_query)
        total_records = df_count.iloc[0]['total']
        print(f"✅ Total de registros na tabela: {total_records}")
        
        # Exibe algumas estatísticas
        stats_query = f"""
        SELECT 
            COUNT(*) as total_clientes,
            COUNT(DISTINCT sexo) as sexos_distintos,
            MIN(data_nascimento) as nascimento_mais_antigo,
            MAX(data_nascimento) as nascimento_mais_recente,
            AVG(renda_mensal) as renda_media,
            countIf(ativo = 1) as clientes_ativos
        FROM {db_name}.{table_name}
        """
        df_stats = clickhouse.execute_query_to_df(stats_query)
        
        print("\nEstatísticas dos dados:")
        print(f"Total de clientes: {df_stats.iloc[0]['total_clientes']}")
        print(f"Sexos distintos: {df_stats.iloc[0]['sexos_distintos']}")
        print(f"Nascimento mais antigo: {df_stats.iloc[0]['nascimento_mais_antigo']}")
        print(f"Nascimento mais recente: {df_stats.iloc[0]['nascimento_mais_recente']}")
        print(f"Renda média: R$ {df_stats.iloc[0]['renda_media']:.2f}")
        print(f"Clientes ativos: {df_stats.iloc[0]['clientes_ativos']}")
        
        print("\n🎉 CARREGAMENTO CONCLUÍDO COM SUCESSO!")
        print(f"Dados do arquivo '{csv_file}' foram carregados na tabela '{db_name}.{table_name}'")
        
    except Exception as e:
        print(f"\n❌ ERRO DURANTE O CARREGAMENTO:")
        print(f"Erro: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)