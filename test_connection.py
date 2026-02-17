#!/usr/bin/env python3
"""
Teste de conexão com ClickHouse usando a classe ClickhouseSync
Este script testa a conexão com o banco de dados ClickHouse.
"""

import os
from dotenv import load_dotenv
from clickhouse_sync import ClickhouseSync

def main():
    """Testa a conexão com o ClickHouse."""
    print("=== TESTE DE CONEXÃO COM CLICKHOUSE ===")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Obtém configurações do .env
    host = os.getenv('TEST_CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_TCP_PORT', 9000))
    user = os.getenv('CLICKHOUSE_USER')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    database = os.getenv('CLICKHOUSE_DB')
    
    print(f"Host: {host}")
    print(f"Porta: {port}")
    print(f"Usuário: {user}")
    print(f"Database: {database}")
    print("-" * 50)
    
    try:
        # Inicializa a classe ClickhouseSync
        clickhouse = ClickhouseSync(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        
        # Testa a conexão
        print("Tentando estabelecer conexão...")
        clickhouse.connect()
        
        # Testa uma consulta simples
        print("Testando consulta simples...")
        clickhouse.test_connection()
        
        # Lista databases disponíveis
        print("\nListando databases disponíveis:")
        df_databases = clickhouse.execute_query_to_df("SHOW DATABASES")
        print(df_databases)
        
        # Testa algumas queries básicas
        print("\nTestando queries básicas:")
        
        # Query de sistema
        df_version = clickhouse.execute_query_to_df("SELECT version() as versao")
        print(f"Versão do ClickHouse: {df_version.iloc[0]['versao']}")
        
        # Query de data/hora atual
        df_now = clickhouse.execute_query_to_df("SELECT now() as data_hora_atual")
        print(f"Data/Hora atual no servidor: {df_now.iloc[0]['data_hora_atual']}")
        
        print("\n✅ CONEXÃO TESTADA COM SUCESSO!")
        print("Todos os testes de conexão passaram.")
        
    except Exception as e:
        print(f"\n❌ ERRO DURANTE O TESTE DE CONEXÃO:")
        print(f"Erro: {e}")
        print("\nVerifique:")
        print("1. Se o container do ClickHouse está rodando")
        print("2. Se as configurações no .env estão corretas")
        print("3. Se a network do Docker está configurada")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)