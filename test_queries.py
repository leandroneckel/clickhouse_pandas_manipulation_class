#!/usr/bin/env python3
"""
Teste de queries no ClickHouse
Este script executa diversas queries de teste nos dados carregados.
"""

import os
from dotenv import load_dotenv
from clickhouse_sync import ClickhouseSync

def main():
    """Executa queries de teste no ClickHouse."""
    print("=== TESTE DE QUERIES NO CLICKHOUSE ===")
    
    # Carrega variáveis de ambiente
    load_dotenv()
    
    # Configurações do banco
    host = os.getenv('TEST_CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_TCP_PORT', 9000))
    user = os.getenv('CLICKHOUSE_USER')
    password = os.getenv('CLICKHOUSE_PASSWORD')
    database = os.getenv('CLICKHOUSE_DB')
    
    # Configurações dos dados
    db_name = "exemplo_db"
    table_name = "clientes"
    
    print(f"Database: {db_name}")
    print(f"Tabela: {table_name}")
    print("-" * 50)
    
    try:
        # Conecta ao ClickHouse
        print("Conectando ao ClickHouse...")
        clickhouse = ClickhouseSync(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        
        clickhouse.connect()
        print("✅ Conexão estabelecida com sucesso!")
        
        # Verifica se a tabela existe
        print(f"\nVerificando se a tabela '{db_name}.{table_name}' existe...")
        check_table_query = f"""
        SELECT COUNT(*) as existe
        FROM system.tables 
        WHERE database = '{db_name}' AND name = '{table_name}'
        """
        df_check = clickhouse.execute_query_to_df(check_table_query)
        
        if df_check.iloc[0]['existe'] == 0:
            print(f"❌ Tabela '{db_name}.{table_name}' não existe!")
            print("Execute o script load_csv_to_clickhouse.py primeiro.")
            return 1
        
        print(f"✅ Tabela '{db_name}.{table_name}' encontrada!")
        
        # QUERY 1: Contagem total de registros
        print("\n" + "="*60)
        print("QUERY 1: Contagem total de registros")
        print("="*60)
        
        query1 = f"SELECT COUNT(*) as total_clientes FROM {db_name}.{table_name}"
        df1 = clickhouse.execute_query_to_df(query1)
        print(f"Total de clientes: {df1.iloc[0]['total_clientes']}")
        
        # QUERY 2: Distribuição por sexo
        print("\n" + "="*60)
        print("QUERY 2: Distribuição por sexo")
        print("="*60)
        
        query2 = f"""
        SELECT 
            sexo,
            COUNT(*) as quantidade,
            ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(), 2) as percentual
        FROM {db_name}.{table_name}
        GROUP BY sexo
        ORDER BY quantidade DESC
        """
        df2 = clickhouse.execute_query_to_df(query2)
        print(df2.to_string(index=False))
        
        # QUERY 3: Top 10 estados com mais clientes
        print("\n" + "="*60)
        print("QUERY 3: Top 10 estados com mais clientes")
        print("="*60)
        
        query3 = f"""
        SELECT 
            estado,
            COUNT(*) as quantidade_clientes,
            ROUND(AVG(renda_mensal), 2) as renda_media_estado
        FROM {db_name}.{table_name}
        GROUP BY estado
        ORDER BY quantidade_clientes DESC
        LIMIT 10
        """
        df3 = clickhouse.execute_query_to_df(query3)
        print(df3.to_string(index=False))
        
        # QUERY 4: Estatísticas de renda por faixa etária
        print("\n" + "="*60)
        print("QUERY 4: Estatísticas de renda por faixa etária")
        print("="*60)
        
        query4 = f"""
        SELECT 
            CASE 
                WHEN idade < 25 THEN '18-24 anos'
                WHEN idade < 35 THEN '25-34 anos'
                WHEN idade < 45 THEN '35-44 anos'
                WHEN idade < 55 THEN '45-54 anos'
                WHEN idade < 65 THEN '55-64 anos'
                ELSE '65+ anos'
            END as faixa_etaria,
            COUNT(*) as quantidade,
            ROUND(AVG(renda_mensal), 2) as renda_media,
            ROUND(MIN(renda_mensal), 2) as renda_minima,
            ROUND(MAX(renda_mensal), 2) as renda_maxima
        FROM (
            SELECT 
                *,
                dateDiff('year', data_nascimento, today()) as idade
            FROM {db_name}.{table_name}
        )
        GROUP BY faixa_etaria
        ORDER BY renda_media DESC
        """
        df4 = clickhouse.execute_query_to_df(query4)
        print(df4.to_string(index=False))
        
        # QUERY 5: Clientes ativos vs inativos com estatísticas
        print("\n" + "="*60)
        print("QUERY 5: Clientes ativos vs inativos")
        print("="*60)
        
        query5 = f"""
        SELECT 
            CASE WHEN ativo = 1 THEN 'Ativo' ELSE 'Inativo' END as status,
            COUNT(*) as quantidade,
            ROUND(AVG(renda_mensal), 2) as renda_media,
            ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(), 2) as percentual
        FROM {db_name}.{table_name}
        GROUP BY ativo
        ORDER BY quantidade DESC
        """
        df5 = clickhouse.execute_query_to_df(query5)
        print(df5.to_string(index=False))
        
        # QUERY 6: Clientes cadastrados por ano
        print("\n" + "="*60)
        print("QUERY 6: Clientes cadastrados por ano")
        print("="*60)
        
        query6 = f"""
        SELECT 
            toYear(data_cadastro) as ano_cadastro,
            COUNT(*) as quantidade_clientes,
            ROUND(AVG(renda_mensal), 2) as renda_media_ano
        FROM {db_name}.{table_name}
        GROUP BY ano_cadastro
        ORDER BY ano_cadastro DESC
        """
        df6 = clickhouse.execute_query_to_df(query6)
        print(df6.to_string(index=False))
        
        # QUERY 7: Top 10 clientes com maior renda
        print("\n" + "="*60)
        print("QUERY 7: Top 10 clientes com maior renda")
        print("="*60)
        
        query7 = f"""
        SELECT 
            id_cliente,
            nome,
            sexo,
            estado,
            renda_mensal,
            CASE WHEN ativo = 1 THEN 'Ativo' ELSE 'Inativo' END as status
        FROM {db_name}.{table_name}
        ORDER BY renda_mensal DESC
        LIMIT 10
        """
        df7 = clickhouse.execute_query_to_df(query7)
        print(df7.to_string(index=False))
        
        # QUERY 8: Análise de domínios de email mais comuns
        print("\n" + "="*60)
        print("QUERY 8: Top 10 domínios de email mais comuns")
        print("="*60)
        
        query8 = f"""
        SELECT 
            splitByChar('@', email)[2] as dominio_email,
            COUNT(*) as quantidade,
            ROUND((COUNT(*) * 100.0) / SUM(COUNT(*)) OVER(), 2) as percentual
        FROM {db_name}.{table_name}
        GROUP BY dominio_email
        ORDER BY quantidade DESC
        LIMIT 10
        """
        df8 = clickhouse.execute_query_to_df(query8)
        print(df8.to_string(index=False))
        
        # QUERY 9: Resumo geral dos dados
        print("\n" + "="*60)
        print("QUERY 9: Resumo geral dos dados")
        print("="*60)
        
        query9 = f"""
        SELECT 
            COUNT(*) as total_clientes,
            COUNT(DISTINCT estado) as estados_unicos,
            COUNT(DISTINCT cidade) as cidades_unicas,
            countIf(ativo = 1) as clientes_ativos,
            countIf(ativo = 0) as clientes_inativos,
            ROUND(AVG(renda_mensal), 2) as renda_media_geral,
            ROUND(MIN(renda_mensal), 2) as menor_renda,
            ROUND(MAX(renda_mensal), 2) as maior_renda,
            MIN(data_nascimento) as cliente_mais_velho,
            MAX(data_nascimento) as cliente_mais_novo,
            MIN(data_cadastro) as primeiro_cadastro,
            MAX(data_cadastro) as ultimo_cadastro
        FROM {db_name}.{table_name}
        """
        df9 = clickhouse.execute_query_to_df(query9)
        
        print("Resumo Geral:")
        for col in df9.columns:
            print(f"  {col}: {df9.iloc[0][col]}")
        
        print("\n🎉 TODAS AS QUERIES EXECUTADAS COM SUCESSO!")
        print("Verifique os resultados acima para análise dos dados.")
        
    except Exception as e:
        print(f"\n❌ ERRO DURANTE A EXECUÇÃO DAS QUERIES:")
        print(f"Erro: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)