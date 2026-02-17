from clickhouse_driver import Client
import pandas as pd
import datetime as _dt
import numpy as _np
from decimal import Decimal
# Se não houver airflow instalado, AirflowException vira Exception comum
try:
    from airflow.exceptions import AirflowException
except ImportError:
    AirflowException = Exception
pd.set_option('display.max_columns', 50)
class ClickhouseSync:
    def __init__(self,host,port,user,password,database):
        ...
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.client = None

    def connect(self):
        """Estabelece a conexão com o banco de dados ClickHouse."""
        try:
            self.client = Client(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            df = self.execute_query_to_df("show databases")
            print("Conexão estabelecida com sucesso!")
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados: {e}")
            raise AirflowException(e)

    def test_connection(self):
        """Realiza uma consulta simples para testar a conexão."""
        if self.client:
            try:
                query = "SELECT version()"
                result = self.client.execute(query)
                print(f"Versão do ClickHouse: {result[0][0]}")
            except Exception as e:
                print(f"Erro ao realizar a consulta: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def create_database_if_not_exists(self, database_name):
        """Cria uma database caso não exista."""
        if self.client:
            try:
                query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
                self.client.execute(query)
                print(f"Database '{database_name}' criado ou já existe.")
            except Exception as e:
                print(f"Erro ao criar a database '{database_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def drop_database(self, db_name):
        """Remove um banco de dados, exceto o banco de dados principal definido no __init__."""
        if self.client:
            if db_name != self.database:
                try:
                    query = f"DROP DATABASE IF EXISTS {db_name}"
                    self.client.execute(query)
                    print(f"Banco de dados '{db_name}' removido com sucesso.")
                except Exception as e:
                    print(f"Erro ao remover o banco de dados '{db_name}': {e}")
                    raise AirflowException(e)
            else:
                print(f"O banco de dados '{db_name}' não pode ser removido, pois é o banco de dados principal.")
        else:
            print("Cliente não conectado ao banco de dados.")

    def df_to_clickhouse_type(self, dtype):
        """Converte tipos de dados pandas para tipos de dados ClickHouse."""
        if pd.api.types.is_integer_dtype(dtype):
            return 'Int32'
        elif pd.api.types.is_float_dtype(dtype):
            return 'Float64'
        elif pd.api.types.is_bool_dtype(dtype):
            return 'UInt8'
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return 'DateTime'
        else:
            return 'String'
        
    def create_table_from_df(
            self,
            db_name: str,
            table_name: str,
            df: pd.DataFrame,
            datetime_nullable_cols: list[str] | None = None,
        ):
        """
        Cria uma tabela a partir de um DataFrame.
        - Colunas passadas em `datetime_nullable_cols` são forçadas a `Nullable(DateTime)`,
        mesmo que no DataFrame venham como string/objeto.
        - Demais colunas seguem a inferência de tipos, com `Nullable(...)`
        se o DF contiver valores nulos.

        Args
        ----
        db_name : str
            Banco de destino.
        table_name : str
            Nome da tabela a ser criada.
        df : pd.DataFrame
            DataFrame de referência.
        datetime_nullable_cols : list[str] | None
            Colunas que devem ser salvas como `Nullable(DateTime)`.
        """
        datetime_nullable_cols = set(datetime_nullable_cols or [])

        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return

        try:
            self.create_database_if_not_exists(db_name)

            columns = []
            for col_name, dtype in df.dtypes.items():
                # --- tipo base inferido ---
                if col_name in datetime_nullable_cols:
                    click_type = "DateTime"  # força DateTime
                    is_nullable = True        # sempre Nullable
                else:
                    click_type = self.df_to_clickhouse_type(dtype)
                    is_nullable = df[col_name].isnull().any()

                if is_nullable:
                    click_type = f"Nullable({click_type})"

                columns.append(f"`{col_name}` {click_type}")

            columns_str = ",\n    ".join(columns)

            query = f"""
            CREATE TABLE IF NOT EXISTS {db_name}.{table_name} (
                {columns_str}
            )
            ENGINE = MergeTree()
            ORDER BY tuple()
            """
            self.client.execute(query)
            print(f"Tabela '{table_name}' criada no banco '{db_name}' com sucesso.")
        except Exception as e:
            print(f"Erro ao criar a tabela '{table_name}': {e}")
            raise AirflowException(e)

    def insert_df_in_batches(self, db_name, table_name, df, batch_size=200000):
        """Insere dados de um DataFrame em uma tabela no ClickHouse em lotes de batch_size, garantindo tipos de dados compatíveis."""
        if self.client:
            try:
                # Passo 1: Obter os tipos de dados das colunas da tabela no banco
                query_get_types = f"DESCRIBE TABLE {db_name}.{table_name}"
                table_schema = self.client.execute(query_get_types)
                
                # Criar um dicionário com os nomes das colunas e seus respectivos tipos de dados
                column_types = {column[0]: column[1] for column in table_schema}
                
                # Passo 2: Verificar os tipos de dados e transformar conforme necessário
                for col in df.columns:
                    expected_type = column_types.get(col)
                    
                    if expected_type:
                        # Mapear o tipo do ClickHouse para um tipo Python equivalente (exemplo básico)
                        if 'Int' in expected_type:
                            df[col] = df[col].astype("Int64")
                        elif 'Float' in expected_type:
                            df[col] = df[col].astype(float)
                        elif 'String' in expected_type or 'FixedString' in expected_type:
                            df[col] = df[col].astype(str)
                        elif 'Date' in expected_type or 'DateTime' in expected_type:
                            df[col] = pd.to_datetime(df[col])
                        # Aqui, você pode adicionar outros tipos conforme necessário

                # Passo 3: Dividir o DataFrame em lotes de tamanho batch_size e inserir no banco
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i+batch_size]

                    # Converter o DataFrame para uma lista de tuplas (formato esperado pelo ClickHouse)
                    data = [tuple(x) for x in batch_df.to_numpy()]
                    
                    # Gerar a lista de colunas para o INSERT
                    columns_str = ', '.join([f"`{col}`" for col in df.columns])
                    
                    # Query de inserção
                    query = f"INSERT INTO {db_name}.{table_name} ({columns_str}) VALUES"
                    
                    # Executar a inserção dos dados
                    self.client.execute(query, data)
                    
                    print(f"Lote {i//batch_size + 1} inserido com sucesso.")
            except Exception as e:
                print(f"Erro ao inserir dados na tabela '{table_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def clean_table(self,db_name,table_name):
        if self.client:
            try:
                query = f"DELETE FROM {db_name}.{table_name} WHERE 1 = 1"
                self.client.execute(query)
                print(f"Dados da tabela {db_name}.{table_name} deletados com sucesso")
            except Exception as e:
                print(f"Erro ao inserir dados na tabela '{table_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def drop_table(self, db_name, table_name):
        """Remove uma tabela de um banco de dados."""
        if self.client:
            try:
                query = f"DROP TABLE IF EXISTS {db_name}.{table_name}"
                self.client.execute(query)
                print(f"Tabela '{table_name}' no banco de dados '{db_name}' removida com sucesso.")
            except Exception as e:
                print(f"Erro ao remover a tabela '{table_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def delete_data_by_date(self, db_name, table_name, date_column, comparator, date_value):
        """
        Apaga dados de uma tabela com base em uma comparação de datas.
        
        Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.
        date_column (str): Nome da coluna de data para comparação.
        comparator (str): Tipo de comparador ('>', '>=', '<', '<=', '=').
        date_value (str): Valor da data para comparação (formato 'YYYY-MM-DD').
        """
        if self.client:
            valid_comparators = ['>', '>=', '<', '<=', '=']
            if comparator not in valid_comparators:
                print(f"Comparador '{comparator}' inválido. Use um dos seguintes: {valid_comparators}")
                return
            
            try:
                query = f"""
                DELETE FROM {db_name}.{table_name}
                WHERE `{date_column}` {comparator} '{date_value}'
                """
                self.client.execute(query)
                print(f"Dados da tabela '{table_name}' no banco '{db_name}' com '{date_column} {comparator} {date_value}' deletados com sucesso.")
            except Exception as e:
                print(f"Erro ao deletar dados: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def get_row_count(self, db_name, table_name):
        """
        Retorna o número de registros em uma tabela específica de um banco de dados.
        
        Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.
        
        Returns:
        int: Contagem de registros na tabela.
        """
        if self.client:
            try:
                query = f"SELECT count(*) FROM {db_name}.{table_name}"
                result = self.client.execute(query)
                row_count = result[0][0]
                print(f"A tabela '{table_name}' no banco '{db_name}' contém {row_count} registros.")
                return row_count
            except Exception as e:
                print(f"Erro ao obter a contagem de registros da tabela '{table_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")
            return None

    def table_exists(self, db_name, table_name):
        """
        Verifica se uma tabela existe em um banco de dados específico.
        
        Args:
        db_name (str): Nome do banco de dados.
        table_name (str): Nome da tabela.
        
        Returns:
        bool: True se a tabela existir, False caso contrário.
        """
        if self.client:
            try:
                query = f"EXISTS TABLE {db_name}.{table_name}"
                result = self.client.execute(query)
                exists = result[0][0] == 1
                print(f"A tabela '{table_name}' no banco '{db_name}' {'existe' if exists else 'não existe'}.")
                return exists
            except Exception as e:
                print(f"Erro ao verificar a existência da tabela '{table_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")
            return False
        
    def execute_query_to_df(self, query):
        """
        Executa uma query e retorna o resultado como um DataFrame do Pandas.
        
        Args:
        query (str): A query SQL a ser executada.
        
        Returns:
        pd.DataFrame: DataFrame contendo os resultados da query.
        """
        if self.client:
            try:
                # Executa a query e obtém os resultados
                result = self.client.execute(query, with_column_types=True)
                
                # A função execute retorna duas coisas quando with_column_types=True:
                # 1. A lista de resultados
                # 2. A lista de colunas com seus tipos [(coluna, tipo), ...]
                data, columns_info = result
                
                # Extrai os nomes das colunas a partir da descrição retornada
                column_names = [col[0] for col in columns_info]
                
                # Retorna os resultados como um DataFrame
                df = pd.DataFrame(data, columns=column_names)
                return df
            
            except Exception as e:
                print(f"Erro ao executar a query: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")
            return None

    # Novos métodos para gerenciamento de views
    def create_view(self, db_name, view_name, select_query):
        """
        Cria uma view no ClickHouse.

        Args:
            db_name (str): Nome do banco de dados.
            view_name (str): Nome da view a ser criada.
            select_query (str): Consulta SQL que define a view.
        """
        if self.client:
            try:
                # Assegura que o banco de dados existe
                self.create_database_if_not_exists(db_name)

                # Define a consulta para criar a view
                query = f"""
                CREATE OR REPLACE VIEW {db_name}.{view_name} AS
                {select_query}
                """
                self.client.execute(query)
                print(f"View '{view_name}' criada no banco de dados '{db_name}' com sucesso.")
            except Exception as e:
                print(f"Erro ao criar a view '{view_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def drop_view(self, db_name, view_name):
        """
        Remove uma view do ClickHouse.

        Args:
            db_name (str): Nome do banco de dados.
            view_name (str): Nome da view a ser removida.
        """
        if self.client:
            try:
                query = f"DROP VIEW IF EXISTS {db_name}.{view_name}"
                self.client.execute(query)
                print(f"View '{view_name}' no banco de dados '{db_name}' removida com sucesso.")
            except Exception as e:
                print(f"Erro ao remover a view '{view_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def export_view_to_parquet(self, db_name, view_name, output_file_path):
        """
        Exporta o resultado de uma view para um arquivo Parquet.

        Args:
            db_name (str): Nome do banco de dados.
            view_name (str): Nome da view a ser exportada.
            output_file_path (str): Caminho para o arquivo Parquet de destino.
        """
        if self.client:
            try:
                query = f"COPY {db_name}.{view_name} TO '{output_file_path}' (FORMAT Parquet, COMPRESSION 'SNAPPY')"
                self.client.execute(query)
                print(f"View '{view_name}' exportada para o arquivo Parquet '{output_file_path}' com sucesso.")
            except Exception as e:
                print(f"Erro ao exportar a view '{view_name}' para Parquet: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def optimize_table(self, db_name, table_name):
        if self.client:
            try:
                query = f"OPTIMIZE TABLE {db_name}.{table_name} FINAL"
                self.client.execute(query)
                print(f"Tabela '{table_name}' otimizada com sucesso.")
            except Exception as e:
                print(f"Erro ao otimizar a tabela '{table_name}': {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def execute_command(self, command):
        """Executa um comando no banco de dados fornecido por uma string via argumento.

        Args:
            command (str): O comando SQL a ser executado.

        Returns:
            list: Resultado da execução do comando.
        """
        if self.client:
            try:
                result = self.client.execute(command)
                print("Comando executado com sucesso!")
                return result
            except Exception as e:
                print(f"Erro ao executar o comando: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")
        return None
    
    def create_view_if_not_exists(self, db_name, view_name, select_query):
        """Cria uma view no banco de dados caso ela não exista.

        Args:
            view_name (str): Nome da view a ser criada.
            select_query (str): Query SELECT para definir o conteúdo da view.

        Returns:
            None
        """
        if self.client:
            try:
                check_query = f"EXISTS VIEW {db_name}.{view_name}"
                exists = self.client.execute(check_query)
                if exists[0][0] == 0:
                    create_query = f"CREATE VIEW {db_name}.{view_name} AS {select_query}"
                    self.client.execute(create_query)
                    print(f"View '{db_name}.{view_name}' criada com sucesso!")
                else:
                    print(f"View '{db_name}.{view_name}' já existe.")
            except Exception as e:
                print(f"Erro ao criar a view: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def ensure_all_columns_nullable(self, db_name: str, table_name: str, skip: tuple[str,...] = ()):
        """
        Converte todas as colunas não-nullable para Nullable(...).
        Use 'skip' se precisar preservar alguma coluna específica.
        """
        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return
        desc = self.client.execute(f"DESCRIBE TABLE {db_name}.{table_name}")
        alters = []
        for row in desc:
            # DESCRIBE retorna: name, type, default_type, default_expression, ... (varia por versão)
            col_name, col_type = row[0], row[1]
            if col_name in skip:
                continue
            if not col_type.startswith("Nullable("):
                alters.append(f"MODIFY COLUMN `{col_name}` Nullable({col_type})")
        if alters:
            alter_sql = f"ALTER TABLE {db_name}.{table_name} " + ", ".join(alters)
            self.client.execute(alter_sql)
            print("Schema atualizado: colunas convertidas para Nullable(...).")
        else:
            print("Schema já compatível: todas as colunas são Nullable(...).")
            
    
    def query(self, query):
        """Realiza uma consulta simples."""
        if self.client:
            try:
                result, columns = self.client.execute(query, with_column_types=True)
                column_names = [col[0] for col in columns]
                return {
                    'columns': column_names,
                    'data': result
                }
            except Exception as e:
                print(f"Erro ao realizar a consulta: {e}")
                raise AirflowException(e)
        else:
            print("Cliente não conectado ao banco de dados.")

    def insert_df_in_batches_v3(
        self,
        db_name: str,
        table_name: str,
        df: pd.DataFrame,
        batch_size: int = 200000,
        datetime_strfmt: str = "%Y-%m-%d %H:%M:%S",
    ):
        """
        Insere dados em lotes respeitando o schema do ClickHouse.
        - Trata NaT/NaN/None (inclui sanitização final por célula).
        - DateTime -> datetime (tz-naive) | None
        - Date     -> date | None
        - String   -> str (Timestamp usa `datetime_strfmt`, date 'YYYY-MM-DD') | None
        - Converte numéricos de forma tolerante.
        - Descarta colunas extras que não existem na tabela.
        """
        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return

        try:
            if df is None or df.empty:
                print("DataFrame vazio; nada a inserir.")
                return



            ts_min = pd.Timestamp("1970-01-01")

            df = df.copy()

            # Permite None em colunas originalmente 'string' do pandas
            for col in df.columns:
                if pd.api.types.is_string_dtype(df[col].dtype):
                    df[col] = df[col].astype(object)

            # Substitui NaN/NaT por None de início
            df = df.where(pd.notna(df), None)

            # Lê o schema da tabela de destino
            schema = self.client.execute(f"DESCRIBE TABLE {db_name}.{table_name}")
            column_types = {col[0]: col[1] for col in schema}

            # Descarta colunas do DF que não existem na tabela
            extra_cols = [c for c in df.columns if c not in column_types]
            if extra_cols:
                df = df.drop(columns=extra_cols)
                print(f"Colunas ignoradas (não existem em {db_name}.{table_name}): {extra_cols}")

            for col in df.columns:
                tp = column_types[col]
                base_tp = tp[9:-1] if tp.startswith("Nullable(") and tp.endswith(")") else tp
                s = df[col]

                # ---- DateTime ----
                if base_tp.startswith("DateTime"):
                    s = pd.to_datetime(s, errors="coerce", utc=False)
                    try:
                        if getattr(s.dt, "tz", None) is not None:
                            s = s.dt.tz_localize(None)
                    except Exception:
                        pass
                    s = s.apply(lambda x: x if (pd.notna(x) and x >= ts_min) else None)
                    df[col] = s.astype(object)

                # ---- Date ----
                elif base_tp == "Date":
                    s = pd.to_datetime(s, errors="coerce", utc=False)
                    try:
                        if getattr(s.dt, "tz", None) is not None:
                            s = s.dt.tz_localize(None)
                    except Exception:
                        pass
                    s = s.apply(lambda x: x.date() if pd.notna(x) else None)
                    df[col] = s.astype(object)

                # ---- String / FixedString ----
                elif "String" in base_tp or base_tp.startswith("FixedString"):
                    # IMPORTANTE: evitar path datetime-like do pandas
                    s = s.astype(object)
                    def _to_str(v):
                        # <<< linha decisiva contra o erro >>>
                        if v is None or v is pd.NaT or pd.isna(v):
                            return None
                        if isinstance(v, (pd.Timestamp, _dt.datetime)):
                            return v.strftime(datetime_strfmt)
                        if isinstance(v, _dt.date):
                            return v.strftime("%Y-%m-%d")
                        return str(v)
                    df[col] = s.map(_to_str).astype(object)

                # ---- Inteiros (Int/UInt) ----
                elif "Int" in base_tp or base_tp.startswith("UInt"):
                    def _to_int(v):
                        if v is None or (isinstance(v, float) and pd.isna(v)):
                            return None
                        try:
                            return int(v)
                        except Exception:
                            return None
                    df[col] = s.map(_to_int).astype(object)

                # ---- Float ----
                elif "Float" in base_tp:
                    def _to_float(v):
                        if v is None or (isinstance(v, float) and pd.isna(v)):
                            return None
                        try:
                            return float(v)
                        except Exception:
                            return None
                    df[col] = s.map(_to_float).astype(object)

                # ---- Decimal ----
                elif base_tp.startswith("Decimal"):
                    def _to_decimal(v):
                        if v is None or (isinstance(v, float) and pd.isna(v)):
                            return None
                        try:
                            return Decimal(str(v))
                        except Exception:
                            try:
                                return float(v)
                            except Exception:
                                return None
                    df[col] = s.map(_to_decimal).astype(object)

                # ---- UUID ----
                elif base_tp == "UUID":
                    def _to_uuid_str(v):
                        if v is None or (isinstance(v, float) and pd.isna(v)):
                            return None
                        return str(v)
                    df[col] = s.map(_to_uuid_str).astype(object)

                # ---- Default ----
                else:
                    def _nullify(v):
                        return None if (v is None or (isinstance(v, float) and pd.isna(v)) or pd.isna(v)) else v
                    df[col] = s.map(_nullify).astype(object)

            # Ordem de colunas conforme o DF final (todas existentes no schema)
            cols = list(df.columns)
            if not cols:
                print("Nenhuma coluna compatível com o schema de destino; nada a inserir.")
                return

            columns_str = ", ".join(f"`{c}`" for c in cols)

            # ===== Sanitizador final no empacotamento do batch =====
            def _clean_cell(v):
                if v is None or v is pd.NaT:
                    return None
                # cobre NaN/NaT genéricos
                try:
                    if pd.isna(v):
                        return None
                except Exception:
                    pass
                # garante datetime tz-naive como datetime.datetime puro
                if isinstance(v, pd.Timestamp):
                    try:
                        return (v.tz_convert(None).to_pydatetime()
                                if v.tzinfo is not None else v.to_pydatetime())
                    except Exception:
                        return None
                return v
            # =======================================================

            # Insert em lotes
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i:i + batch_size]
                data = [tuple(_clean_cell(v) for v in row)
                        for row in batch_df.itertuples(index=False, name=None)]
                query = f"INSERT INTO {db_name}.{table_name} ({columns_str}) VALUES"
                self.client.execute(query, data)
                print(f"Lote {i // batch_size + 1} inserido com sucesso.")

        except Exception as e:
            print(f"Erro ao inserir dados na tabela '{table_name}': {e}")
            raise AirflowException(e)

    def value_exists(
        self,
        db_name: str,
        table_name: str,
        column_name: str,
        value,
    ) -> bool:
        """
        Verifica se existe algum registro em uma tabela com um determinado valor em uma coluna.

        Args
        ----
        db_name : str
            Nome do banco de dados (schema).
        table_name : str
            Nome da tabela.
        column_name : str
            Nome da coluna a ser verificada.
        value : qualquer tipo
            Valor a ser procurado (string, número, data etc).

        Returns
        -------
        bool
            True se existir pelo menos um registro com o valor especificado, False caso contrário.
        """
        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return False

        try:
            # tratamento para strings e datas
            if isinstance(value, str):
                value_expr = f"'{value.replace('\'', '')}'"
            elif isinstance(value, (_dt.date, _dt.datetime)):
                value_expr = f"'{value}'"
            else:
                value_expr = str(value)

            query = f"""
            SELECT count() > 0 AS exists_flag
            FROM {db_name}.{table_name}
            WHERE `{column_name}` = {value_expr}
            LIMIT 1
            """
            result = self.client.execute(query)
            exists = bool(result[0][0])
            print(
                f"Valor '{value}' {'encontrado' if exists else 'não encontrado'} "
                f"em {db_name}.{table_name}.{column_name}"
            )
            return exists

        except Exception as e:
            print(f"Erro ao verificar existência do valor na tabela '{table_name}': {e}")
            raise AirflowException(e)

    def delete_data_by_date_and_value(
        self,
        db_name: str,
        table_name: str,
        date_column: str,
        comparator: str,
        date_value: str,
        filter_column: str,
        filter_value,
    ):
        """
        Apaga dados de uma tabela com base em uma comparação de datas e uma condição adicional de igualdade.

        Args
        ----
        db_name : str
            Nome do banco de dados.
        table_name : str
            Nome da tabela.
        date_column : str
            Nome da coluna de data para comparação.
        comparator : str
            Tipo de comparador ('>', '>=', '<', '<=', '=').
        date_value : str
            Valor da data para comparação (formato 'YYYY-MM-DD' ou 'YYYY-MM-DD HH:MM:SS').
        filter_column : str
            Nome da coluna adicional usada no filtro.
        filter_value : qualquer tipo
            Valor que deve ser igual na coluna adicional.

        Exemplo
        -------
        ch.delete_data_by_date_and_value(
            "bi_commercial", "vendas_raw",
            date_column="data_venda", comparator=">=", date_value="2025-10-01",
            filter_column="loja", filter_value="Loja 01"
        )
        """
        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return

        valid_comparators = ['>', '>=', '<', '<=', '=']
        if comparator not in valid_comparators:
            print(f"Comparador '{comparator}' inválido. Use um dos seguintes: {valid_comparators}")
            return

        try:
            if isinstance(filter_value, str):
                value_expr = repr(filter_value).strip("'").replace("'", "")
            elif isinstance(filter_value, (_dt.date, _dt.datetime)):
                value_expr = f"'{filter_value}'"
            else:
                value_expr = str(filter_value)

            query = f"""
            DELETE FROM {db_name}.{table_name}
            WHERE `{date_column}` {comparator} '{date_value}'
              AND `{filter_column}` = '{value_expr}'
            """

            self.client.execute(query)
            print(
                f"Registros de '{db_name}.{table_name}' removidos com sucesso "
                f"onde {date_column} {comparator} '{date_value}' e {filter_column} = {filter_value}"
            )

        except Exception as e:
            print(f"Erro ao deletar dados da tabela '{table_name}': {e}")
            raise AirflowException(e)


    def delete_by_value(
        self,
        db_name: str,
        table_name: str,
        column_name: str,
        value=None,
        comparator: str = "=",
        dry_run: bool = False,
    ):
        """
        Deleta registros de {db_name}.{table_name} onde `column_name` <comparator> value.
        Suporta: '=', '!=', '>', '>=', '<', '<=', 'IN', 'NOT IN', 'LIKE', 'ILIKE',
                'IS NULL', 'IS NOT NULL'.

        Args
        ----
        db_name : str
        table_name : str
        column_name : str
        value : qualquer tipo | list/tuple/set (para IN/NOT IN) | ignorado em IS NULL/IS NOT NULL
        comparator : str
        dry_run : bool  -> se True, apenas mostra a contagem afetada (não deleta)
        """
        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return

        valid_ops = {
            "=", "!=", ">", ">=", "<", "<=",
            "IN", "NOT IN", "LIKE", "ILIKE",
            "IS NULL", "IS NOT NULL"
        }
        if comparator not in valid_ops:
            raise AirflowException(
                f"Comparador '{comparator}' inválido. Use um de: {sorted(valid_ops)}"
            )

        try:
            col = f"`{column_name}`"  # protege o identificador

            # Monta query e parâmetros
            params = {}
            if comparator in ("IS NULL", "IS NOT NULL"):
                where = f"{col} {comparator}"
            elif comparator in ("IN", "NOT IN"):
                if not isinstance(value, (list, tuple, set)) or len(value) == 0:
                    raise AirflowException("Para IN/NOT IN, 'value' deve ser lista/tupla/set não vazia.")
                params["vals"] = list(value)
                where = f"{col} {comparator} %(vals)s"
            else:
                params["val"] = value
                where = f"{col} {comparator} %(val)s"

            # Dry run: quantos seriam afetados?
            count_q = f"SELECT count() FROM {db_name}.{table_name} WHERE {where}"
            n = self.client.execute(count_q, params)[0][0]
            msg_prefix = f"[{db_name}.{table_name}] {n} registro(s) "
            if dry_run:
                print(msg_prefix + "seriam afetados (dry_run=True). Nenhuma exclusão realizada.")
                return

            # Executa a deleção (mutação)
            del_q = f"DELETE FROM {db_name}.{table_name} WHERE {where}"
            self.client.execute(del_q, params)
            print(msg_prefix + "deletados com sucesso.")

        except Exception as e:
            print(f"Erro ao deletar dados de {db_name}.{table_name}: {e}")
            raise AirflowException(e)
            
    def insert_df_in_batches_v4(
        self,
        db_name: str,
        table_name: str,
        df: pd.DataFrame,
        batch_size: int = 200000,
        datetime_strfmt: str = "%Y-%m-%d %H:%M:%S",
        debug_bad: bool = True,
        debug_bad_n: int = 20,
    ):
        """
        Insert super robusto:
        - Converte de acordo com DESCRIBE TABLE
        - Int/UInt sempre vira Python int ou None (Nullable) / default (não-nullable)
        - Remove wrappers (Nullable / LowCardinality) em LOOP
        - Detecta Int/UInt mesmo que o type venha com wrappers residuais
        - Evita pd.NA e numpy scalars no payload final
        - (debug_bad) acusa e falha se ainda sobrar algo não-int em colunas Int/UInt
        """
        import re
        import math
        import datetime as _dt
        from decimal import Decimal

        if not self.client:
            print("Cliente não conectado ao banco de dados.")
            return

        if df is None or df.empty:
            print("DataFrame vazio; nada a inserir.")
            return

        # ---------- helpers ----------
        def _strip_wrappers(tp: str) -> tuple[str, bool]:
            """
            Remove LowCardinality(...) e Nullable(...) em LOOP (pode vir aninhado).
            Retorna (base_type, is_nullable).
            Ex:
            Nullable(Int32) -> ("Int32", True)
            LowCardinality(Nullable(Int32)) -> ("Int32", True)
            Nullable(LowCardinality(Int32)) -> ("Int32", True)
            """
            t = tp.strip()
            nullable = False

            while True:
                m = re.match(r"LowCardinality\((.+)\)$", t)
                if m:
                    t = m.group(1).strip()
                    continue

                m = re.match(r"Nullable\((.+)\)$", t)
                if m:
                    nullable = True
                    t = m.group(1).strip()
                    continue

                break

            return t, nullable

        _INT_RE = re.compile(r"^[-+]?\d+([.,]\d+)?$")

        def _int_bounds(base_type: str):
            unsigned = base_type.startswith("UInt")
            bits = int(re.findall(r"\d+", base_type)[0])
            if unsigned:
                return 0, (2**bits) - 1
            return -(2 ** (bits - 1)), (2 ** (bits - 1)) - 1

        def _coerce_int(v, base_type: str, nullable: bool):
            unsigned = base_type.startswith("UInt")
            default = 0 if unsigned else -1

            # None / NaN / pd.NA
            if v is None:
                return None if nullable else default
            try:
                if pd.isna(v):
                    return None if nullable else default
            except Exception:
                pass

            # bool
            if isinstance(v, bool):
                iv = int(v)

            # python/numpy ints
            elif isinstance(v, (int, _np.integer)):
                iv = int(v)

            # float / numpy float / Decimal
            elif isinstance(v, (float, _np.floating, Decimal)):
                fv = float(v)
                if math.isnan(fv) or not fv.is_integer():
                    return None if nullable else default
                iv = int(fv)

            else:
                s = str(v).strip()
                if s == "" or s.lower() in ("nan", "none", "<na>", "null"):
                    return None if nullable else default
                s2 = s.replace(",", ".")
                if not _INT_RE.match(s2):
                    return None if nullable else default
                try:
                    fv = float(s2)
                except Exception:
                    return None if nullable else default
                if not fv.is_integer():
                    return None if nullable else default
                iv = int(fv)

            if unsigned and iv < 0:
                return None if nullable else default

            mn, mx = _int_bounds(base_type)
            if iv < mn or iv > mx:
                return None if nullable else default

            return iv  # PYTHON INT PURO

        def _coerce_float(v, nullable: bool):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            try:
                return float(v)
            except Exception:
                return None

        def _coerce_datetime(v, nullable: bool):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            ts = pd.to_datetime(v, errors="coerce", utc=False)
            if pd.isna(ts):
                return None
            if isinstance(ts, pd.Timestamp):
                if ts.tzinfo is not None:
                    ts = ts.tz_convert(None)
                return ts.to_pydatetime()
            if isinstance(ts, _dt.datetime):
                return ts.replace(tzinfo=None)
            return None

        def _coerce_date(v, nullable: bool):
            dtv = _coerce_datetime(v, nullable)
            if dtv is None:
                return None
            return dtv.date()

        def _coerce_str(v, nullable: bool):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            if isinstance(v, (pd.Timestamp, _dt.datetime)):
                return v.strftime(datetime_strfmt)
            if isinstance(v, _dt.date):
                return v.strftime("%Y-%m-%d")
            return str(v)

        def _coerce_decimal(v, nullable: bool):
            if v is None:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass
            try:
                return Decimal(str(v))
            except Exception:
                try:
                    return float(v)
                except Exception:
                    return None

        # ---------- schema ----------
        schema = self.client.execute(f"DESCRIBE TABLE {db_name}.{table_name}")
        column_types = {col[0]: col[1] for col in schema}

        dfx = df.copy()

        # drop extras
        extra_cols = [c for c in dfx.columns if c not in column_types]
        if extra_cols:
            dfx = dfx.drop(columns=extra_cols)
            print(f"Colunas ignoradas (não existem em {db_name}.{table_name}): {extra_cols}")

        if dfx.empty or len(dfx.columns) == 0:
            print("Nenhuma coluna compatível com o schema de destino; nada a inserir.")
            return

        # troca NaN/NaT por None logo
        dfx = dfx.where(pd.notna(dfx), None)

        # ---------- per-column coercion ----------
        bad_report = {}

        for col in dfx.columns:
            tp = column_types[col]
            base_tp, nullable = _strip_wrappers(tp)
            s = dfx[col].astype("object")

            # DateTime / Date
            if base_tp.startswith("DateTime"):
                out = s.map(lambda v: _coerce_datetime(v, nullable))
            elif base_tp == "Date":
                out = s.map(lambda v: _coerce_date(v, nullable))

            # String / FixedString
            elif "String" in base_tp or base_tp.startswith("FixedString"):
                out = s.map(lambda v: _coerce_str(v, nullable))

            # Decimal
            elif base_tp.startswith("Decimal"):
                out = s.map(lambda v: _coerce_decimal(v, nullable))

            # UUID
            elif base_tp == "UUID":
                out = s.map(lambda v: None if v is None else str(v))

            # Int/UInt (ROBUSTO: acha Int32 mesmo se vier com wrappers residuais)
            else:
                m_int = re.search(r"\bU?Int(8|16|32|64)\b", base_tp)
                if m_int:
                    int_tp = m_int.group(0)  # Int32 / UInt16 ...
                    out = s.map(lambda v: _coerce_int(v, int_tp, nullable))
                elif base_tp.startswith("Float"):
                    out = s.map(lambda v: _coerce_float(v, nullable))
                else:
                    # fallback: só garante None pra NA
                    out = s.map(lambda v: None if v is None else v)

            # debug dos "ruins" para Int/UInt: valida o RESULTADO FINAL
            if debug_bad:
                m_chk = re.search(r"\bU?Int(8|16|32|64)\b", base_tp)
                if m_chk:
                    def _is_ok_int(v):
                        if v is None:
                            return True
                        return isinstance(v, (int, _np.integer))  # python int ou numpy int

                    bad_mask = out.map(lambda v: not _is_ok_int(v))
                    if bool(bad_mask.any()):
                        bad_report[col] = {
                            "type": tp,
                            "count": int(bad_mask.sum()),
                            "sample": out[bad_mask].head(debug_bad_n).tolist(),
                            "types": out[bad_mask].map(type).value_counts().head(5).to_dict(),
                        }

            dfx[col] = out.astype("object")

        if debug_bad and bad_report:
            print("⚠️ Colunas Int/UInt ainda com valores inválidos após coerce:")
            for c, info in bad_report.items():
                print(f" - {c} ({info['type']}): {info['count']}")
                print(f"   sample: {info['sample']}")
                print(f"   types: {info['types']}")
            raise AirflowException("Ainda há valores inválidos em colunas Int/UInt (ver logs).")

        # ---------- insert batches ----------
        cols = list(dfx.columns)
        columns_str = ", ".join(f"`{c}`" for c in cols)
        query = f"INSERT INTO {db_name}.{table_name} ({columns_str}) VALUES"

        def _clean_cell(v):
            # None / NaT / NA / NaN
            if v is None or v is pd.NaT:
                return None
            try:
                if pd.isna(v):
                    return None
            except Exception:
                pass

            # numpy scalars -> python nativo (inclusive np.int64, np.float64)
            if isinstance(v, _np.generic):
                v = v.item()

            # pandas Timestamp -> datetime python tz-naive
            if isinstance(v, pd.Timestamp):
                try:
                    if v.tzinfo is not None:
                        v = v.tz_convert(None)
                    return v.to_pydatetime()
                except Exception:
                    return None

            return v

        for i in range(0, len(dfx), batch_size):
            batch_df = dfx.iloc[i:i + batch_size]
            data = [
                tuple(_clean_cell(x) for x in row)
                for row in batch_df.itertuples(index=False, name=None)
            ]
            self.client.execute(query, data)
            print(f"Lote {i // batch_size + 1} inserido com sucesso.")
    def create_view_engine(
            self,
            db_name: str,
            view_name: str,
            select_query: str,
            *,
            kind: str = "view",                  # "view" | "materialized" | "refreshable"
            if_not_exists: bool = True,
            or_replace: bool = False,          # só faz sentido p/ kind="view"
            to_table: str | None = None,       # ex: "dosing_enriched" (opcional)
            engine: str | None = None,         # ex: "MergeTree ORDER BY (company_code, data_hora)"
            populate: bool = False,            # só p/ kind="materialized" e sem TO (ver nota)
            refresh: str | None = None,        # ex: "EVERY 1 YEAR" ou "AFTER 365 DAY"
            append: bool = False,              # p/ refreshable
            empty: bool = False,               # p/ refreshable
        ) -> None:
            """
            Cria VIEW, MATERIALIZED VIEW ou REFRESHABLE MATERIALIZED VIEW no ClickHouse.
            Compatível com clickhouse_driver.Client (porta TCP 9000).
            
            Cria views no ClickHouse. Resumo de parâmetros por tipo:

            Tipo                    | kind         | to_table | engine | populate | refresh     | append | Uso recomendado
            ------------------------|--------------|----------|--------|----------|-------------|--------|-----------------
            View normal             | view         | não      | não    | não      | não         | não    | Consultas simples
            Atualizar view          | view         | não      | não    | não      | não         | não    | or_replace=True
            Materialized persistida | materialized | sim      | não    | não      | não         | não    | Produção (recomendado)
            Materialized interna    | materialized | não      | sim    | sim/não  | não         | não    | Testes/protótipos
            Refreshable (agendada)  | refreshable  | sim/não  | sim    | não      | obrigatório | sim    | Dashboards/relatórios periódicos

            Regras importantes:
            - TO + POPULATE: proibido
            - refreshable: refresh deve ser 'EVERY ...' ou 'AFTER ...'
            - OR REPLACE: só para view
            """
            if not self.client:
                raise AirflowException("Cliente não conectado. Chame .connect() primeiro.")

            kind = (kind or "view").lower().strip()
            if kind not in {"view", "materialized", "refreshable"}:
                raise AirflowException(f"kind inválido: {kind}. Use 'view', 'materialized' ou 'refreshable'.")

            select_sql = _sanitize_select(select_query)
            ine = "IF NOT EXISTS " if if_not_exists else ""
            name_sql = _qn(db_name, view_name)

            try:
                if kind == "view":
                    rep = "OR REPLACE " if or_replace else ""
                    create_sql = f"CREATE {rep}VIEW {ine}{name_sql} AS {select_sql}"

                elif kind == "materialized":
                    if or_replace:
                        drop_sql = f"DROP VIEW IF EXISTS {name_sql}"
                        try:
                            self.client.execute(drop_sql)
                            print(f"[INFO] View '{view_name}' existente foi dropada (devido a or_replace=True)")
                        except Exception as drop_err:
                            # Ignora se já não existia ou erro não crítico, mas loga
                            print(f"[WARN] Falha ao dropar view existente: {str(drop_err)} (continuando...)")
                    to_sql = f" TO {_qn(db_name, to_table)}" if to_table else ""
                    eng_sql = f" ENGINE = {engine}" if engine else ""
                    pop_sql = " POPULATE" if populate else ""

                    if not to_table and not engine:
                        raise AirflowException(
                            "Para MATERIALIZED VIEW sem TO, informe 'engine' "
                            "(ex: MergeTree() ORDER BY (col1, col2))"
                        )
                    if to_table and populate:
                        raise AirflowException("POPULATE não pode ser usado com TO.")

                    create_sql = f"CREATE MATERIALIZED VIEW {ine}{name_sql}{to_sql}{eng_sql}{pop_sql} AS {select_sql}"

                else:  # refreshable
                    if not refresh:
                        raise AirflowException(
                            "Para refreshable, informe 'refresh' (ex: 'EVERY 1 DAY', 'AFTER 1 HOUR')"
                        )
                    refresh_upper = refresh.strip().upper()
                    if not (refresh_upper.startswith("EVERY ") or refresh_upper.startswith("AFTER ")):
                        raise AirflowException(
                            f"refresh inválido: '{refresh}'. Deve começar com 'EVERY ' ou 'AFTER '."
                        )
                    if or_replace:
                        raise AirflowException("OR REPLACE não suportado em refreshable materialized view.")

                    to_sql = f" TO {_qn(db_name, to_table)}" if to_table else ""
                    eng_sql = f" ENGINE = {engine}" if engine else ""
                    app_sql = " APPEND" if append else ""
                    emp_sql = " EMPTY" if empty else ""

                    if not to_table and not engine:
                        raise AirflowException(
                            "Para refreshable sem TO, informe 'engine' "
                            "(ex: MergeTree() ORDER BY ...)"
                        )

                    create_sql = (
                        f"CREATE MATERIALIZED VIEW {ine}{name_sql} "
                        f"REFRESH {refresh} "
                        f"{app_sql}{to_sql}{eng_sql}{emp_sql} AS {select_sql}"
                    )

                self.client.execute(create_sql)
                print(f"[OK] View '{view_name}' criada em '{db_name}' (kind: {kind})")  # opcional: remova se preferir logging

            except Exception as e:
                raise AirflowException(f"Erro ao criar view '{view_name}' em '{db_name}': {str(e)}")