import sys
import logging
import traceback
from io import BytesIO
from uuid import uuid4
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
import pyarrow as pa
import pyarrow.dataset as ds
import boto3
import pandas as pd
import awswrangler as wr

try:
    from awsglue.utils import getResolvedOptions
except Exception:
    import argparse
    def getResolvedOptions(argv, options):
        p = argparse.ArgumentParser()
        for opt in options:
            p.add_argument(f'--{opt}')
        ns, _ = p.parse_known_args(argv[1:])
        return vars(ns)

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Global clients cache
_BOTO3_CLIENTS = {}


def prepare_partition_columns(df: pd.DataFrame, year, month, day) -> pd.DataFrame:
    """ Prepara as colunas de partição no DataFrame

    Args:
        df (pd.DataFrame): DataFrame original
        year (str): Ano para a partição
        month (str): Mês para a partição
        day (str): Dia para a partição
    Returns:
        pd.DataFrame: DataFrame com as colunas de partição adicionadas
    """
    df['dat_ano_rffc'] = str(year)
    df['dat_mes_rffc'] = str(month)
    df['dat_dia_rffc'] = str(day)
    df['ticker'] = df['ticker'].astype(str)
    return df


def save_df_to_s3_parquet(df : pd.DataFrame, output_uri: str) -> None:
    """"
    Salva o DataFrame em formato Parquet no S3 com particionamento
    Args:
        df (pd.DataFrame): DataFrame a ser salvo
        output_uri (str): URI do bucket S3 onde os dados serão salvos
    """
    try:
        numeric_columns = [
            'capitalizao_mercado', 'preco_mercado', 'abertura', 
            'minimo_dia', 'maximo_dia', 'delta_variacao_do_dia',
            'delta_variacao_dia_anterior'
        ]
        
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        string_columns = ['nome_completo', 'setor', 'tipo_acao', 
                         'dat_ano_rffc', 'dat_mes_rffc', 'dat_dia_rffc', 'ticker']
    
        for col in string_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)

        schema = pa.schema([
            ('nome_completo', pa.string()),
            ('setor', pa.string()),
            ('capitalizao_mercado', pa.float64()),
            ('tipo_acao', pa.string()),
            ('preco_mercado', pa.float64()),
            ('abertura', pa.float64()),
            ('minimo_dia', pa.float64()),
            ('maximo_dia', pa.float64()),
            ('delta_variacao_dia_anterior', pa.float64()),
            ('delta_variacao_do_dia', pa.float64()),
            ('dat_ano_rffc', pa.string()),
            ('dat_mes_rffc', pa.string()),
            ('dat_dia_rffc', pa.string()),
            ('ticker', pa.string())
        ])

        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        partition_schema = ds.DirectoryPartitioning(
            pa.schema([
                ('dat_ano_rffc', pa.string()),
                ('dat_mes_rffc', pa.string()),
                ('dat_dia_rffc', pa.string()),
                ('ticker', pa.string())
            ])
        )

        ds.write_dataset(
            data=table,
            base_dir=output_uri,
            format="parquet",
            partitioning=partition_schema,
            existing_data_behavior="overwrite_or_ignore",
            use_threads=True
        )
        
    except Exception as e:
        logger.error(f"Error saving parquet: {str(e)}")
        raise


def get_client(service_name: str) -> Any:
    """"
    Retorna um cliente boto3 para o serviço especificado, reutilizando clientes existentes
    Args:
        service_name (str): Nome do serviço AWS (e.g., 's3', 'glue')
    Returns:
        boto3.client: Cliente boto3 para o serviço especificado
    """
    if service_name not in _BOTO3_CLIENTS:
        _BOTO3_CLIENTS[service_name] = boto3.client(service_name)
    return _BOTO3_CLIENTS[service_name]


def get_last_partition(database: str, table: str) -> str:
    """
    Obtém a última partição de uma tabela Glue com base nas colunas de partição
    Args:
        database (str): Nome do banco de dados Glue
        table (str): Nome da tabela Glue
    Returns:
        str: Última partição no formato (dat_ano_rffc, dat_mes_rffc, dat_dia_rffc) ou None se não houver partições
    """
    client = boto3.client('glue')
    paginator = client.get_paginator('get_partitions')
    response_iterator = paginator.paginate(
        DatabaseName=database,
        TableName=table,
        PaginationConfig={
            'PageSize': 100
        }
    )

    last_partition = None

    for page in response_iterator:
        partitions = page.get('Partitions', [])
        for partition in partitions:
            values = partition.get('Values', [])
            if len(values) >= 3:
                dat_ano_rffc, dat_mes_rffc, dat_dia_rffc = values[0], values[1], values[2]
                if last_partition is None or (dat_ano_rffc, dat_mes_rffc, dat_dia_rffc) > last_partition:
                    last_partition = (dat_ano_rffc, dat_mes_rffc, dat_dia_rffc)

    if last_partition:
        return last_partition
    else:
        return None


def calculate_delta(df: pd.DataFrame, df_last_partition: Optional[pd.DataFrame]) -> pd.DataFrame:
    """ Calcula as colunas de delta no DataFrame
    Args:
        df (pd.DataFrame): DataFrame atual
        df_last_partition (Optional[pd.DataFrame]): DataFrame da última partição (pode ser None)
    Returns:
        pd.DataFrame: DataFrame com as colunas de delta calculadas
    """
    df["delta_variacao_dia_anterior"] = 0.0  # valor default
    df["delta_variacao_do_dia"] = pd.to_numeric(df["maximo_dia"], errors="coerce") - pd.to_numeric(df["minimo_dia"], errors="coerce")
    
    if df_last_partition is not None and not df_last_partition.empty:
        df["preco_mercado"] = pd.to_numeric(df["preco_mercado"], errors="coerce")
        df_last_partition["preco_mercado"] = pd.to_numeric(df_last_partition["preco_mercado"], errors="coerce")
        
        df_merged = df.merge(
            df_last_partition[["ticker", "preco_mercado"]],
            on="ticker",
            how="left",
            suffixes=("", "_anterior")
        )
        
        df["delta_variacao_dia_anterior"] = df_merged["preco_mercado"] - df_merged["preco_mercado_anterior"]
        
        df["delta_variacao_dia_anterior"].fillna(0.0, inplace=True)
    
    return df


def read_data_from_s3(uri_raw: str) -> pd.DataFrame:
    """Lê um arquivo Parquet do S3 e retorna como DataFrame
    Args:
        uri_raw (str): URI do arquivo no S3 (e.g., s3://bucket/path/to/file.parquet)
    Returns:
        pd.DataFrame: DataFrame contendo os dados do arquivo Parquet
    """
    s3_client = get_client("s3")
    bucket_name = uri_raw.replace("s3://", "").split("/", 1)[0]
    object_key = uri_raw.replace("s3://", "").split("/", 1)[1]
    
    response_file = s3_client.get_object(Bucket=bucket_name, Key=object_key)["Body"].read()
    return pd.read_parquet(BytesIO(response_file))


def rename_df(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia as colunas do DataFrame para português
    Args:
        df (pd.DataFrame): DataFrame original
    Returns:
        pd.DataFrame: DataFrame com as colunas renomeadas
    """
    col_renames = {
        "longName"          : "nome_completo", 
        "sector"            : "setor", 
        "marketCap"         : "capitalizao_mercado", 
        "quoteType"         : "tipo_acao",
        "regularMarketPrice": "preco_mercado",
        "open"              : "abertura", 
        "dayLow"            : "minimo_dia",
        "dayHigh"           : "maximo_dia" 
    }
    df.rename(columns=col_renames, inplace=True)
    return df


def create_logical_partitioning(df: pd.DataFrame, database: str, table: str, uri_refined: str) -> None:
    """ Cria particionamento lógico na tabela Glue
    Args:
        df (pd.DataFrame): DataFrame com as colunas de partição
        database (str): Nome do banco de dados Glue
        table (str): Nome da tabela Glue
        uri_refined (str): URI do bucket S3 onde os dados refinados estão armazenados
    """
    glue_client = get_client("glue")
    
    df.drop_duplicates(subset=['dat_ano_rffc', 'dat_mes_rffc', 'dat_dia_rffc', 'ticker'], keep='last', inplace=True)
   
    column_types = {
        'nome_completo': 'string',
        'setor': 'string',
        'capitalizao_mercado': 'double',
        'tipo_acao': 'string',
        'preco_mercado': 'double',
        'abertura': 'double',
        'minimo_dia': 'double',
        'maximo_dia': 'double',
        'delta_variacao_dia_anterior': 'double',
        'delta_variacao_do_dia': 'double'
    }
    for index, item in df.iterrows():
        partition_path = f"{uri_refined}{item['dat_ano_rffc']}/{item['dat_mes_rffc']}/{item['dat_dia_rffc']}/{item['ticker']}/"
        partition_values = [str(item['dat_ano_rffc']), str(item['dat_mes_rffc']), 
                          str(item['dat_dia_rffc']), str(item['ticker'])]
        
        try:
            glue_client.get_partition(
                DatabaseName=database,
                TableName=table,
                PartitionValues=partition_values
            )
            logger.info(f"Partition already exists: {partition_path}")
            continue
        
        except glue_client.exceptions.EntityNotFoundException:
            try:
                glue_client.create_partition(
                    DatabaseName=database,
                    TableName=table,
                    PartitionInput={
                        'Values': partition_values,
                        'StorageDescriptor': {
                            'Columns':  [
                                {'Name': col, 'Type': column_types[col]} 
                                for col in column_types.keys()
                            ],
                            'Location': partition_path,
                            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                            'Compressed': True,
                            'NumberOfBuckets': -1,
                            'SerdeInfo': {
                                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                                'Parameters': {
                                    'serialization.format': '1'
                                }
                            },
                            'BucketColumns': [],
                            'SortColumns': [],
                            'Parameters': {
                                'parquet.compression': 'SNAPPY'
                            },
                            'StoredAsSubDirectories': False
                        },
                        'Parameters': {}
                    }
                )
                logger.info(f"Created partition: {partition_path}")
                    
            except Exception as e:
                logger.error(f"Error creating partition {partition_path}: {str(e)}")
                continue

    print("particões criadas com sucesso")


def busca_ultimos_dados(uri_refined: str, last_partition: Tuple[str, str, str]) -> pd.DataFrame:
    """ Busca os dados da última partição na tabela Glue
    Args:
        uri_refined (str): URI do bucket S3 onde os dados refinados estão armazenados
        last_partition (Tuple[str, str, str]): Última partição no formato (dat_ano_rffc, dat_mes_rffc, dat_dia_rffc)
    Returns:
        pd.DataFrame: DataFrame contendo os dados da última partição
    """
    partition_schema = pa.schema([
        ("dat_ano_rffc", pa.string()),
        ("dat_mes_rffc", pa.string()),
        ("dat_dia_rffc", pa.string()),
        ("ticker", pa.string())
    ])
    
    dataset = ds.dataset(
        uri_refined,
        format="parquet",
        partitioning=ds.DirectoryPartitioning(partition_schema)
    )
    
    filter_expr = (
        (ds.field("dat_ano_rffc") == last_partition[0]) &
        (ds.field("dat_mes_rffc") == last_partition[1]) &
        (ds.field("dat_dia_rffc") == last_partition[2])
    )
    
    return dataset.to_table(filter=filter_expr).to_pandas()


def cria_uri_refined(output_db: str, output_table: str) -> str:
    """Cria a URI do bucket S3 onde os dados refinados serão armazenados
    Args:
        output_db (str): Nome do banco de dados Glue
        output_table (str): Nome da tabela Glue
    Returns:
        str: URI do bucket S3 onde os dados refinados serão armazenados
    """
    glue_client = get_client("glue")
    uri_refined = glue_client.get_table(DatabaseName=output_db, Name=output_table)['Table']['StorageDescriptor']['Location']
    return uri_refined


def extract_date_from_s3_path(s3_path: str) -> tuple:
    """ Extrai o ano, mês e dia de uma URI S3 no formato esperado
    Args:
        s3_path (str): URI do S3
    Returns:
        tuple: (year, month, day) extraídos da URI
    """
    try:
        parts = s3_path.split('/')
        
        year = next(part.split('=')[1] for part in parts if part.startswith('year='))
        month = next(part.split('=')[1] for part in parts if part.startswith('month='))
        day = next(part.split('=')[1] for part in parts if part.startswith('day='))
        
        return year, month, day
        
    except Exception as e:
        logger.error(f"Error extracting date from path {s3_path}: {str(e)}")
        raise


def main():
    args = getResolvedOptions(sys.argv, ['DT_REF', 'ENV', 'URI_OBJECT_RAW',  'OUTPUT_DATABASE', 'OUTPUT_TABLE'])
    uri_raw = args['URI_OBJECT_RAW']
    output_db = args['OUTPUT_DATABASE']
    output_table = args['OUTPUT_TABLE']    
    
    if args['DT_REF'].lower() != "auto":
        year, month, day = extract_date_from_s3_path(uri_raw)
    else:
        year, month, day = date.today().strftime("%Y"), date.today().strftime("%m"), date.today().strftime("%d")
    
    uri_refined = cria_uri_refined(output_db, output_table)
    last_partition = get_last_partition(output_db, output_table)

    df_file = read_data_from_s3(uri_raw)
    df_file = rename_df(df_file)
    
    df_dia_anterior = busca_ultimos_dados(uri_refined, last_partition) if last_partition else None
    df_delta = calculate_delta(df_file, df_dia_anterior)
    
    df_prepared = prepare_partition_columns(df_delta, year, month, day)
    
    save_df_to_s3_parquet(df_prepared, uri_refined)
    
    logical = df_prepared[['dat_ano_rffc', 'dat_mes_rffc', 'dat_dia_rffc', 'ticker']]
    create_logical_partitioning(logical, output_db, output_table, uri_refined)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Erro ao processar o job:")
        traceback.print_exc()
        raise
