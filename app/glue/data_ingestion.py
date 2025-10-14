
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Standard library imports
import sys
import logging
import traceback
from io import BytesIO
from uuid import uuid4
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, date
import pyarrow as pa
import pyarrow.dataset as ds

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Third-party imports
import boto3
import pandas as pd
import awswrangler as wr

# Global clients cache
_BOTO3_CLIENTS = {}

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
# -----------------------------------------------------------------------------
# Glue args compatibility (works locally and inside Glue)
# -----------------------------------------------------------------------------
def prepare_partition_columns(df: pd.DataFrame, year, month, day) -> pd.DataFrame:
    df['dat_ano_rffc'] = str(year)
    df['dat_mes_rffc'] = str(month)
    df['dat_dia_rffc'] = str(day)
    df['ticker'] = df['ticker'].astype(str)
    return df

def save_df_to_s3_parquet(df, output_uri):
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
        # Define schema explícito para garantir tipos corretos
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

        # Converte DataFrame para Table com schema explícito
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

        # Define particionamento
        partition_schema = ds.DirectoryPartitioning(
            pa.schema([
                ('dat_ano_rffc', pa.string()),
                ('dat_mes_rffc', pa.string()),
                ('dat_dia_rffc', pa.string()),
                ('ticker', pa.string())
            ])
        )

        # Salva dataset
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

def get_client(service_name: str):
    """Get cached boto3 client or create new one"""
    if service_name not in _BOTO3_CLIENTS:
        _BOTO3_CLIENTS[service_name] = boto3.client(service_name)
    return _BOTO3_CLIENTS[service_name]

def get_last_partition(database: str, table: str) -> str:
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
                    last_location = partition.get('StorageDescriptor', {}).get('Location', '')

    if last_partition:
        return last_partition
    else:
        return None

def calculate_delta(df: pd.DataFrame, df_last_partition: Optional[pd.DataFrame]) -> pd.DataFrame:
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
        
        # Calcula a variação entre os dias
        df["delta_variacao_dia_anterior"] = df_merged["preco_mercado"] - df_merged["preco_mercado_anterior"]
        
        # Preenche valores NaN com 0
        df["delta_variacao_dia_anterior"].fillna(0.0, inplace=True)
    
    return df

def read_data_from_s3(uri_raw: str) -> pd.DataFrame:
    s3_client = get_client("s3")
    bucket_name = uri_raw.replace("s3://", "").split("/", 1)[0]
    object_key = uri_raw.replace("s3://", "").split("/", 1)[1]
    
    response_file = s3_client.get_object(Bucket=bucket_name, Key=object_key)["Body"].read()
    return pd.read_parquet(BytesIO(response_file))

def rename_df(df: pd.DataFrame) -> pd.DataFrame:
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

def create_logical_partitioning(df: pd.DataFrame, database, table, uri_refined):
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
            # Try to get existing partition
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
    
def busca_ultimos_dados(uri_refined, last_partition):
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

def cria_uri_refined(output_db, output_table):
    glue_client = get_client("glue")
    uri_refined = glue_client.get_table(DatabaseName=output_db, Name=output_table)['Table']['StorageDescriptor']['Location']
    return uri_refined
        
def main():
    args = getResolvedOptions(sys.argv, ['DT_REF', 'ENV', 'URI_OBJECT_RAW',  'OUTPUT_DATABASE', 'OUTPUT_TABLE'])
    uri_raw = args['URI_OBJECT_RAW']
    output_db = args['OUTPUT_DATABASE']
    output_table = args['OUTPUT_TABLE']    
    
    uri_refined = cria_uri_refined(output_db, output_table) # caminho do refined
    last_partition = get_last_partition(output_db, output_table) # pegando a ultima partição

    df_file = read_data_from_s3(uri_raw) # lendo o arquivo do cru do yfinance
    df_file = rename_df(df_file) # definindo nomes em portugues
    
    df_dia_anterior = busca_ultimos_dados(uri_refined, last_partition) if last_partition else None
    df_delta = calculate_delta(df_file, df_dia_anterior) # calculando os deltas
    
    year, month, day = date.today().strftime("%Y"), date.today().strftime("%m"), date.today().strftime("%d")
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