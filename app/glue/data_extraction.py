
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
    df['dat_ano_rffc'] = year
    df['dat_mes_rffc'] = month
    df['dat_dia_rffc'] = day
    df['ticker'] = df['ticker'].astype(str)
    return df

def save_df_to_s3_parquet(df, output_uri):
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        data=table,
        base_dir=output_uri,
        format="parquet",
        partitioning=['dat_ano_rffc', 'dat_mes_rffc', 'dat_dia_rffc', 'ticker'],   # ajuste suas colunas
        existing_data_behavior="overwrite_or_ignore",  # sobrescreve por arquivo; não apaga pastas antigas
    )

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
    last_location = None

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
        return last_partition + (last_location,)
    else:
        return None

def calculate_delta(df: pd.DataFrame, df_last_partition: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Calcula os deltas de variação do dia e variação em relação ao dia anterior
    
    Args:
        df: DataFrame atual com os dados do dia
        df_last_partition: DataFrame opcional com os dados da última partição
    
    Returns:
        DataFrame com as novas colunas calculadas
    """
    # Calcula delta_variacao_do_dia
    df["delta_variacao_do_dia"] = pd.to_numeric(df["dayHigh"], errors="coerce") - pd.to_numeric(df["dayLow"], errors="coerce")
    
    # Calcula delta_variacao_dia_anterior
    df["delta_variacao_dia_anterior"] = 0.0  # valor default
    
    if df_last_partition is not None and not df_last_partition.empty:
        # Assegura que as colunas de preço são numéricas
        df["regularMarketPrice"] = pd.to_numeric(df["regularMarketPrice"], errors="coerce")
        df_last_partition["regularMarketPrice"] = pd.to_numeric(df_last_partition["regularMarketPrice"], errors="coerce")
        
        # Cria um merge dos DataFrames usando o ticker como chave
        df_merged = df.merge(
            df_last_partition[["ticker", "regularMarketPrice"]],
            on="ticker",
            how="left",
            suffixes=("", "_anterior")
        )
        
        # Calcula a variação entre os dias
        df["delta_variacao_dia_anterior"] = df_merged["regularMarketPrice"] - df_merged["regularMarketPrice_anterior"]
        
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
    col_renames = {"longName": "nome_completo", "sector": "setor", "capitalizao_mercado": "marketCap", "tipo_acao": "quoteType", "preco_mercado": "regularMarketPrice", "abertura": "open", "maximo_dia": "dayHigh", "minimo_dia": "dayLow" }
    df.rename(columns=col_renames, inplace=True)
    return df

def main():
    args = getResolvedOptions(sys.argv, ['DT_REF', 'ENV', 'URI_OBJECT_RAW',  'OUTPUT_DATABASE', 'OUTPUT_TABLE'])
    uri_raw = args['URI_OBJECT_RAW']
    output_db = args['OUTPUT_DATABASE']
    output_table = args['OUTPUT_TABLE']

    logger.info("Iniciando processamento", extra={
        "input_path": uri_raw,
        "glue_table": f"{output_db}.{output_table}"
    })
    
    glue_client = get_client("glue")
    uri_refined = glue_client.get_table(DatabaseName=output_db, Name=output_table)['Table']['StorageDescriptor']['Location']
    
    df_file = read_data_from_s3(uri_raw)
    last_partition = get_last_partition(output_db, output_table)
    s3_file_path_last_partition = last_partition[3] if last_partition else None
    
    df_last_partition = read_data_from_s3(s3_file_path_last_partition) if s3_file_path_last_partition else None
    
    df_delta = calculate_delta(df_file, df_last_partition)
    df_renamed = rename_df(df_delta)
    
    year, month, day = date.today().strftime("%Y"), date.today().strftime("%m"), date.today().strftime("%d")
    df_prepared = prepare_partition_columns(df_renamed, year, month, day)
    
    df_prepared.head(10)
    print(df_prepared.head(10))
    save_df_to_s3_parquet(df_prepared, uri_refined)
    
    
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Erro ao processar o job:")
        traceback.print_exc()
        raise