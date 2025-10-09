
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from io import BytesIO
import re
import traceback
from uuid import uuid4
from typing import Dict, Any, List, Tuple, Optional

import boto3
import pandas as pd
import awswrangler as wr

# -----------------------------------------------------------------------------
# Glue args compatibility (works locally and inside Glue)
# -----------------------------------------------------------------------------
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
# Partition keys & helpers
# -----------------------------------------------------------------------------
EXPECTED_PARTITION_KEYS = [
    {"Name": "ano", "Type": "int"},
    {"Name": "mes", "Type": "int"},
    {"Name": "dia", "Type": "int"},
    {"Name": "ticker", "Type": "string"},
]
EXPECTED_PK_NAMES = [k["Name"] for k in EXPECTED_PARTITION_KEYS]

def ensure_database(db_name: str):
    glue_client = boto3.client("glue")
    try:
        glue_client.get_database(Name=db_name)
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(DatabaseInput={"Name": db_name})

def pandas_dtype_to_glue(dtype) -> str:
    if pd.api.types.is_integer_dtype(dtype): return "bigint"
    if pd.api.types.is_float_dtype(dtype):   return "double"
    if pd.api.types.is_bool_dtype(dtype):    return "boolean"
    if pd.api.types.is_datetime64_any_dtype(dtype): return "timestamp"
    return "string"

def ensure_table_4p_from_df(db_name: str, table_name: str, s3_location: str, df_: pd.DataFrame):
    """
    Garante a tabela no Glue com 4 partições (ano, mes, dia, ticker) nessa ordem
    e com StorageDescriptor.Location == s3_location.
    """
    glue_client = boto3.client("glue")
    target_loc = s3_location.rstrip("/")
    part_cols = set(EXPECTED_PK_NAMES)
    storage_descriptor = {
        "Columns": [{"Name": c, "Type": pandas_dtype_to_glue(df_[c].dtype)} for c in df_.columns if c not in part_cols],
        "Location": target_loc,
        "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                      "Parameters": {"serialization.format": "1"}},
        "Parameters": {"classification": "parquet"},
    }
    try:
        t = glue_client.get_table(DatabaseName=db_name, Name=table_name)["Table"]
        current_keys = [k["Name"] for k in t.get("PartitionKeys", [])]
        current_loc = t["StorageDescriptor"].get("Location", "").rstrip("/")
        if current_keys != EXPECTED_PK_NAMES:
            glue_client.delete_table(DatabaseName=db_name, Name=table_name)
            glue_client.create_table(DatabaseName=db_name, TableInput={
                "Name": table_name, "TableType": "EXTERNAL_TABLE",
                "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
                "PartitionKeys": EXPECTED_PARTITION_KEYS, "StorageDescriptor": storage_descriptor})
        elif current_loc != target_loc:
            sd = t["StorageDescriptor"]; sd["Location"] = target_loc; sd["Columns"] = storage_descriptor["Columns"]
            glue_client.update_table(DatabaseName=db_name, TableInput={
                "Name": table_name, "TableType": t.get("TableType", "EXTERNAL_TABLE"),
                "Parameters": t.get("Parameters", {"EXTERNAL": "TRUE", "classification": "parquet"}),
                "PartitionKeys": EXPECTED_PARTITION_KEYS, "StorageDescriptor": sd})
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(DatabaseName=db_name, TableInput={
            "Name": table_name, "TableType": "EXTERNAL_TABLE",
            "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
            "PartitionKeys": EXPECTED_PARTITION_KEYS, "StorageDescriptor": storage_descriptor})

def update_glue_table_schema_from_df(refined_db: str, refined_table: str, refined_path: str, df_sample: pd.DataFrame) -> None:
    """
    Atualiza o schema (Columns) da tabela usando o DF enriquecido desta execução.
    """
    glue = boto3.client("glue")
    t = glue.get_table(DatabaseName=refined_db, Name=refined_table)["Table"]
    sd = t["StorageDescriptor"]
    part_cols = set(EXPECTED_PK_NAMES)
    def _map(dtype):
        if pd.api.types.is_float_dtype(dtype): return "double"
        if pd.api.types.is_integer_dtype(dtype): return "bigint"
        if pd.api.types.is_bool_dtype(dtype): return "boolean"
        if pd.api.types.is_datetime64_any_dtype(dtype): return "timestamp"
        return "string"
    columns = [{"Name": c, "Type": _map(df_sample[c].dtype)} for c in df_sample.columns if c not in part_cols]
    sd["Columns"] = columns
    table_input = {"Name": refined_table, "TableType": t.get("TableType", "EXTERNAL_TABLE"),
                   "Parameters": t.get("Parameters", {"EXTERNAL":"TRUE","classification":"parquet"}),
                   "PartitionKeys": t.get("PartitionKeys") or EXPECTED_PARTITION_KEYS, "StorageDescriptor": sd}
    glue.update_table(DatabaseName=refined_db, TableInput=table_input)
    print("[SCHEMA][OK] Atualizado com colunas:", [c["Name"] for c in columns])

def compute_enrichments(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria/atualiza colunas calculadas:
      - Delta_Valor
      - QtdAcoesDia
      - QtdAcoesSetorDia (se 'setor' existir)
      - MediaMovel7 (MM7) por ticker considerando últimos 7 dias do próprio DF (se houver)
    """
    # Renomes padrão do projeto (idempotente)
    if "longName" in df.columns and "nomecompleto" not in df.columns:
        df = df.rename(columns={"longName": "nomecompleto"})
    if "sector" in df.columns and "setor" not in df.columns:
        df = df.rename(columns={"sector": "setor"})

    # Delta_Valor
    if {"dayHigh","dayLow"} <= set(df.columns):
        df["Delta_Valor"] = (pd.to_numeric(df["dayHigh"], errors="coerce") -
                             pd.to_numeric(df["dayLow"], errors="coerce"))

    # Contagens
    if {"ano","mes","dia","ticker"} <= set(df.columns):
        df["ticker"] = df["ticker"].astype(str)
        cnt = (df.groupby(["ano","mes","dia"])["ticker"].nunique()
                 .reset_index().rename(columns={"ticker":"QtdAcoesDia"}))
        df = df.merge(cnt, on=["ano","mes","dia"], how="left")

    if {"ano","mes","dia","ticker","setor"} <= set(df.columns):
        cset = (df.groupby(["ano","mes","dia","setor"])["ticker"].nunique()
                  .reset_index().rename(columns={"ticker":"QtdAcoesSetorDia"}))
        df = df.merge(cset, on=["ano","mes","dia","setor"], how="left")

    # MM7 (com base nas linhas disponíveis no DF; ideal se você concatenar vários dias antes)
    if {"ano","mes","dia","ticker"} <= set(df.columns):
        df = df.copy()
        df["data"] = pd.to_datetime(df[["ano","mes","dia"]].astype(str).agg("-".join, axis=1), errors="coerce")
        if "regularmarketprice" in df.columns:
            price_col = "regularmarketprice"
        elif "regularMarketPrice" in df.columns:
            price_col = "regularMarketPrice"
        else:
            price_col = None
        if price_col:
            df[price_col] = pd.to_numeric(df[price_col], errors="coerce")
            df = df.sort_values(["ticker","data"])
            df["MediaMovel7"] = df.groupby("ticker")[price_col].transform(lambda s: s.rolling(7, min_periods=1).mean())
    return df

def sanitize_partitions(df: pd.DataFrame) -> pd.DataFrame:
    for c in ["ano","mes","dia"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "ticker" not in df.columns:
        df["ticker"] = "UNKNOWN"
    df["ticker"] = (df["ticker"].astype(str).str.strip().str.upper()
                    .replace({"": pd.NA, "NAN": pd.NA})
                    .str.replace(r"[/=\\]", "-", regex=True)
                    .str.replace(r"[^A-Z0-9._-]", "_", regex=True))
    df = df.dropna(subset=["ano","mes","dia","ticker"]).copy()
    df["ano"] = df["ano"].astype(int); df["mes"] = df["mes"].astype(int); df["dia"] = df["dia"].astype(int)
    return df

def write_grouped_partitions(df: pd.DataFrame, refined_path: str) -> List[Dict[str, Any]]:
    cols_part = ["ano","mes","dia","ticker"]
    touched: List[Dict[str, Any]] = []
    for (a, m, d, t), g in df.groupby(cols_part, dropna=False):
        part_path = f"{refined_path.rstrip('/')}/ano={a:04d}/mes={m:02d}/dia={d:02d}/ticker={t}/"
        obj = f"{part_path}part-{uuid4().hex}.parquet"
        wr.s3.to_parquet(df=g.drop(columns=[c for c in cols_part if c in g.columns], errors="ignore"),
                         path=obj, dataset=False, schema_evolution=True)
        touched.append({"ano": a, "mes": m, "dia": d, "ticker": str(t)})
    return touched

def register_partitions_boto3(refined_db: str, refined_table: str, refined_path: str, particoes: List[Dict[str, Any]]) -> int:
    glue = boto3.client("glue")
    base_sd = glue.get_table(DatabaseName=refined_db, Name=refined_table)["Table"]["StorageDescriptor"]
    def _to_str(v): return str(int(v)) if isinstance(v,(int,float)) and not isinstance(v,bool) else str(v)
    def make_pi(p):
        loc = f"{refined_path.rstrip('/')}/ano={int(p['ano']):04d}/mes={int(p['mes']):02d}/dia={int(p['dia']):02d}/ticker={p['ticker']}/"
        return {"Values":[_to_str(p[k]) for k in ["ano","mes","dia","ticker"]],
                "StorageDescriptor":{"Columns": base_sd.get("Columns", []), "Location": loc,
                    "InputFormat": base_sd.get("InputFormat"), "OutputFormat": base_sd.get("OutputFormat"),
                    "SerdeInfo": base_sd.get("SerdeInfo"), "Compressed": base_sd.get("Compressed", False),
                    "NumberOfBuckets": base_sd.get("NumberOfBuckets", 0), "StoredAsSubDirectories": base_sd.get("StoredAsSubDirectories", False),
                    "Parameters": base_sd.get("Parameters", {})}, "Parameters": {}}
    # dedup
    uniq = {(p["ano"],p["mes"],p["dia"],p["ticker"]):p for p in particoes}
    particoes = list(uniq.values())
    created = 0; batch = []
    for p in particoes:
        batch.append(make_pi(p))
        if len(batch)==100:
            resp = glue.batch_create_partition(DatabaseName=refined_db, TableName=refined_table, PartitionInputList=batch)
            if resp.get("Errors"): print("[WARN] Erros:", resp["Errors"][:5])
            created += len(batch) - len(resp.get("Errors", [])); batch = []
    if batch:
        resp = glue.batch_create_partition(DatabaseName=refined_db, TableName=refined_table, PartitionInputList=batch)
        if resp.get("Errors"): print("[WARN] Erros:", resp["Errors"][:5])
        created += len(batch) - len(resp.get("Errors", []))
    return created

from datetime import datetime, date
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

def discover_today_key(s3_client, bucket: str, object_key: str) -> List[str]:
    if object_key.endswith("finance.parquet"):
        return [object_key]
    if ZoneInfo:
        hoje = datetime.now(ZoneInfo("America/Sao_Paulo")).date()
    else:
        hoje = datetime.utcnow().date()
    candidate = f"{object_key.rstrip('/')}/ano={hoje.year:04d}/mes={hoje.month:02d}/dia={hoje.day:02d}/finance.parquet"
    head = s3_client.list_objects_v2(Bucket=bucket, Prefix=candidate)
    if head.get("KeyCount", 0) > 0:
        return [candidate]
    print(f"[INFO] Nenhum parquet para o dia de hoje em s3://{bucket}/{candidate}.")
    return []

def main():
    args = getResolvedOptions(sys.argv, ['LOCATION_PATH','REFINED_BUCKET','REFINED_PREFIX','REFINED_DB','REFINED_TABLE'])
    s3_path = args['LOCATION_PATH']
    bucket_name = s3_path.replace("s3://", "").split("/", 1)[0]
    object_key = s3_path.replace("s3://", "").split("/", 1)[1]

    refined_bucket = re.sub(r'^s3://','', args['REFINED_BUCKET']).split('/')[0]
    refined_prefix = args['REFINED_PREFIX'].strip('/') + '/'
    refined_db = args['REFINED_DB']; refined_table = args['REFINED_TABLE']
    refined_path = f"s3://{refined_bucket}/{refined_prefix}"

    print(f"[INFO] Input       = s3://{bucket_name}/{object_key}")
    print(f"[INFO] Refined     = {refined_path}")
    print(f"[INFO] Glue table  = {refined_db}.{refined_table}")

    s3 = boto3.client("s3")
    keys = discover_today_key(s3, bucket_name, object_key)
    if not keys:
        print("[INFO] Nada a processar hoje. Saindo OK."); sys.exit(0)

    # --- cria/alinha a tabela usando o DF ENRIQUECIDO de hoje ---
    today_key = keys[0]
    body0 = s3.get_object(Bucket=bucket_name, Key=today_key)["Body"].read()
    df0 = pd.read_parquet(BytesIO(body0))
    m0 = re.search(r"ano=(\d{4})/mes=(\d{1,2})/dia=(\d{1,2})", today_key)
    if m0:
        df0["ano"], df0["mes"], df0["dia"] = map(int, m0.groups())
    if "ticker" not in df0.columns: df0["ticker"] = pd.Series(dtype="string")
    df0_enriched = compute_enrichments(df0.copy())
    ensure_database(refined_db)
    ensure_table_4p_from_df(refined_db, refined_table, refined_path, df0_enriched)

    # --- processa somente hoje ---
    particoes_escritas: List[Dict[str, Any]] = []
    last_df: Optional[pd.DataFrame] = None

    for k in keys:
        print(f"[RUN] {k}")
        body = s3.get_object(Bucket=bucket_name, Key=k)["Body"].read()
        df = pd.read_parquet(BytesIO(body))
        m = re.search(r"ano=(\d{4})/mes=(\d{1,2})/dia=(\d{1,2})", k)
        if not m: 
            print("[WARN] sem ano/mes/dia no caminho, pulando."); 
            continue
        a, mth, d = map(int, m.groups())
        df["ano"], df["mes"], df["dia"] = a, mth, d

        df = compute_enrichments(df)
        df = sanitize_partitions(df)
        touched = write_grouped_partitions(df, refined_path)
        particoes_escritas.extend(touched)
        last_df = df.copy()

    created = register_partitions_boto3(refined_db, refined_table, refined_path, particoes_escritas)
    print(f"[OK] Partições registradas (hoje): {created}/{len(particoes_escritas)}")

    if last_df is not None:
        update_glue_table_schema_from_df(refined_db, refined_table, refined_path, last_df)

    print("[DONE] Concluído (somente hoje).")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Erro ao processar o job:")
        traceback.print_exc()
        raise
