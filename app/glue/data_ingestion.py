#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Glue ETL – B3 Batch (All Days) com agregações numéricas
- Varre um arquivo específico OU todos os dias de um prefixo (LOCATION_PATH).
- Normaliza e escreve em refined/ano=YYYY/mes=MM/dia=DD/ticker=XXX/part-*.parquet (sem dataset).
- Registra as partições no Glue Catalog via boto3.
- Atualiza o schema (Columns) da tabela no final, a partir de um parquet real de refined.
- Requisitos do item 5 atendidos:
    * Renomear 2 colunas: longName->NomeCompleto, sector->Setor
    * Agregações numéricas (ex.: soma de Volume por dia e por setor; média de preço por setor no dia)
    * Cálculo de média móvel de 7 dias por ticker (MM7) usando regularMarketPrice
    * Métrica simples: Delta_Valor = dayHigh - dayLow
"""

import sys
from io import BytesIO
import re
import traceback
from uuid import uuid4
from typing import Dict, Any, List, Tuple

import boto3
import pandas as pd
import numpy as np
import awswrangler as wr

# -----------------------------------------------------------------------------
# Args (Glue) - compat com execução local se awsglue não existir
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
# Constantes de partição
# -----------------------------------------------------------------------------
EXPECTED_PARTITION_KEYS = [
    {"Name": "ano", "Type": "int"},
    {"Name": "mes", "Type": "int"},
    {"Name": "dia", "Type": "int"},
    {"Name": "ticker", "Type": "string"},
]
EXPECTED_PK_NAMES = [k["Name"] for k in EXPECTED_PARTITION_KEYS]

# -----------------------------------------------------------------------------
# Helpers de Glue Catalog
# -----------------------------------------------------------------------------
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
    Se existir com outra ordem, recria. Se Location divergir, faz update.
    """
    glue_client = boto3.client("glue")
    target_loc = s3_location.rstrip("/")

    def build_cols():
        part_cols = set(EXPECTED_PK_NAMES)
        return [
            {"Name": c, "Type": pandas_dtype_to_glue(df_[c].dtype)}
            for c in df_.columns if c not in part_cols
        ]

    def create_table():
        storage_descriptor = {
            "Columns": build_cols(),
            "Location": target_loc,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Parameters": {"classification": "parquet"},
        }
        glue_client.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": table_name,
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"EXTERNAL": "TRUE", "classification": "parquet"},
                "PartitionKeys": EXPECTED_PARTITION_KEYS,
                "StorageDescriptor": storage_descriptor,
            },
        )
        print(f"[GLUE] Tabela criada: {db_name}.{table_name} @ {target_loc}")

    try:
        t = glue_client.get_table(DatabaseName=db_name, Name=table_name)["Table"]
        current_keys = [k["Name"] for k in t.get("PartitionKeys", [])]
        current_loc = t["StorageDescriptor"].get("Location", "").rstrip("/")

        if current_keys != EXPECTED_PK_NAMES:
            print(f"[GLUE][FIX] PartitionKeys {current_keys} != {EXPECTED_PK_NAMES}. Recriando...")
            glue_client.delete_table(DatabaseName=db_name, Name=table_name)
            create_table()
            return

        if current_loc != target_loc:
            print(f"[GLUE][UPDATE] Location {current_loc} -> {target_loc}")
            sd = t["StorageDescriptor"]
            sd["Location"] = target_loc
            sd["Columns"] = build_cols()
            table_input = {
                "Name": table_name,
                "TableType": t.get("TableType", "EXTERNAL_TABLE"),
                "Parameters": t.get("Parameters", {"EXTERNAL": "TRUE", "classification": "parquet"}),
                "PartitionKeys": EXPECTED_PARTITION_KEYS,
                "StorageDescriptor": sd,
            }
            glue_client.update_table(DatabaseName=db_name, TableInput=table_input)
            print(f"[GLUE] Location atualizado.")
        else:
            print(f"[GLUE] Tabela OK: keys={current_keys} location={current_loc}")
    except glue_client.exceptions.EntityNotFoundException:
        create_table()

def get_or_fix_partition_keys(glue_client, db: str, table: str, refined_path: str, df_for_schema: pd.DataFrame) -> List[str]:
    """
    Retorna os nomes das PartitionKeys. Se estiverem ausentes ou divergentes, recria/atualiza a tabela.
    """
    t = glue_client.get_table(DatabaseName=db, Name=table)["Table"]
    current_keys = [k["Name"] for k in t.get("PartitionKeys", [])]
    if current_keys != EXPECTED_PK_NAMES:
        print(f"[GLUE][WARN] PartitionKeys ausentes ou incorretas: {current_keys}. Ajustando tabela...")
        ensure_table_4p_from_df(db, table, refined_path, df_for_schema)
        t = glue_client.get_table(DatabaseName=db, Name=table)["Table"]
        current_keys = [k["Name"] for k in t.get("PartitionKeys", [])]
    if current_keys != EXPECTED_PK_NAMES:
        raise RuntimeError(f"PartitionKeys continuam incorretas após ajuste: {current_keys}")
    return current_keys

# -----------------------------------------------------------------------------
# Atualização do SCHEMA no Glue a partir de um Parquet de exemplo
# -----------------------------------------------------------------------------
def update_glue_table_schema_from_sample(refined_db: str, refined_table: str, refined_path: str) -> None:
    """
    Lê um parquet dentro de refined_path e atualiza o Schema (Columns) da tabela,
    mantendo as PartitionKeys e o restante do StorageDescriptor.
    """
    print("[SCHEMA] Atualizando schema a partir de um parquet de exemplo...")
    s3 = boto3.client("s3")
    glue = boto3.client("glue")

    # 1) encontra um parquet
    m = re.match(r"s3://([^/]+)/(.+)", refined_path)
    assert m, f"REFINED_PATH inválido: {refined_path}"
    bucket, prefix = m.group(1), m.group(2)

    paginator = s3.get_paginator("list_objects_v2")
    key_sample = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in page.get("Contents", []):
            if c["Key"].endswith(".parquet"):
                key_sample = c["Key"]
                break
        if key_sample:
            break
    if not key_sample:
        print("[SCHEMA][WARN] Nenhum parquet encontrado em refined; mantendo schema atual.")
        return

    body = s3.get_object(Bucket=bucket, Key=key_sample)["Body"].read()
    df   = pd.read_parquet(BytesIO(body))

    # 2) mapeia pandas -> Glue (ignorando colunas de partição)
    part_cols = set(EXPECTED_PK_NAMES)
    columns = [{"Name": c, "Type": pandas_dtype_to_glue(df[c].dtype)}
               for c in df.columns if c not in part_cols]

    # 3) lê a tabela, mantém PartitionKeys (ou seta se ausentes), e atualiza Columns
    t = glue.get_table(DatabaseName=refined_db, Name=refined_table)["Table"]
    sd = t["StorageDescriptor"]
    sd["Columns"] = columns

    pkeys = t.get("PartitionKeys") or EXPECTED_PARTITION_KEYS  # fallback se vier vazio
    table_input = {
        "Name": refined_table,
        "TableType": t.get("TableType", "EXTERNAL_TABLE"),
        "Parameters": t.get("Parameters", {"EXTERNAL":"TRUE","classification":"parquet"}),
        "PartitionKeys": pkeys,
        "StorageDescriptor": sd,
    }

    glue.update_table(DatabaseName=refined_db, TableInput=table_input)
    print("[SCHEMA][OK] Schema atualizado com colunas:", [c["Name"] for c in columns])

# -----------------------------------------------------------------------------
# Descoberta de arquivos: 1 arquivo ou todos os dias de um prefixo
# -----------------------------------------------------------------------------
def discover_finance_keys(s3_client, bucket: str, object_key: str) -> List[str]:
    """
    Se object_key for um arquivo (termina com finance.parquet), retorna [object_key].
    Caso contrário, varre o prefixo e retorna todos os .../ano=YYYY/mes=MM/dia=DD/finance.parquet.
    """
    if object_key.endswith("finance.parquet"):
        return [object_key]

    prefix = object_key.rstrip("/") + "/"
    paginator = s3_client.get_paginator("list_objects_v2")
    keys: List[str] = []
    pat = re.compile(r"ano=\d{4}/mes=\d{1,2}/dia=\d{1,2}/finance\.parquet$")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for c in page.get("Contents", []):
            k = c["Key"]
            if pat.search(k):
                keys.append(k)

    def key_sort(k: str):
        m = re.search(r"ano=(\d{4})/mes=(\d{1,2})/dia=(\d{1,2})", k)
        return tuple(int(x) for x in m.groups()) if m else (0, 0, 0)

    return sorted(keys, key=key_sort)

def extract_ymd_from_key(key: str) -> Tuple[int,int,int]:
    m = re.search(r"ano=(\d{4})/mes=(\d{1,2})/dia=(\d{1,2})", key)
    return tuple(map(int, m.groups())) if m else None

# -----------------------------------------------------------------------------
# MM7 – Calcula média móvel 7 dias por ticker usando todos os arquivos listados
# -----------------------------------------------------------------------------
def build_mm7_from_keys(s3_client, bucket_name: str, finance_keys: List[str]) -> pd.DataFrame:
    """
    Retorna um DataFrame (ticker, ano, mes, dia, MM7) com a média móvel de 7 dias
    calculada sobre 'regularMarketPrice' para todos os arquivos informados.
    """
    rows = []
    # lê somente colunas necessárias para ser leve
    for k in finance_keys:
        ymd = extract_ymd_from_key(k)
        if not ymd:
            continue
        try:
            body = s3_client.get_object(Bucket=bucket_name, Key=k)["Body"].read()
            dfh = pd.read_parquet(BytesIO(body), columns=["ticker","regularMarketPrice"])
        except Exception:
            continue
        dfh["ano"], dfh["mes"], dfh["dia"] = ymd
        rows.append(dfh)

    if not rows:
        return pd.DataFrame(columns=["ticker","ano","mes","dia","MM7"])

    df_mm = pd.concat(rows, ignore_index=True)
    # normaliza
    df_mm["regularMarketPrice"] = pd.to_numeric(df_mm["regularMarketPrice"], errors="coerce")
    df_mm["ticker"] = (
        df_mm["ticker"].astype(str).str.strip().str.upper()
            .replace({"": pd.NA, "NAN": pd.NA})
            .str.replace(r"[/=\\]", "-", regex=True)
            .str.replace(r"[^A-Z0-9._-]", "_", regex=True)
    )
    df_mm = df_mm.dropna(subset=["ticker","regularMarketPrice"]).copy()

    # cria data e ordena
    df_mm["data"] = pd.to_datetime(
        df_mm[["ano","mes","dia"]].astype(str).agg("-".join, axis=1),
        format="%Y-%m-%d", errors="coerce"
    )
    df_mm = df_mm.dropna(subset=["data"]).sort_values(["ticker","data"])

    # MM7 por ticker
    df_mm["MM7"] = (
        df_mm.groupby("ticker")["regularMarketPrice"]
             .transform(lambda s: s.rolling(window=7, min_periods=1).mean())
    )

    # reduz para uma linha por (ticker, data)
    df_mm_day = (
        df_mm[["ticker","ano","mes","dia","MM7"]]
        .dropna(subset=["MM7"])
        .drop_duplicates(subset=["ticker","ano","mes","dia"], keep="last")
    )
    return df_mm_day

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main():
    args = getResolvedOptions(
        sys.argv,
        ['LOCATION_PATH', 'REFINED_BUCKET', 'REFINED_PREFIX', 'REFINED_DB', 'REFINED_TABLE']
    )

    # INPUT
    s3_file_path = args['LOCATION_PATH']  # pode ser arquivo ou prefixo
    bucket_name = s3_file_path.replace("s3://", "").split("/", 1)[0]
    object_key  = s3_file_path.replace("s3://", "").split("/", 1)[1]

    # OUTPUT / CATALOGO
    refined_bucket = re.sub(r'^s3://', '', args['REFINED_BUCKET']).split('/')[0]
    refined_prefix = args['REFINED_PREFIX'].strip('/') + '/'
    refined_db     = args['REFINED_DB']
    refined_table  = args['REFINED_TABLE']
    refined_path   = f"s3://{refined_bucket}/{refined_prefix}"

    print(f"[INFO] Input path      = s3://{bucket_name}/{object_key}")
    print(f"[INFO] Refined path    = {refined_path}")
    print(f"[INFO] Glue table      = {refined_db}.{refined_table}")

    s3_client = boto3.client("s3")

    # Descobre os arquivos a processar
    finance_keys = discover_finance_keys(s3_client, bucket_name, object_key)
    if not finance_keys:
        print("[INFO] Nenhum finance.parquet encontrado no prefixo/arquivo informado.")
        sys.exit(0)
    print(f"[INFO] Encontrados {len(finance_keys)} arquivo(s) para processar.")

    # Pré: valida/garante catálogo e Location
    ensure_database(refined_db)

    # Leitura do primeiro arquivo só para inferir schema e criar/alinhar a tabela
    first_key = finance_keys[0]
    first_body = s3_client.get_object(Bucket=bucket_name, Key=first_key)["Body"].read()
    df_first = pd.read_parquet(BytesIO(first_body))

    # Renomes obrigatórios (se existirem)
    if "longName" in df_first.columns: df_first.rename(columns={'longName': 'NomeCompleto'}, inplace=True)
    if "sector"   in df_first.columns: df_first.rename(columns={'sector': 'Setor'}, inplace=True)

    # Garante colunas pois compõem a tabela/partição
    if "ticker" not in df_first.columns:
        df_first["ticker"] = pd.Series(dtype="string")
    for c in ("ano", "mes", "dia"):
        if c not in df_first.columns:
            df_first[c] = pd.Series(dtype="Int64")

    ensure_table_4p_from_df(refined_db, refined_table, refined_path, df_first)

    glue = boto3.client("glue")
    table_keys = get_or_fix_partition_keys(glue, refined_db, refined_table, refined_path, df_first)
    catalog_loc = glue.get_table(DatabaseName=refined_db, Name=refined_table)["Table"]["StorageDescriptor"]["Location"].rstrip("/")
    assert refined_path.rstrip("/") == catalog_loc, f"Path divergente: {refined_path} != {catalog_loc}"
    print(f"[CHECK] PartitionKeys no Glue = {table_keys}")

    # -------------------- MM7 (histórico) --------------------
    df_mm_day = build_mm7_from_keys(s3_client, bucket_name, finance_keys)

    # -------------------- Processamento dia a dia --------------------
    particoes_escritas: List[Dict[str, Any]] = []

    for finance_key in finance_keys:
        print(f"\n[RUN] Processando: s3://{bucket_name}/{finance_key}")

        try:
            body = s3_client.get_object(Bucket=bucket_name, Key=finance_key)["Body"].read()
        except s3_client.exceptions.NoSuchKey:
            print(f"[WARN] Arquivo ausente: {finance_key}, pulando...")
            continue

        df = pd.read_parquet(BytesIO(body))

        # Renomeios obrigatórios (requisito)
        if "longName" in df.columns: df.rename(columns={'longName': 'NomeCompleto'}, inplace=True)
        if "sector"   in df.columns: df.rename(columns={'sector': 'Setor'}, inplace=True)

        # Métrica simples: Delta_Valor
        if {"dayHigh","dayLow"} <= set(df.columns):
            df['Delta_Valor'] = (df['dayHigh'] - df['dayLow']).astype(float)

        # Extrai ano/mes/dia do caminho atual
        ymd = extract_ymd_from_key(finance_key)
        if not ymd:
            print(f"[WARN] Não achou ano/mes/dia em {finance_key}, pulando...")
            continue
        ano_out, mes_out, dia_out = ymd
        df["ano"] = ano_out; df["mes"] = mes_out; df["dia"] = dia_out

        # Sanitização das chaves de partição
        for c in ["ano","mes","dia"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        if "ticker" not in df.columns:
            print("[WARN] Sem coluna 'ticker' — preenchendo 'UNKNOWN'.")
            df["ticker"] = "UNKNOWN"

        df["ticker"] = (
            df["ticker"].astype(str).str.strip().str.upper()
              .replace({"": pd.NA, "NAN": pd.NA})
              .str.replace(r"[/=\\]", "-", regex=True)
              .str.replace(r"[^A-Z0-9._-]", "_", regex=True)
        )
        df = df.dropna(subset=["ano","mes","dia","ticker"]).copy()
        df["ano"] = df["ano"].astype(int)
        df["mes"] = df["mes"].astype(int)
        df["dia"] = df["dia"].astype(int)

        # ---------------- Agregações numéricas que fazem sentido ----------------
        # Se houver coluna 'volume', calculamos:
        # 1) VolumeTotalDia: soma do volume no dia (todas as ações)
        # 2) VolumeSetorDia: soma do volume por setor no dia
        # 3) PrecoMedioSetorDia: média do regularMarketPrice por setor no dia
        if "volume" in df.columns:
            vol_total = (
                df.groupby(["ano","mes","dia"], as_index=False)["volume"]
                  .sum().rename(columns={"volume": "VolumeTotalDia"})
            )
            df = df.merge(vol_total, on=["ano","mes","dia"], how="left")

        if {"Setor","volume"} <= set(df.columns):
            vol_setor = (
                df.groupby(["ano","mes","dia","Setor"], as_index=False)["volume"]
                  .sum().rename(columns={"volume": "VolumeSetorDia"})
            )
            df = df.merge(vol_setor, on=["ano","mes","dia","Setor"], how="left")

        if {"Setor","regularMarketPrice"} <= set(df.columns):
            preco_setor = (
                df.groupby(["ano","mes","dia","Setor"], as_index=False)["regularMarketPrice"]
                  .mean().rename(columns={"regularMarketPrice": "PrecoMedioSetorDia"})
            )
            df = df.merge(preco_setor, on=["ano","mes","dia","Setor"], how="left")

        # Merge da MM7 (se disponível) – requisito
        if not df_mm_day.empty and "ticker" in df.columns:
            df = df.merge(
                df_mm_day,
                on=["ticker","ano","mes","dia"],
                how="left"
            )
            # Caso prefira outro nome:
            # df.rename(columns={"MM7": "Media_Movel_7d"}, inplace=True)

        # Escreve por partição (ticker) - sem dataset
        cols_part = ["ano","mes","dia","ticker"]
        for (a, m, d, t), g in df.groupby(cols_part, dropna=False):
            part_path = f"{refined_path.rstrip('/')}/ano={a:04d}/mes={m:02d}/dia={d:02d}/ticker={t}/"
            obj = f"{part_path}part-{uuid4().hex}.parquet"

            wr.s3.to_parquet(
                df=g.drop(columns=[c for c in cols_part if c in g.columns], errors="ignore"),
                path=obj,
                dataset=False,
                schema_evolution=True,
            )
            particoes_escritas.append({"ano": a, "mes": m, "dia": d, "ticker": str(t)})

    print(f"\n[OK] Escrita concluída. Total de partições tocadas: {len(particoes_escritas)}")

    # De-dup partições
    uniq: Dict[Tuple[int,int,int,str], Dict[str,Any]] = {}
    for p in particoes_escritas:
        uniq[(p["ano"], p["mes"], p["dia"], p["ticker"])] = p
    particoes_escritas = list(uniq.values())
    print(f"[INFO] Partições únicas a registrar: {len(particoes_escritas)}")

    # Registro no catálogo via boto3
    glue = boto3.client("glue")
    tinfo = glue.get_table(DatabaseName=refined_db, Name=refined_table)["Table"]
    base_sd = tinfo["StorageDescriptor"]

    def _to_str(v):
        return str(int(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)

    def make_partition_input(p):
        values_in_order = [_to_str(p[k]) for k in EXPECTED_PK_NAMES]
        part_location = (
            f"{refined_path.rstrip('/')}/"
            f"ano={int(p['ano']):04d}/mes={int(p['mes']):02d}/dia={int(p['dia']):02d}/ticker={p['ticker']}/"
        )
        return {
            "Values": values_in_order,
            "StorageDescriptor": {
                "Columns": base_sd.get("Columns", []),
                "Location": part_location,
                "InputFormat": base_sd.get("InputFormat"),
                "OutputFormat": base_sd.get("OutputFormat"),
                "SerdeInfo": base_sd.get("SerdeInfo"),
                "Compressed": base_sd.get("Compressed", False),
                "NumberOfBuckets": base_sd.get("NumberOfBuckets", 0),
                "StoredAsSubDirectories": base_sd.get("StoredAsSubDirectories", False),
                "Parameters": base_sd.get("Parameters", {}),
            },
            "Parameters": {},
        }

    batch, created = [], 0
    for p in particoes_escritas:
        batch.append(make_partition_input(p))
        if len(batch) == 100:
            resp = glue.batch_create_partition(
                DatabaseName=refined_db,
                TableName=refined_table,
                PartitionInputList=batch
            )
            if resp.get("Errors"):
                print("[WARN] Erros:", resp["Errors"][:5])
            created += len(batch) - len(resp.get("Errors", []))
            batch = []

    if batch:
        resp = glue.batch_create_partition(
            DatabaseName=refined_db,
            TableName=refined_table,
            PartitionInputList=batch
        )
        if resp.get("Errors"):
            print("[WARN] Erros (resto):", resp["Errors"][:5])
        created += len(batch) - len(resp.get("Errors", []))

    print(f"[OK] Partições registradas via boto3: {created}/{len(particoes_escritas)}")

    # Atualiza SCHEMA no final (a partir de um parquet de refined)
    update_glue_table_schema_from_sample(refined_db, refined_table, refined_path)

    print("[DONE] Job finalizado com sucesso.")

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("Erro ao processar o job:")
        traceback.print_exc()
        raise
