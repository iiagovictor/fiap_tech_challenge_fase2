import sys
from awsglue.utils import getResolvedOptions
from io import BytesIO
import pandas as pd
import boto3
from typing import Dict, Any, List, Optional, Tuple
import pyarrow as pa, pyarrow.parquet as pq
import awswrangler as wr
import re
import traceback
from uuid import uuid4

#Funcoes auxiliares
def verifico_existencia_particao(database_name: str, table_name: str) -> Dict[str, Any]:
    glue = boto3.client("glue")

    paginator = glue.get_paginator("get_partitions")
    pages = paginator.paginate(DatabaseName=database_name, TableName=table_name)

    raw_partitions: List[Dict[str, Any]] = []

    # Coleta todas as partições (com ticker)
    for page in pages:
        for p in page.get("Partitions", []):
            vals = p.get("Values", [])
            # Espera-se ordem: [ano, mes, dia, ticker]
            if len(vals) < 3:
                continue
            try:
                ano = int(vals[0])
                mes = int(vals[1])
                dia = int(vals[2])
            except (ValueError, TypeError):
                # pula partições fora do padrão numérico
                continue

            ticker = vals[3] if len(vals) > 3 else None
            raw_partitions.append(
                {
                    "ano": ano,
                    "mes": mes,
                    "dia": dia,
                    "ticker": ticker,
                }
            )

    if not raw_partitions:
        return {
            "raw_partitions": [],
            "date_partitions": [],
            "latest_date": None,
        }

    # Deduplica por (ano, mes, dia) ignorando ticker
    unique_dates: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for part in raw_partitions:
        key = (part["ano"], part["mes"], part["dia"])
        if key not in unique_dates:
            unique_dates[key] = {
                "ano": part["ano"],
                "mes": part["mes"],
                "dia": part["dia"],
                "as_path": f"ano={part['ano']}/mes={part['mes']:02d}/dia={part['dia']:02d}",
            }

    date_partitions = list(unique_dates.values())

    # Mais recente por (ano, mes, dia)
    latest = max(date_partitions, key=lambda x: (x["ano"], x["mes"], x["dia"]))

    return {
        "latest_date": latest
    }


def listar_datas_unicas_glue(database_name: str, table_name: str):
    glue = boto3.client("glue")
    paginator = glue.get_paginator("get_partitions")
    pages = paginator.paginate(DatabaseName=database_name, TableName=table_name)

    seen = set()
    datas = []
    for page in pages:
        for p in page.get("Partitions", []):
            vals = p.get("Values", [])
            if len(vals) < 3:
                continue
            try:
                ano, mes, dia = int(vals[0]), int(vals[1]), int(vals[2])
            except Exception:
                continue
            key = (ano, mes, dia)
            if key not in seen:
                seen.add(key)
                datas.append({"ano": ano, "mes": mes, "dia": dia})

    datas.sort(key=lambda x: (x["ano"], x["mes"], x["dia"]))
    return datas

# ==== helpers para garantir DB e TABELA ====
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

    expected_keys = [
        {"Name": "ano", "Type": "int"},
        {"Name": "mes", "Type": "int"},
        {"Name": "dia", "Type": "int"},
        {"Name": "ticker", "Type": "string"},
    ]
    expected_names = [k["Name"] for k in expected_keys]
    target_loc = s3_location.rstrip("/")

    def build_cols():
        part_cols = set(expected_names)
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
                "PartitionKeys": expected_keys,
                "StorageDescriptor": storage_descriptor,
            },
        )
        print(f"[GLUE] Tabela criada: {db_name}.{table_name} @ {target_loc}")

    try:
        t = glue_client.get_table(DatabaseName=db_name, Name=table_name)["Table"]
        current_keys = [k["Name"] for k in t.get("PartitionKeys", [])]
        current_loc = t["StorageDescriptor"].get("Location", "").rstrip("/")

        if current_keys != expected_names:
            print(f"[GLUE][FIX] PartitionKeys {current_keys} != {expected_names}. Recriando...")
            glue_client.delete_table(DatabaseName=db_name, Name=table_name)
            create_table()
            return

        if current_loc != target_loc:
            print(f"[GLUE][UPDATE] Location {current_loc} -> {target_loc}")
            sd = t["StorageDescriptor"]
            sd["Location"] = target_loc
            # opcional: alinhar Columns ao DF atual (não inclui colunas de partição)
            sd["Columns"] = build_cols()

            table_input = {
                "Name": table_name,
                "TableType": t.get("TableType", "EXTERNAL_TABLE"),
                "Parameters": t.get("Parameters", {"EXTERNAL": "TRUE", "classification": "parquet"}),
                "PartitionKeys": expected_keys,
                "StorageDescriptor": sd,
            }
            glue_client.update_table(DatabaseName=db_name, TableInput=table_input)
            print(f"[GLUE] Location atualizado.")

        else:
            print(f"[GLUE] Tabela OK: keys={current_keys} location={current_loc}")

    except glue_client.exceptions.EntityNotFoundException:
        create_table()



args = getResolvedOptions(sys.argv, ['LOCATION_PATH'])

s3_file_path = args['LOCATION_PATH']
s3_path_parts = s3_file_path.replace("s3://", "").split("/", 1)
bucket_name = s3_path_parts[0]
object_key = s3_path_parts[1]

# Define o prefixo base ANTES de ano=/mes=/dia=
m_base = re.search(r"(.*?)(?=ano=\d{4}/mes=\d{1,2}/dia=\d{1,2})", object_key)
if not m_base:
    raise ValueError("LOCATION_PATH não contém padrão de partição ano/mes/dia.")
base_prefix = m_base.group(1)  # ex.: "raw/" ou "dados/raw/"


print(f"Bucket: {bucket_name}, Chave: {object_key}")

try:
    s3_client = boto3.client('s3')

    # Checa se o arquivo existe antes de ler, isso evita do script quebrar caso o arquivo de partição do dia não exista
    head = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=object_key)
    if head.get("KeyCount", 0) == 0:
        print("Arquivo de partição não encontrado")  # <- sua mensagem pedida
        # opcional: sair “limpo” do job
        sys.exit(0)
        
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    file_content = response['Body'].read()
    
    df = pd.read_parquet(BytesIO(file_content))
    
    # Renomeando colunas
    df.rename(columns={'longName': 'NomeCompleto', 'sector': 'Setor'}, inplace=True)

    #Calculo de diferenca
    df['Delta_Valor'] = (df['dayHigh'] - df['dayLow']).astype(float)

    #Pegando os ultimos 7 dias
    N_DIAS_MM = 7
    
    # pega as datas únicas do catálogo (mesma tabela que você já usa na sua função)
    datas_unicas = listar_datas_unicas_glue('default', 'tabela-glue-catalog')
    if len(datas_unicas) == 0:
        print("[MM7] Nenhuma data no catálogo; MM7 não será calculada.")
    else:
        # recorta as últimas N datas (janela)
        ultimas_datas = datas_unicas[-N_DIAS_MM:]
    
    # ===== MM7 lendo 1 arquivo por dia: .../ano=YYYY/mes=MM/dia=DD/finance.parquet =====
    frames = []
    for d in ultimas_datas:
        key_dia = f"{base_prefix}ano={d['ano']:04d}/mes={d['mes']:02d}/dia={d['dia']:02d}/finance.parquet"
        try:
            body = s3_client.get_object(Bucket=bucket_name, Key=key_dia)["Body"].read()
        except s3_client.exceptions.NoSuchKey:
            print(f"[MM7] Arquivo ausente para {key_dia}, pulando...")
            continue
    
        df_dia = pd.read_parquet(BytesIO(body))
        # garante colunas de partição no DF diário
        df_dia["ano"] = d["ano"]; df_dia["mes"] = d["mes"]; df_dia["dia"] = d["dia"]
    
        # precisa ter 'ticker' para calcular/mesclar por ativo
        if "ticker" not in df_dia.columns:
            print(f"[MM7][WARN] '{key_dia}' sem coluna 'ticker'; pulando esse dia.")
            continue
    
        frames.append(df_dia)
    
    if frames:
        import numpy as np
        df_all = pd.concat(frames, ignore_index=True)
    
        # normaliza numérico
        if "regularMarketPrice" in df_all.columns:
            df_all["regularMarketPrice"] = pd.to_numeric(df_all["regularMarketPrice"], errors="coerce")
        else:
            print("[MM7][WARN] Coluna 'regularMarketPrice' não existe; preenchendo MM7 como NaN.")
            df_all["regularMarketPrice"] = np.nan
    
        # coluna data e ordenação
        df_all["data"] = pd.to_datetime(
            df_all[["ano","mes","dia"]].astype(str).agg("-".join, axis=1),
            format="%Y-%m-%d", errors="coerce"
        )
        df_all = df_all.sort_values(["ticker","data"])
    
        # MM7 por ticker
        df_all["Media_Movel"] = (
            df_all.groupby("ticker")["regularMarketPrice"]
                  .transform(lambda s: s.rolling(window=N_DIAS_MM, min_periods=1).mean())
        )
    
        # Descobre a data da partição atual (do LOCATION_PATH) para trazer só a MM do dia-alvo
        m_atual_merge = re.search(r"ano=(\d{4})/mes=(\d{1,2})/dia=(\d{1,2})", object_key)
        if m_atual_merge:
            ano_at, mes_at, dia_at = map(int, m_atual_merge.groups())
            df_mm_dia = df_all[(df_all["ano"]==ano_at)&(df_all["mes"]==mes_at)&(df_all["dia"]==dia_at)]
    
            # merge por ticker (garanta que seu df final tem 'ticker')
            # se não existir ticker, não tem como fazer merge – apenas loga/continua
            if "ticker" in df.columns:
                df["ticker"] = df["ticker"].astype(str)
                df = df.merge(df_mm_dia[["ticker", "Media_Movel"]], on="ticker", how="left")
            else:
                print("[MM7] Não foi possível fazer merge: coluna 'ticker' ausente no DF atual.")

    
            if "ticker" in df.columns:
                df = df.merge(
                    df_mm_dia[["ticker", "Media_Movel"]],
                    on="ticker",
                    how="left"
                )
            else:
                print("[MM7] Não foi possível fazer merge: coluna 'ticker' ausente no DF atual.")
        else:
            print("[MM7] Não foi possível inferir a data atual da partição para merge da MM7.")
    else:
        print("[MM7] Nenhum parquet encontrado nas últimas datas; MM7 não será calculada.")
    

    retorno = verifico_existencia_particao('default','tabela-glue-catalog')
    print(retorno)

    print("Arquivo Parquet lido com sucesso para um DataFrame do Pandas!")
    print("Informações do DataFrame:")
    df.info()

    print("\nAmostra dos dados (5 primeiras linhas):")
    print(df.head())

    # ===== Salvar em S3/Parquet particionado em refined (data + ticker) =====
    # Deduz partições da partição atual (ou use latest do catálogo)
    m_atual = re.search(r"ano=(\d{4})/mes=(\d{1,2})/dia=(\d{1,2})", object_key)
    if not m_atual:
        raise ValueError("Não foi possível inferir ano/mes/dia do LOCATION_PATH para salvar em refined.")
    ano_out, mes_out, dia_out = map(int, m_atual.groups())
    df["ano"] = ano_out; df["mes"] = mes_out; df["dia"] = dia_out
    
    if "ticker" not in df.columns:
        # se você renomeou para 'Acoes', mantemos ticker a partir de Acoes
        if "ticker" in df.columns:
            df["ticker"] = df["ticker"]
        else:
            df["ticker"] = "UNKNOWN"
    
    # ==== garantir partições válidas no DF ====
    for c in ["ano", "mes", "dia"]:
        if c not in df.columns:
            raise ValueError(f"Coluna de partição ausente no DF: {c}")
    
    df["ano"] = pd.to_numeric(df["ano"], errors="coerce").astype("Int64")
    df["mes"] = pd.to_numeric(df["mes"], errors="coerce").astype("Int64")
    df["dia"] = pd.to_numeric(df["dia"], errors="coerce").astype("Int64")
    df["ticker"] = df["ticker"].astype(str).str.strip()
    
    mask_valid = df["ano"].notna() & df["mes"].notna() & df["dia"].notna() & df["ticker"].ne("") & df["ticker"].ne("nan")
    if not mask_valid.all():
        print("[WARN] Linhas inválidas para partição (mostrando até 10):")
        print(df.loc[~mask_valid, ["ano","mes","dia","ticker"]].head(10))
        df = df.loc[mask_valid].copy()
    
    print("[DEBUG] Exemplos de partições no DF (até 10):")
    print(df[["ano","mes","dia","ticker"]].drop_duplicates().head(10))
        
    # Caminho base refined (mesmo bucket do input, ajuste se quiser outro)
    refined_base_prefix = re.sub(r"(^.*?)(?=ano=\d{4}/mes=\d{1,2}/dia=\d{1,2})", "refined/", base_prefix)
    
    args = getResolvedOptions(
        sys.argv,
        ['LOCATION_PATH', 'REFINED_BUCKET', 'REFINED_PREFIX', 'REFINED_DB', 'REFINED_TABLE']
    )
    
    refined_bucket = args['REFINED_BUCKET']                 # ex: bucket-destino-desafio2-fiap
    refined_prefix = args['REFINED_PREFIX'].strip('/')+'/'  # ex: refined/
    refined_db     = args['REFINED_DB']                     # ex: default
    refined_table  = args['REFINED_TABLE']                  # ex: refined_b3


    # LOCATION_PATH
    s3_file_path = args['LOCATION_PATH']     # ex: s3://.../ano=YYYY/mes=MM/dia=DD/finance.parquet
    bucket_name = s3_file_path.replace("s3://", "").split("/", 1)[0]
    object_key  = s3_file_path.replace("s3://", "").split("/", 1)[1]
    
    # TARGET (refined)
    refined_bucket = re.sub(r'^s3://', '', args['REFINED_BUCKET']).split('/')[0]
    refined_prefix = args['REFINED_PREFIX'].strip('/') + '/'
    refined_db     = args['REFINED_DB']
    refined_table  = args['REFINED_TABLE']
    
    refined_path   = f"s3://{refined_bucket}/{refined_prefix}"  # use ESTE em tudo (catálogo e write)

    print(f"[INFO] Path target  = {refined_path}")
    print(f"[INFO] Glue target  = {refined_db}.{refined_table}")
    
    # 1) garante DB
    ensure_database(refined_db)
    
    # 2) cria a tabela, se não existir, com 4 partitions **no mesmo Location** do write
    ensure_table_4p_from_df(refined_db, refined_table, refined_path, df)
    
    # 3) (opcional) confirme que a ordem das chaves é a esperada
    glue_client = boto3.client("glue")
    tinfo = glue_client.get_table(DatabaseName=refined_db, Name=refined_table)
    table_keys = [k["Name"] for k in tinfo["Table"]["PartitionKeys"]]
    print(f"[CHECK] PartitionKeys no Glue = {table_keys}")  # deve imprimir ['ano','mes','dia','ticker']
    
    print('Printando refined path, db e table:')
    print(refined_path)
    print(refined_db)
    print(refined_table)
    print(tinfo)
    
    
    catalog_loc = tinfo["Table"]["StorageDescriptor"]["Location"].rstrip("/")
    assert refined_path.rstrip("/") == catalog_loc, f"Path divergente: {refined_path} != {catalog_loc}"

    # --- 1) sanity de partição ---
    # (use o patch de sanitização de 'ticker' que mandei antes!)
    cols_part = ["ano", "mes", "dia", "ticker"]
    missing = [c for c in cols_part if c not in df.columns]
    if missing:
        raise ValueError(f"Faltam colunas de partição: {missing}")
    
    for c in ["ano","mes","dia"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
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
    
    print("[DEBUG] Partições únicas deste batch:")
    print(df[cols_part].drop_duplicates().head(20))
    
    # --- 2) escreve agrupando por partição, sem dataset (sem varrer prefixo) ---
    particoes_escritas = []
    for (a, m, d, t), g in df.groupby(cols_part, dropna=False):
        # caminho exato da partição alvo
        part_path = f"{refined_path.rstrip('/')}/ano={a:04d}/mes={m:02d}/dia={d:02d}/ticker={t}/"
        # nome de arquivo único pra evitar colisão
        obj = f"{part_path}part-{uuid4().hex}.parquet"
    
        # escreve SOMENTE esse grupo, sem 'dataset'
        wr.s3.to_parquet(
            df=g.drop(columns=[c for c in cols_part if c in g.columns], errors="ignore"),
            path=obj,
            dataset=False,
            schema_evolution=True,
            # database/table INTENCIONALMENTE não informados aqui
        )
    
        particoes_escritas.append({"ano": a, "mes": m, "dia": d, "ticker": str(t)})
        



        
         # --- 3) REGISTRO DE PARTIÇÕES VIA BOTO3 (robusto) ---
        glue = boto3.client("glue")
        
        # Leia o SD da tabela para herdar formatos/serde/columns
        tinfo = glue.get_table(DatabaseName=refined_db, Name=refined_table)["Table"]
        base_sd = tinfo["StorageDescriptor"]
        table_keys = [k["Name"] for k in tinfo["PartitionKeys"]]  # deve ser ['ano','mes','dia','ticker']
        
        def _to_str(v):
            return str(int(v)) if isinstance(v, (int, float)) and not isinstance(v, bool) else str(v)
        
        def make_partition_input(p):
            # p = {"ano": 2025, "mes": 9, "dia": 30, "ticker": "ABEV3"}
            # 1) Values em ORDEM das keys
            values_in_order = [_to_str(p[k]) for k in table_keys]  # ['2025','9','30','ABEV3']
        
            # 2) Location completo da partição
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
        
        # DEBUG opcional (conferir 1 exemplo)
        if particoes_escritas:
            ex = make_partition_input(particoes_escritas[0])
            print("[DEBUG] Values exemplo:", ex["Values"])
            print("[DEBUG] Location exemplo:", ex["StorageDescriptor"]["Location"])
        
        # Envie em lotes de até 100
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
           
        
        
    print("[OK] wr.s3.to_parquet finalizado.")
    
    
except Exception as e:
    print("Erro ao processar o arquivo:")
    traceback.print_exc()   # mostra exatamente a linha/origem real
    print(e)
    raise e

print("Job Python Shell finalizado com sucesso.")
    