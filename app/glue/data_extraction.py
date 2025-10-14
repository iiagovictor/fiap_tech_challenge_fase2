import yfinance as yf
import pandas as pd
import concurrent.futures
import json
import time
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import sys
from awsglue.utils import getResolvedOptions
import boto3
from io import BytesIO

s3_client = boto3.client('s3')

class FinanceData:
    def __init__(self):
        self.tickers = ["ALOS3", "ABEV3", "ASAI3", "AURE3", "AZZA3", "B3SA3", "BBSE3", "BBDC3", "BBDC4", 
                       "BRAP4", "BBAS3", "BRKM5", "BRAV3", "BPAC11", "CXSE3", "CEAB3", "CMIG4", "COGN3", 
                       "CPLE6", "CSAN3", "CPFE3", "CMIN3", "CURY3", "CVCB3", "CYRE3", "DIRR3", "ELET3", 
                       "ELET6", "EMBR3", "ENGI11", "ENEV3", "EGIE3", "EQTL3", "FLRY3", "GGBR4", "GOAU4", 
                       "HAPV3", "HYPE3", "IGTI11", "IRBR3", "ISAE4", "ITSA4", "ITUB4", "KLBN11", "RENT3", 
                       "LREN3", "MGLU3", "POMO4", "MBRF3", "BEEF3", "MOTV3", "MRVE3", "MULT3", "NATU3", 
                       "PCAR3", "PETR3", "PETR4", "RECV3", "PRIO3", "PSSA3", "RADL3", "RAIZ4", "RDOR3", 
                       "RAIL3", "SBSP3", "SANB11", "CSNA3", "SLCE3", "SMFT3", "SUZB3", "TAEE11", "VIVT3", 
                       "TIMS3", "TOTS3", "UGPA3", "USIM5", "VALE3", "VAMO3", "VBBR3", "VIVA3", "WEGE3", "YDUQ3"]
        self.sa_tickers = [f"{ticker}.SA" for ticker in self.tickers]
        self.df_tickers = pd.DataFrame(self.sa_tickers, columns=["id_ticker"])
        self.max_retries = 3
        self.retry_delay = 1  # segundos

    def get_tickers_info(self) -> List[Dict]:
        """Obtém informações de todos os tickers com tratamento de erros."""
        print("Buscando dados dos tickers...")
        start_time = time.time()
        tickers_info = {}
        
        try:
            # Divide os tickers em lotes para evitar sobrecarga
            batch_size = 20
            for i in range(0, len(self.df_tickers), batch_size):
                batch_tickers = self.df_tickers["id_ticker"][i:i+batch_size]
                
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    future_to_ticker = {
                        executor.submit(self.get_ticker_info, ticker): ticker 
                        for ticker in batch_tickers
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_ticker):
                        ticker_name, result = future.result()
                        if result is not None:
                            tickers_info[ticker_name] = result
                
                # Pequena pausa entre lotes para evitar rate limiting
                time.sleep(0.5)
            
            if not tickers_info:
                raise Exception("Nenhum dado foi obtido dos tickers")
            
            combined_data = pd.concat(tickers_info)
            combined_data = combined_data.reset_index()
            response = self.fetch_ticker_info(combined_data)
            
            print(f"Tempo total de execução: {time.time() - start_time:.2f} segundos")
            return response
            
        except Exception as e:
            print(f"Erro ao buscar informações dos tickers: {e}")
            return []
        
    def get_ticker_info(self, ticker_name: str) -> Tuple[str, Optional[pd.DataFrame]]:
        """Obtém informações de um ticker específico com retry."""
        for attempt in range(self.max_retries):
            try:
                ticker = yf.Ticker(ticker_name)
                temp = pd.DataFrame.from_dict(ticker.info, orient="index")
                temp.reset_index(inplace=True)
                temp.columns = ["attribute", "value"]
                return ticker_name, temp
            except Exception as e:
                if attempt < self.max_retries - 1:
                    print(f"Tentativa {attempt + 1} falhou para {ticker_name}: {e}")
                    time.sleep(self.retry_delay * (attempt + 1))
                else:
                    print(f"Todas as tentativas falharam para {ticker_name}: {e}")
        return ticker_name, None
   
    def fetch_ticker_info(self, combined_data: pd.DataFrame) -> List[Dict]:
        """Processa e formata as informações dos tickers."""
        result = []
        try:
            for ticker in combined_data['level_0'].unique():
                ticker_id = ticker.replace('.SA', '')
                ticker_data = combined_data[combined_data['level_0'] == ticker]
                info = dict(zip(ticker_data['attribute'], ticker_data['value']))
                
                if ticker_id not in [r['ticker'] for r in result]:
                    result.append({
                        'ticker': ticker_id,
                        'longName': info.get('longName'),
                        'sector': info.get('sector'),
                        'marketCap': info.get('marketCap'),
                        'volume': info.get('volume'),
                        'quoteType': info.get('quoteType'),
                        'regularMarketPrice': info.get('regularMarketPrice'),
                        'open': info.get('open'),
                        'dayLow': info.get('dayLow'),
                        'dayHigh': info.get('dayHigh')
                    })
        except Exception as e:
            print(f"Erro ao processar dados dos tickers: {e}")
            
        return result

    def save_to_parquet(self, data: List[Dict], bucket_target: str, base_path: str) -> None:
        """
        Salva os dados em formato parquet usando uma estrutura de pastas por data.
        
        Args:
            data: Lista de dicionários com os dados dos tickers
            base_path: Caminho base onde será criada a estrutura de pastas
        """
        try:
            # Converte a lista de dicionários para DataFrame
            df = pd.DataFrame(data)
            
            # Obtém a data atual
            current_date = datetime.now()
    
            # Cria a estrutura de pastas
            year_folder = str(current_date.year)
            month_folder = str(current_date.month).zfill(2)
            day_folder = str(current_date.day).zfill(2)
            
            location_uri = f"s3://{bucket_target}/{base_path}/year={year_folder}/month={month_folder}/day={day_folder}/"
            file_name = f"tickers_data_{current_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            full_path = os.path.join(location_uri, file_name)

            # Salva o DataFrame como arquivo parquet no S3

            buffer = BytesIO()
            df.to_parquet(buffer, index=False)
            buffer.seek(0)

            s3_client.upload_fileobj(
                buffer,
                bucket_target,
                f"{base_path}/year={year_folder}/month={month_folder}/day={day_folder}/{file_name}"
                )
            print(f"Arquivo salvo em: {full_path}")
            
        except Exception as e:
            print(f"Erro ao salvar arquivo parquet: {e}")

if __name__ == "__main__":

    args = getResolvedOptions(sys.argv, ['ENV', 'DT_REF', 'BUCKET_TARGET', 'BASE_DIR'])
    
    print(f"=== Job iniciado com sucesso ===")
    print(f"Data de referência: {args['DT_REF']}")
    print(f"Bucket target: {args['BUCKET_TARGET']}")
    print(f"Base dir: {args['BASE_DIR']}")
    
    # Instancia a classe e obtém os dados
    finance_data = FinanceData()
    result = finance_data.get_tickers_info()
    
    # Salva os dados em parquet
    finance_data.save_to_parquet(result, args['BUCKET_TARGET'], args['BASE_DIR'])
    
    # Imprime o resultado em JSON (opcional)
    print(json.dumps(result[10], indent=2))

    print("=== Sucesso ===")