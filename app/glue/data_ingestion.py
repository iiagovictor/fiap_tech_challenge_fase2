import sys
from awsglue.utils import getResolvedOptions

def main():
    args = getResolvedOptions(sys.argv, ['ENV', 'DT_REF', 'BUCKET_TARGET', 'KEY', 'URI_OBJECT', 'OUTPUT_DATABASE', 'OUTPUT_TABLE'])
    
    print(f"=== Job iniciado com sucesso ===")
    print(f"Parâmetros recebidos: {args}")
    print("=== Sucesso ===")

if __name__ == "__main__":
    main()