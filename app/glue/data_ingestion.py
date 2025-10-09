import sys
from awsglue.utils import getResolvedOptions

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'ENV', 'DT_REF'])
    
    print(f"=== Job iniciado com sucesso ===")
    print(f"Environment: {args['ENV']}")
    print(f"Data de referência: {args['DT_REF']}")
    print(f"Job Name: {args['JOB_NAME']}")
    print("=== Sucesso ===")

if __name__ == "__main__":
    main()