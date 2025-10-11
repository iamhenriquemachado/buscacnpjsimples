from etl.downloader import download_all_async
from etl.converter import convert_csv_to_parquet

def main():
    download_all_async()
    convert_csv_to_parquet()

if __name__ == "__main__":
    main()