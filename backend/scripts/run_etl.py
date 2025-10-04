from etl.downloader import download_all_async
from etl.converter import convert_csv_to_parquet
from etl.loader import load_to_postgres


def main():
    download_all_async()
    convert_csv_to_parquet
    load_to_postgres()

if __name__ == "__main__":
    main()