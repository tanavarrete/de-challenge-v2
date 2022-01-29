from os import walk
from pandas import DataFrame

from functions import get_positions_tables, get_top_referee, get_most_thrashed, get_data
from logger import get_logger


def extract_data(dir_files) -> DataFrame:
    data = get_data(dir_files)

    return data


def transform_data(data) -> list:
    positions_tables = get_positions_tables(data)
    top_referees = get_top_referee(data)
    most_thrashed = get_most_thrashed(data)

    return [positions_tables, top_referees, most_thrashed]


def load_data(transformed_data) -> None:
    transformed_data[0].to_csv("/output/EPL_positions_tables.csv")
    transformed_data[1].to_csv("/output/EPL_top_referees.csv")
    transformed_data[2].to_csv("/output/EPL_most_thrasheds.csv")


def etl(files_names) -> None:
    data = extract_data(files_names)
    transformed_data = transform_data(data)
    load_data(transformed_data)


if __name__ == "__main__":
    my_logger = get_logger("English Premier League ETL")
    my_logger.info("Initialize ETL")
    files_names = next(walk("/data"), (None, None, []))[2]
    etl(files_names)
    my_logger.info("Finalize ETL")
