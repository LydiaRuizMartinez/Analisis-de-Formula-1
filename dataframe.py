"""
dataframe.py

Adquisición de datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Lydia Ruiz
    - David Tarrasa
    - Jorge Vančo
    - Alberto Velasco

Descripción:
Obtención de un DataFrame producto del cruzado de los csv obtenidos a partir de formula1_spider.py y ergast_pitstops-data.py"""

import pandas as pd
import os


cache_path = "./cache/"
spider_path = os.path.join(cache_path, "spider/")


def get_all_csv(
    df, path_to_csv_folder, sep=",", encoding="utf-8", clean=False
) -> pd.DataFrame:
    """
    Function that creates a DataFrame from all the CSV files in a directory.
    """
    # To terate through the CSV files.
    for csv in os.listdir(path_to_csv_folder):
        csv_path = os.path.join(path_to_csv_folder, csv)

        # To read the CSV file.
        new_df = pd.read_csv(csv_path, sep=sep, encoding=encoding)

        # To clean the CSV.
        if clean:
            try:
                # Position of the first Source. ...
                first_position_source = (
                    new_df[(new_df["Pos"].str.contains("Source", na=False))]
                    .iloc[0]
                    .name
                )
            except:
                first_position_source = len(new_df)
            try:
                # Position of the first Fastest ...
                first_position_fastest = (
                    new_df[(new_df["Pos"].str.contains("Fastest", na=False))]
                    .iloc[0]
                    .name
                )
            except:
                first_position_fastest = len(new_df)

            # We take the dataframe up to the minimum value to remove the part that does not belong to the table.

            new_df = new_df.iloc[: min(first_position_fastest, first_position_source)]

        # To concatenate the new data to the existing dataframe.
        df = pd.concat([df, new_df])
    return df


def get_df() -> pd.DataFrame:
    """
    Function that retrieves the dataframe containing all the information extracted from Wikipedia.
    """
    df = pd.DataFrame()

    # To iterate through the spider directories.
    for dir in os.listdir(spider_path):
        current_dir = os.path.join(spider_path, dir)
        # We retrieve all the CSV files within the directory.
        df = get_all_csv(df, current_dir, clean=True)

    # To remove NA values.
    df = df.dropna(subset="Constructor")
    df["DriverNumber"] = df["DriverNumber"].astype(int)

    # To change the names.
    df = df.replace(
        {
            "Carlos Sainz Jr. 2": "Carlos Sainz",
            "Carlos Sainz Jr.": "Carlos Sainz",
            "Nikita Mazepin [a]": "Nikita Mazepin",
            "Nikita Mazepin [b]": "Nikita Mazepin",
            "Nikita Mazepin [c]": "Nikita Mazepin",
            "Nikita Mazepin [e]": "Nikita Mazepin",
            "Nikita Mazepin [d]": "Nikita Mazepin",
        }
    )

    # To remove numbers that appear to the right of the actual value (separated by a space).
    cols = ["Pos", "DriverNumber", "Grid"]
    df[cols] = df[cols].apply(
        lambda x: x.apply(lambda y: y.split(" ")[0] if isinstance(y, str) else y)
    )
    return df


def get_procesado() -> pd.DataFrame:
    """
    Function that retrieves the dataframe of Ergast F1 API
    """
    df = pd.DataFrame()

    procesado_path = os.path.join(cache_path, "procesado")

    # We iterate through the processed directory.
    for dir in os.listdir(procesado_path):
        dir_path = os.path.join(procesado_path, dir)
        df = get_all_csv(df, dir_path)

    return df


def merge_dfs(df1, df_procesado) -> pd.DataFrame:
    """
    Function that merges the two F1 dataframes
    """
    # We merge the dataframes.
    return pd.merge(
        df1, df_procesado, on=["Driver", "Season", "RaceNumber"], how="outer"
    )


def create_df() -> pd.DataFrame:
    """
    Create a single dataframe from the information extracted from Wikipedia and Ergast F1 API
    """
    # The two dataframes are created.
    df1 = get_df()
    df_procesado = get_procesado()

    merged_df = merge_dfs(df1, df_procesado)

    # Save the merged DataFrame to a CSV file
    merged_df.to_csv("dataframe.csv", index=False)

    return merged_df
