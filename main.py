"""
main.py

Adquisición de datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Lydia Ruiz
    - David Tarrasa
    - Jorge Vančo
    - Alberto Velasco

Descripción:
Obtención de un dataset que permite extraer conclusiones sobre la influencia de los pit-stops en los resultados y otros parámetros del campeonato mundial de la Formula 1"""

from dataframe import create_df
from formula1_spider import setup_crawler
from ergast_pitstops_data import crear_csv_dataframes_intervalos

anno_inicial = 2012
anno_final = 2023


if __name__ == "__main__":
    # Get contents from wikipedia using Scrapy
    process = setup_crawler()
    process.start()

    # Get data from Ergast F1 Ap
    crear_csv_dataframes_intervalos(anno_inicial, anno_final)

    # Create dataframe by merging data
    df = create_df()
    print(df)
