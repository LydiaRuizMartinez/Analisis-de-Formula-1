"""
ergast_pitstops_data.py

Adquisición de datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Lydia Ruiz
    - David Tarrasa
    - Jorge Vančo
    - Alberto Velasco

Descripción:
Obtención de un DataFrame producto del cruzado de los csv obtenidos a partir de formula1_spider.py y ergast_pitstops-data.py"""

import shutil
import requests as req
import pandas as pd
import re
import os
import json


patron_pitstop = re.compile(
    r'<PitStop driverId="(\w+)" stop="(\d+)" lap="(\d+)" time="(\d+:\d+:\d+)" duration="([\d:|]+\d+\.\d+)"\/>'
)


def conseguir_pitstops_año_carrera(anno, num_carrera, update=False, crear_csv=False):
    """
    Function that retrieves pitstop information for a specific Formula 1 race and year
    """
    global patron_pitstop
    # Ensure the existence of necessary directories
    os.makedirs("cache", exist_ok=True)
    os.makedirs(f"cache/{anno}", exist_ok=True)

    # Define the path for the cache file
    path = f"cache/{anno}/{anno}_{num_carrera}.csv"

    # Check if the cache file doesn't exist or if update is requested
    if not os.path.exists(path) or update:
        pitstops_carrera = []
        i = 1
        seguir = True

        # Loop to retrieve pitstop data from an API
        while seguir:
            response = req.get(
                f"https://ergast.com/api/f1/{anno}/{num_carrera}/pitstops/{i}"
            )
            contenido = response.content.decode("utf-8")
            contenido = contenido.split("\n")

            nuevas_pitstops = []

            # Parse pitstop data from the API response
            for linea in contenido:
                bus = re.search(patron_pitstop, linea)
                if bus != None:
                    piloto, parada, vuelta, hora, duracion = bus.groups()
                    nuevas_pitstops.append(
                        f"{piloto};{parada};{vuelta};{hora};{duracion};{anno};{num_carrera}\n"
                    )

            pitstops_carrera.extend(nuevas_pitstops)
            seguir = True if len(nuevas_pitstops) > 0 else False
            i += 1

        # Write pitstop data to a CSV file if there is data and creation of CSV is requested
        if len(pitstops_carrera) > 0 and crear_csv:
            with open(path, "w") as f:
                f.write("piloto;parada;vuelta;hora;duracion;anno;carrera\n")
                f.writelines(pitstops_carrera)

    else:
        # Read pitstop data from the existing cache file
        with open(path, "r") as f:
            pitstops_carrera = f.readlines()

    return pitstops_carrera


def crear_csv_pitstops_intervalo(
    anno_inicial, anno_final, update=False, crear_general=False
):
    """
    Function that creates or updates a CSV file with pitstop information for a range of Formula 1 races.
    """
    os.makedirs(f"cache", exist_ok=True)
    path = f"cache/pitstops_{anno_inicial}_{anno_final}.csv"

    # Check if the cache file doesn't exist or if update is requested
    if not os.path.exists(path) or update:
        # Create a general CSV file with headers if requested
        if crear_general:
            with open(path, "w") as f:
                f.write("piloto;parada;vuelta;hora;duracion;anno;carrera\n")

        # Iterate over the specified range of years and races
        for anno in range(anno_inicial, anno_final + 1):
            for carrera in range(1, 25):
                pitstops_carrera = conseguir_pitstops_año_carrera(
                    anno, carrera, update, True
                )

                # Exclude the header line if creating a general CSV
                if len(pitstops_carrera) > 1 and crear_general:
                    pitstops_carrera.pop(
                        0
                    )  # Remove the first line: piloto;parada;vuelta;hora;duracion;anno;carrera
                    with open(path, "a") as f:
                        f.writelines(pitstops_carrera)


patron_driverid = re.compile(r'<Driver driverId="([\w]+)"')
patron_drivernumber = re.compile(r"<PermanentNumber>([\d]+)<\/PermanentNumber>")


def mapping_pilotos_intervalo(anno_inicial, anno_final, update=False):
    """
    Function that maps driver information for a specified range of Formula 1 seasons
    """
    os.makedirs("cache", exist_ok=True)
    pilotos = {}

    # Iterate over the specified range of years
    for anno in range(anno_inicial, anno_final + 1):
        response = req.get(f"https://ergast.com/api/f1/{anno}/drivers")
        contenido = response.content.decode("utf-8")
        contenido = contenido.split("\n")
        estado = 0

        # Iterate over the API response to extract driver information
        for linea in contenido:
            if estado == 0:
                busqueda = re.search(patron_driverid, linea)
                if busqueda != None:
                    nombre_piloto = busqueda.group(1)
                    estado = 1
            elif (
                estado == 1
            ):  # The line with the driver number always follows the driverId line if it exists
                busqueda = re.search(patron_drivernumber, linea)
                if busqueda == None:
                    numero_piloto = "-1"  # sentinel value
                else:
                    numero_piloto = busqueda.group(1)
                estado = 0

                # Check if the driver is not already in the dictionary
                if nombre_piloto not in pilotos:
                    pilotos[nombre_piloto] = {"number": numero_piloto}
                    info_piloto = req.get(
                        f"https://ergast.com/api/f1/drivers/{nombre_piloto}.json"
                    ).json()
                    detalles_piloto = info_piloto["MRData"]["DriverTable"]["Drivers"][0]
                    givenName = detalles_piloto["givenName"]
                    familyName = detalles_piloto["familyName"]

                    # Swap givenName and familyName for the driver named "zhou"
                    if nombre_piloto == "zhou":
                        givenName, familyName = familyName, givenName

                    pilotos[nombre_piloto]["name"] = givenName + " " + familyName

    # Write the mapped driver information to a JSON file
    with open(
        f"cache/pilotos_{anno_inicial}_{anno_final}.json", "w", encoding="utf-8"
    ) as fich:
        fich.write(json.dumps(pilotos, indent=4, ensure_ascii=False))

    return pilotos


def crear_banco_datos(anno_inicial, anno_final, update=False):
    """
    Function that creates a database by retrieving pitstop and driver information.
    """
    crear_csv_pitstops_intervalo(anno_inicial, anno_final, update)
    mapping_pilotos_intervalo(anno_inicial, anno_final, update)


def func_pasar(tiempo: str):
    """
    Function that converts a string representation of time to a numerical format (minutes).
    """
    try:
        n = tiempo.count(":")
    except AttributeError:
        return tiempo
    if n == 0:
        return float(tiempo)
    if (
        n > 1
    ):  # If a pitstop takes more than an hour, it doesn't make sense to consider it
        return None
    min, secs = tiempo.split(":")
    return float(min) * 60 + float(secs)


def crear_csv_dataframes_intervalos(anno_inicial, anno_final, update=False):
    """
    Function that creates or updates CSV files with processed pitstop information for a specified range of Formula 1 races.
    """
    os.makedirs("cache", exist_ok=True)
    os.makedirs("cache/procesado", exist_ok=True)

    # Create or update driver information mapping
    mapping_pilotos_intervalo(anno_inicial, anno_final, update)

    # Load driver numbers from the mapping file
    with open(
        f"cache/pilotos_{anno_inicial}_{anno_final}.json", "r", encoding="utf-8"
    ) as fich_pilotos:
        numeros_pilotos = json.load(fich_pilotos)

    # Process pitstop information for each race in the specified range
    for anno in range(anno_inicial, anno_final + 1):
        os.makedirs(f"cache/procesado/{anno}", exist_ok=True)
        for carrera in range(1, 25):
            pitstops_carrera = conseguir_pitstops_año_carrera(
                anno, carrera, update, True
            )

            # Process pitstop data if there is information available
            if len(pitstops_carrera) > 1:
                path = f"cache/{anno}/{anno}_{carrera}.csv"
                df_carrera = pd.read_csv(path, sep=";")
                df_carrera["duracion"] = df_carrera["duracion"].apply(func_pasar)

                # Create a dictionary to store processed pitstop information
                dicti = {
                    "driverId": [],
                    "DriverNumber": [],
                    "Driver": [],
                    "NPitstops": [],
                    "MedianPitStopDuration": [],
                }
                indi = []
                i = 0

                # Iterate over unique drivers in the race
                for piloto in df_carrera["piloto"].unique():
                    df_carrera_piloto = df_carrera.loc[df_carrera["piloto"] == piloto]

                    # Populate the dictionary with processed information
                    dicti["driverId"].append(piloto)
                    dicti["DriverNumber"].append(numeros_pilotos[piloto]["number"])
                    dicti["Driver"].append(numeros_pilotos[piloto]["name"])
                    dicti["MedianPitStopDuration"].append(
                        df_carrera_piloto["duracion"].median()
                    )
                    dicti["NPitstops"].append(df_carrera_piloto["parada"].count())
                    indi.append(i)
                    i += 1

                # Create a DataFrame from the dictionary
                df_carrera = pd.DataFrame(data=dicti, index=indi)
                df_carrera["Season"] = anno
                df_carrera["RaceNumber"] = carrera

                # Save the processed pitstop information to a CSV file
                df_carrera.to_csv(
                    f"cache/procesado/{anno}/pitstops_{anno}_{carrera}.csv",
                    index=False,
                    encoding="utf-8",
                )

        # Eliminate previous directory
        shutil.rmtree(f"cache/{anno}")
