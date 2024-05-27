"""
graphs.py

Adquisición de datos - IMAT
ICAI, Universidad Pontificia Comillas

Integrantes del grupo:
    - Lydia Ruiz
    - David Tarrasa
    - Jorge Vančo
    - Alberto Velasco

Descripción:
Funciones destinadas a representar los gráficos empleados en el informe.pdf"""

import pandas as pd
import matplotlib.pyplot as plt
from dataframe import create_df


def plot_median_pitstop_duration_by_team(df):
    """
    Chart of the average pit-stop duration per team with the fastest team highlighted.
    """
    team_median_pitstops = (
        df.groupby("Constructor")["MedianPitStopDuration"].median().sort_values()
    )
    colors = ["skyblue" if i > 0 else "blue" for i in range(len(team_median_pitstops))]
    team_median_pitstops.plot(kind="bar", figsize=(12, 6), color=colors)

    plt.title("Duración Media de Pit-Stops por Equipo")
    plt.xlabel("Equipo")
    plt.ylabel("Duración Media (segundos)")
    plt.tight_layout()
    plt.show()


def plot_average_pitstops_per_race(df):
    """
    Chart of the average pit stops per race with the race with the fewest average pit stops highlighted.
    """
    average_pitstops = df.groupby("RaceName")["NPitstops"].mean().sort_values()
    colors = ["skyblue" if i > 0 else "green" for i in range(len(average_pitstops))]
    average_pitstops.plot(kind="bar", figsize=(12, 6), color=colors)

    plt.title("Media de Pit-Stops por Carrera")
    plt.xlabel("Carrera")
    plt.ylabel("Media de Pit-Stops")
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()


def plot_average_pitstop_duration_by_season(df):
    """
    Chart of the average pit-stop duration per season.
    """
    avg_duration_by_season = df.groupby("Season")["MedianPitStopDuration"].mean()
    avg_duration_by_season.plot(kind="line", marker="o", figsize=(10, 5))

    plt.title("Duración Promedio de Pit-Stops por Temporada")
    plt.xlabel("Temporada")
    plt.ylabel("Duración Promedio (segundos)")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_average_pitstops_by_season(df):
    """
    Chart of the average number of pit stops per race per season.
    """
    avg_pitstops_by_season = df.groupby("Season")["NPitstops"].mean()
    avg_pitstops_by_season.plot(kind="line", marker="o", figsize=(10, 5), color="green")

    plt.title("Número Promedio de Pit-Stops por Carrera por Temporada")
    plt.xlabel("Temporada")
    plt.ylabel("Número Promedio de Pit-Stops por Carrera")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


def plot_points_by_drivers(df):
    """
    Chart of the accumulated points by the top 10 drivers.
    """
    df["Points"] = pd.to_numeric(df["Points"], errors="coerce")
    df["Points"].fillna(0, inplace=True)
    top_10_drivers = (
        df.groupby("Driver")["Points"].sum().sort_values(ascending=False).head(10)
    )
    colors = ["green" if i == max(top_10_drivers) else "blue" for i in top_10_drivers]
    top_10_drivers.plot(kind="bar", color=colors, figsize=(10, 6))

    plt.title("Top 10 Pilotos con Más Puntos Acumulados")
    plt.xlabel("Piloto")
    plt.ylabel("Puntos Acumulados")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()


def plot_laps_led_by_constructor(df):
    """
    This function takes a Formula 1 DataFrame and generates a bar chart depicting the total number of laps led by each constructor.
    """
    df["Laps"] = pd.to_numeric(df["Laps"], errors="coerce")
    df["Laps"].fillna(0, inplace=True)
    laps_led_by_constructor = (
        df.groupby("Constructor")["Laps"].sum().sort_values(ascending=False)
    )

    plt.figure(figsize=(14, 7))
    plt.bar(laps_led_by_constructor.index, laps_led_by_constructor.values, color="navy")
    plt.title("Total de Vueltas Lideradas por Constructor")
    plt.xlabel("Constructor")
    plt.ylabel("Vueltas Lideradas")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()
