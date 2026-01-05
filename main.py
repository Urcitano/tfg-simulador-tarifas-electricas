# -*- coding: utf-8 -*-
"""
- Carga consumo (Datadis) y catálogo de tarifas (Comercializadoras.csv).
- Ejecuta simulación de ranking anual y mejor tarifa mensual.
- Guarda resultados en ./out con:
  - separador de columnas ';'
  - separador decimal ','
  - redondeo a 4 decimales
  - codificación 'utf-8-sig' (para tildes y Excel en Windows)
"""

from pathlib import Path
import pandas as pd

from src.config import ProjectPaths
from src.pipeline_ingesta import cargar_datos
from src.simulacion.costes import (
    calcular_costes_anuales_por_tarifa,
    calcular_mejor_tarifa_por_mes,
)


def guardar_csv_es(df: pd.DataFrame, path: Path) -> None:
    """
    Guarda un CSV compatible con configuración regional ES y Excel:
    - Columnas separadas por ';'
    - Decimales con ','
    - Redondeo a 4 decimales en columnas numéricas
    - UTF-8 con BOM (utf-8-sig) para evitar problemas de tildes
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    df_out = df.copy()

    # Redondeo SOLO de columnas numéricas
    for col in df_out.columns:
        if pd.api.types.is_numeric_dtype(df_out[col]):
            df_out[col] = df_out[col].round(4)

    df_out.to_csv(
        path,
        index=False,
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
    )


def main() -> None:
    paths = ProjectPaths(project_root=Path(__file__).resolve().parent)

    # Nombres EXACTOS tal como estén en ./data
    consumo_file = "Consumo_01-01-2025_31-12-2025.csv"
    comercializadoras_file = "Comercializadoras.csv"

    datos = cargar_datos(
        data_dir=paths.data_dir,
        consumo_filename=consumo_file,
        comercializadoras_filename=comercializadoras_file,
    )

    consumo = datos["consumo"]
    comercializadoras = datos["comercializadoras"]

    print("Consumo (primeras filas):")
    print(consumo.head())

    print("\nComercializadoras (primeras filas):")
    print(comercializadoras.head())

    # ---------------------------------------------------------------------
    # SIMULACIÓN DE COSTES (PROTOTIPO)
    # ---------------------------------------------------------------------
    # Potencia contratada (kW): parámetros fijos del prototipo.
    potencia_kw_valle = 3.45
    potencia_kw_punta_llano = 4.60

    # 1) Ranking ANUAL (todas las tarifas)
    ranking_anual = calcular_costes_anuales_por_tarifa(
        consumo_df=consumo,
        tarifas_df=comercializadoras,
        potencia_kw_valle=potencia_kw_valle,
        potencia_kw_punta_llano=potencia_kw_punta_llano,
    )

    print("\nRanking de tarifas ANUAL (top 10):")
    print(ranking_anual.head(10))

    # 2) Mejor tarifa por MES
    mejor_mensual = calcular_mejor_tarifa_por_mes(
        consumo_df=consumo,
        tarifas_df=comercializadoras,
        potencia_kw_valle=potencia_kw_valle,
        potencia_kw_punta_llano=potencia_kw_punta_llano,
    )

    print("\nMejor tarifa por MES:")
    print(mejor_mensual)

    # Guardado final (directo en ./out)
    out_dir = paths.project_root / "out"
    guardar_csv_es(ranking_anual, out_dir / "ranking_tarifas_anual.csv")
    guardar_csv_es(mejor_mensual, out_dir / "ranking_tarifas_mensual.csv")


if __name__ == "__main__":
    main()
