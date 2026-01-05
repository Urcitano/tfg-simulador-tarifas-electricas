# -*- coding: utf-8 -*-
"""
Pipeline de ingesta.

Responsabilidad:
- Cargar consumo (Datadis) y catálogo de tarifas (Comercializadoras.csv).
- Dejar los DataFrames listos para la simulación.

Nota:
- Se elimina la lectura de potencias.
- Se elimina el enriquecimiento_base (se integra lo mínimo en el lector de consumo).
"""

from pathlib import Path
import pandas as pd

from src.config import DatadisCSVConfig, NormalizacionConfig
from src.io.datadis_reader import read_consumo_datadis_csv
from src.io.comercializadoras_reader import read_comercializadoras_csv


def cargar_datos(
    data_dir: Path,
    consumo_filename: str,
    comercializadoras_filename: str = "Comercializadoras.csv",
    csv_cfg=None,
    norm_cfg=None,
) -> dict[str, pd.DataFrame]:
    """
    Carga datasets desde ./data:

    - Consumo horario (Datadis)
    - Tarifas sintéticas (Comercializadoras.csv)

    Devuelve:
      - 'consumo'
      - 'comercializadoras'
    """
    if csv_cfg is None:
        csv_cfg = DatadisCSVConfig()
    if norm_cfg is None:
        norm_cfg = NormalizacionConfig()

    consumo_path = data_dir / consumo_filename
    comercializadoras_path = data_dir / comercializadoras_filename

    consumo = read_consumo_datadis_csv(consumo_path, csv_cfg=csv_cfg, norm_cfg=norm_cfg)
    comercializadoras = read_comercializadoras_csv(comercializadoras_path)

    return {
        "consumo": consumo,
        "comercializadoras": comercializadoras,
    }
