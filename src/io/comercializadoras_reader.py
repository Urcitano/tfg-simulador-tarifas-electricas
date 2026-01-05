# -*- coding: utf-8 -*-

"""
Lector del fichero Comercializadoras.csv.

Incluye lógica de lectura robusta para codificaciones típicas en Windows,
incluyendo UTF-8 con BOM (utf-8-sig), para evitar errores en caracteres con tilde.
"""

from pathlib import Path
import pandas as pd


def read_comercializadoras_csv(path: Path) -> pd.DataFrame:
    """
    Lee Comercializadoras.csv (separador ';') y devuelve un DataFrame.

    - Intenta varias codificaciones habituales.
    - Convierte columnas numéricas a float (admite coma o punto decimal).
    - Elimina duplicados por (comercializadora, tarifa).
    """
    if not path.exists():
        raise FileNotFoundError(f"No existe el fichero de comercializadoras: {path}")

    df = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            df = pd.read_csv(path, sep=";", encoding=encoding)
            break
        except UnicodeDecodeError:
            continue

    if df is None:
        raise UnicodeDecodeError(
            "No se pudo leer el CSV con las codificaciones habituales.",
            b"", 0, 1, "decode"
        )

    columnas_esperadas = [
        "comercializadora",
        "tarifa",
        "punta_LV",
        "llano_LV",
        "valle_LV",
        "valle_SD",
        "potencia_mes_valle",
        "potencia_mes_punta_llano",
    ]

    missing = set(columnas_esperadas) - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en Comercializadoras.csv: {sorted(missing)}")

    # Normalización numérica: coma -> punto
    columnas_numericas = columnas_esperadas[2:]
    for col in columnas_numericas:
        df[col] = (
            df[col].astype(str).str.strip().str.replace(",", ".", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="raise")

    # Limpieza básica de texto (manteniendo acentos correctamente)
    df["comercializadora"] = df["comercializadora"].astype(str).str.strip()
    df["tarifa"] = df["tarifa"].astype(str).str.strip()

    df = df.drop_duplicates(subset=["comercializadora", "tarifa"]).reset_index(drop=True)

    return df
