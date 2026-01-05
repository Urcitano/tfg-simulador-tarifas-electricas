# -*- coding: utf-8 -*-
"""
Lectura de datos Datadis.

- Lectura de consumo horario.
- Normalización de fecha/hora:
  - Admite fecha en DD/MM/YYYY o YYYY/MM/DD (y variantes con '-' o '.').
  - Admite hora en HH:MM o HH:MM:SS.
  - Trata '24:00' / '24:00:00' como '00:00:00' del día siguiente.
- Genera columnas mínimas necesarias para simulación:
  - timestamp (datetime)
  - kwh (float)
  - es_fin_semana (bool)

"""

from pathlib import Path
import re
import pandas as pd


def _normalizar_hora_a_hhmmss(hora_series: pd.Series) -> pd.Series:
    """
    Normaliza una serie de horas a formato HH:MM:SS.
    Admite HH:MM, HH:MM:SS y recorta decimales si existieran.
    """
    h = hora_series.astype(str).str.strip()

    # Quitar posibles milisegundos: "12:30:00.000" -> "12:30:00"
    h = h.str.replace(r"(\d{2}:\d{2}:\d{2})\..*$", r"\1", regex=True)

    # Si viene como HH:MM, añadir :00
    mask_hhmm = h.str.match(r"^\d{1,2}:\d{2}$")
    h.loc[mask_hhmm] = h.loc[mask_hhmm] + ":00"

    # Si viene como H:MM:SS (una sola cifra de hora), normalizar a 2 cifras
    mask_h1 = h.str.match(r"^\d{1}:\d{2}:\d{2}$")
    h.loc[mask_h1] = "0" + h.loc[mask_h1]

    return h


def _inferir_formato_fecha(fecha_series: pd.Series) -> tuple[bool, bool]:
    """
    Devuelve (dayfirst, yearfirst) según una heurística simple:
    - Si la fecha empieza por 4 dígitos (YYYY/...), asumimos yearfirst=True.
    - Si no, asumimos dayfirst=True (caso típico DD/MM/YYYY en ES).
    """
    sample = fecha_series.dropna().astype(str).str.strip()
    if sample.empty:
        return True, False

    s = sample.iloc[0]
    if re.match(r"^\d{4}[-/\.]", s):
        return False, True  # yearfirst
    return True, False  # dayfirst


def read_consumo_datadis_csv(path: Path, csv_cfg=None, norm_cfg=None) -> pd.DataFrame:
    """
    Lee el CSV de consumo Datadis con separador ';' y devuelve un DataFrame normalizado.

    Salida:
    - timestamp: datetime
    - kwh: float
    - es_fin_semana: bool
    """
    if not path.exists():
        raise FileNotFoundError(f"No existe el fichero de consumo: {path}")

    # Lectura robusta de codificación (Windows)
    df = None
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin1"):
        try:
            df = pd.read_csv(path, sep=";", encoding=encoding)
            break
        except UnicodeDecodeError:
            continue

    if df is None:
        raise UnicodeDecodeError(
            "No se pudo leer el CSV de consumo con codificaciones habituales.",
            b"", 0, 1, "decode"
        )

    # Identificación de columnas (heurística simple)
    col_fecha = None
    col_hora = None
    col_kwh = None

    for c in df.columns:
        cl = c.strip().lower()
        if col_fecha is None and ("fecha" in cl or "date" in cl):
            col_fecha = c
        if col_hora is None and ("hora" in cl or "hour" in cl):
            col_hora = c
        if col_kwh is None and ("consumo" in cl or "kwh" in cl or "energia" in cl):
            col_kwh = c

    if not col_fecha or not col_hora or not col_kwh:
        raise ValueError(
            "No se han podido identificar columnas de fecha/hora/consumo en el CSV. "
            f"Columnas encontradas: {list(df.columns)}"
        )

    # Normalizar separadores de fecha (., -) a /
    fecha = df[col_fecha].astype(str).str.strip()
    fecha = (
        fecha.str.replace(".", "/", regex=False)
             .str.replace("-", "/", regex=False)
    )

    # Inferir si la fecha es dayfirst o yearfirst
    dayfirst, yearfirst = _inferir_formato_fecha(fecha)

    # Normalizar hora a HH:MM:SS
    hora_raw = df[col_hora].astype(str).str.strip()
    hora_norm = _normalizar_hora_a_hhmmss(hora_raw)

    # Caso especial 24:00 / 24:00:00
    mask_24 = hora_raw.isin(["24:00", "24:00:00"]) | hora_norm.isin(["24:00:00"])
    hora_norm.loc[mask_24] = "00:00:00"

    # Construcción de timestamp con formato mixto (robusto para variantes)
    ts = pd.to_datetime(
        fecha + " " + hora_norm,
        errors="raise",
        format="mixed",
        dayfirst=dayfirst,
        yearfirst=yearfirst,
    )

    # Si era 24:00, sumar un día
    ts.loc[mask_24] = ts.loc[mask_24] + pd.Timedelta(days=1)

    # kWh (admite coma o punto)
    kwh = (
        df[col_kwh]
        .astype(str)
        .str.strip()
        .str.replace(",", ".", regex=False)
    )
    kwh = pd.to_numeric(kwh, errors="raise")

    out = pd.DataFrame({"timestamp": ts, "kwh": kwh})
    out["es_fin_semana"] = out["timestamp"].dt.dayofweek >= 5  # 5=sábado, 6=domingo

    out = out.sort_values("timestamp").reset_index(drop=True)
    return out
