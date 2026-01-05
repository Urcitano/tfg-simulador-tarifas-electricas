# -*- coding: utf-8 -*-
"""
Normaliza el tiempo de 24.00 a 00.00 para indicar las 12 a.m.
"""

from __future__ import annotations
import pandas as pd


def build_timestamp_from_fecha_hora(
    df: pd.DataFrame,
    col_fecha: str,
    col_hora: str,
    formato_fecha: str = "%Y/%m/%d",
    normalizar_24h: bool = True,
) -> pd.DataFrame:
    """
    Construye 'timestamp' (datetime) a partir de fecha y hora.
    Regla: '24:00' -> '00:00' del día siguiente.
    """
    out = df.copy()

    if normalizar_24h:
        mask_24 = out[col_hora].astype(str).str.strip().eq("24:00")
        if mask_24.any():
            fechas = pd.to_datetime(out.loc[mask_24, col_fecha], format=formato_fecha, errors="coerce")
            if fechas.isna().any():
                raise ValueError("Se han detectado fechas inválidas en filas con hora 24:00.")
            out.loc[mask_24, col_fecha] = (fechas + pd.Timedelta(days=1)).dt.strftime(formato_fecha)
            out.loc[mask_24, col_hora] = "00:00"

    dt_str = out[col_fecha].astype(str).str.strip() + " " + out[col_hora].astype(str).str.strip()
    out["timestamp"] = pd.to_datetime(dt_str, format=f"{formato_fecha} %H:%M", errors="coerce")

    if out["timestamp"].isna().any():
        n_bad = int(out["timestamp"].isna().sum())
        raise ValueError(f"Se han detectado {n_bad} filas con timestamp inválido tras la normalización.")

    return out


def split_timestamp_fecha_hora(df: pd.DataFrame) -> pd.DataFrame:
    """Deriva 'fecha' (date) y 'hora' (0..23) a partir de 'timestamp'."""
    out = df.copy()
    out["fecha"] = out["timestamp"].dt.date
    out["hora"] = out["timestamp"].dt.hour.astype(int)
    return out
