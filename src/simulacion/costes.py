# -*- coding: utf-8 -*-
"""
Módulo de simulación de costes.

Objetivo académico:
- Calcular el coste anual por tarifa.
- Calcular la mejor tarifa por mes.
- Añadir KPIs orientados a BI:
  - ahorro absoluto y relativo frente a:
    * mediana (referencia robusta)
    * peor tarifa (escenario desfavorable)
  - desglose porcentual del coste total:
    * % energía vs % potencia
  - distribución del consumo por periodos (kWh y %):
    * punta/llano/valle_LV/valle_SD

Convención:
- Ahorro positivo => la tarifa recomendada es mejor (más barata).
  ahorro = referencia - coste
"""

import pandas as pd


# --------------------------------------------------
# Utilidades internas
# --------------------------------------------------
def _clasificar_periodo(timestamp: pd.Series, es_fin_semana: pd.Series) -> pd.Series:
    horas = timestamp.dt.hour.astype(int)
    periodo = pd.Series(index=timestamp.index, dtype="object")

    # Fin de semana: todo valle
    periodo.loc[es_fin_semana] = "valle_SD"
    mask_lv = ~es_fin_semana

    # Valle L-V: 00-08
    periodo.loc[mask_lv & (horas < 8)] = "valle_LV"

    # Punta L-V: 10-14 y 18-22
    mask_punta = mask_lv & (((horas >= 10) & (horas < 14)) | ((horas >= 18) & (horas < 22)))
    periodo.loc[mask_punta] = "punta_LV"

    # Resto L-V: llano
    periodo.loc[mask_lv & periodo.isna()] = "llano_LV"

    if periodo.isna().any():
        raise ValueError("Error en la asignación de periodos horarios.")

    return periodo


def _safe_pct(parte: float, total: float) -> float:
    """Porcentaje (0-100) evitando división por cero."""
    if total == 0:
        return 0.0
    return (parte / total) * 100.0


def _anadir_kpis_ahorro(df: pd.DataFrame, col_coste: str, sufijo: str) -> pd.DataFrame:
    """
    Añade KPIs de ahorro vs:
    - mediana
    - peor
    """
    res = df.copy()

    mediana = float(res[col_coste].median())
    peor = float(res[col_coste].max())

    res[f"ref_mediana_coste_total_{sufijo}"] = mediana
    res[f"ref_peor_coste_total_{sufijo}"] = peor

    res[f"ahorro_vs_mediana_eur_{sufijo}"] = mediana - res[col_coste]
    res[f"ahorro_vs_peor_eur_{sufijo}"] = peor - res[col_coste]

    res[f"ahorro_vs_mediana_pct_{sufijo}"] = res[f"ahorro_vs_mediana_eur_{sufijo}"].map(
        lambda x: _safe_pct(float(x), mediana)
    )
    res[f"ahorro_vs_peor_pct_{sufijo}"] = res[f"ahorro_vs_peor_eur_{sufijo}"].map(
        lambda x: _safe_pct(float(x), peor)
    )

    return res


def _anadir_pct_energia_potencia(
    df: pd.DataFrame,
    col_energia: str,
    col_potencia: str,
    col_total: str,
    sufijo: str,
) -> pd.DataFrame:
    """
    Añade el desglose porcentual energía vs potencia sobre el total.
    Crea:
    - pct_coste_energia_<sufijo>
    - pct_coste_potencia_<sufijo>
    """
    res = df.copy()

    res[f"pct_coste_energia_{sufijo}"] = res.apply(
        lambda r: _safe_pct(float(r[col_energia]), float(r[col_total])),
        axis=1
    )
    res[f"pct_coste_potencia_{sufijo}"] = res.apply(
        lambda r: _safe_pct(float(r[col_potencia]), float(r[col_total])),
        axis=1
    )

    return res


def _anadir_distribucion_consumo(
    df: pd.DataFrame,
    kwh_punta: float,
    kwh_llano: float,
    kwh_valle_lv: float,
    kwh_valle_sd: float,
    total_kwh: float,
    sufijo_cols: str,
) -> pd.DataFrame:
    """
    Añade al DataFrame:
    - kWh por periodo
    - % kWh por periodo

    sufijo_cols permite diferenciar naming:
    - anual: sufijo_cols="" (sin sufijo adicional)
    - mensual: sufijo_cols="_mes"
    """
    res = df.copy()

    res[f"kwh_punta_LV{sufijo_cols}"] = kwh_punta
    res[f"kwh_llano_LV{sufijo_cols}"] = kwh_llano
    res[f"kwh_valle_LV{sufijo_cols}"] = kwh_valle_lv
    res[f"kwh_valle_SD{sufijo_cols}"] = kwh_valle_sd

    res[f"pct_kwh_punta_LV{sufijo_cols}"] = _safe_pct(kwh_punta, total_kwh)
    res[f"pct_kwh_llano_LV{sufijo_cols}"] = _safe_pct(kwh_llano, total_kwh)
    res[f"pct_kwh_valle_LV{sufijo_cols}"] = _safe_pct(kwh_valle_lv, total_kwh)
    res[f"pct_kwh_valle_SD{sufijo_cols}"] = _safe_pct(kwh_valle_sd, total_kwh)

    return res


# --------------------------------------------------
# Ranking anual
# --------------------------------------------------
def calcular_costes_anuales_por_tarifa(
    consumo_df: pd.DataFrame,
    tarifas_df: pd.DataFrame,
    potencia_kw_valle: float,
    potencia_kw_punta_llano: float,
) -> pd.DataFrame:

    df = consumo_df.copy()
    df["periodo"] = _clasificar_periodo(df["timestamp"], df["es_fin_semana"])

    kwh_periodo = df.groupby("periodo")["kwh"].sum().to_dict()

    kwh_punta = float(kwh_periodo.get("punta_LV", 0.0))
    kwh_llano = float(kwh_periodo.get("llano_LV", 0.0))
    kwh_valle_lv = float(kwh_periodo.get("valle_LV", 0.0))
    kwh_valle_sd = float(kwh_periodo.get("valle_SD", 0.0))

    total_kwh = float(kwh_punta + kwh_llano + kwh_valle_lv + kwh_valle_sd)

    resultados = []

    for _, t in tarifas_df.iterrows():
        # Coste variable de energía
        coste_energia = (
            kwh_punta * float(t["punta_LV"])
            + kwh_llano * float(t["llano_LV"])
            + kwh_valle_lv * float(t["valle_LV"])
            + kwh_valle_sd * float(t["valle_SD"])
        )

        # Coste fijo de potencia (mensual * kW * 12)
        coste_potencia = (
            float(t["potencia_mes_valle"]) * float(potencia_kw_valle) * 12.0
            + float(t["potencia_mes_punta_llano"]) * float(potencia_kw_punta_llano) * 12.0
        )

        total = coste_energia + coste_potencia
        coste_medio = (total / total_kwh) if total_kwh > 0 else 0.0

        resultados.append({
            "comercializadora": t["comercializadora"],
            "tarifa": t["tarifa"],
            "kwh_anuales": total_kwh,
            "coste_energia_anual": coste_energia,
            "coste_potencia_anual": coste_potencia,
            "coste_total_anual": total,
            "coste_medio_eur_kwh": coste_medio,
        })

    res = pd.DataFrame(resultados).sort_values("coste_total_anual").reset_index(drop=True)

    # KPIs ahorro (anual)
    res = _anadir_kpis_ahorro(res, "coste_total_anual", "anual")

    # KPI % energía vs potencia (anual)
    res = _anadir_pct_energia_potencia(
        res,
        col_energia="coste_energia_anual",
        col_potencia="coste_potencia_anual",
        col_total="coste_total_anual",
        sufijo="anual",
    )

    # Distribución de consumo por periodos (kWh y %): igual para todas las tarifas
    res = _anadir_distribucion_consumo(
        res,
        kwh_punta=kwh_punta,
        kwh_llano=kwh_llano,
        kwh_valle_lv=kwh_valle_lv,
        kwh_valle_sd=kwh_valle_sd,
        total_kwh=total_kwh,
        sufijo_cols="",  # anual
    )

    return res


# --------------------------------------------------
# Mejor tarifa por mes
# --------------------------------------------------
def calcular_mejor_tarifa_por_mes(
    consumo_df: pd.DataFrame,
    tarifas_df: pd.DataFrame,
    potencia_kw_valle: float,
    potencia_kw_punta_llano: float,
) -> pd.DataFrame:

    df = consumo_df.copy()
    df["periodo"] = _clasificar_periodo(df["timestamp"], df["es_fin_semana"])
    df["anio"] = df["timestamp"].dt.year.astype(int)
    df["mes"] = df["timestamp"].dt.month.astype(int)

    resultados = []

    for (anio, mes), g in df.groupby(["anio", "mes"]):
        kwh_map = g.groupby("periodo")["kwh"].sum().to_dict()

        kwh_punta = float(kwh_map.get("punta_LV", 0.0))
        kwh_llano = float(kwh_map.get("llano_LV", 0.0))
        kwh_valle_lv = float(kwh_map.get("valle_LV", 0.0))
        kwh_valle_sd = float(kwh_map.get("valle_SD", 0.0))
        total_kwh_mes = float(kwh_punta + kwh_llano + kwh_valle_lv + kwh_valle_sd)

        mejor = None
        costes_mes = []

        for _, t in tarifas_df.iterrows():
            coste_energia = (
                kwh_punta * float(t["punta_LV"])
                + kwh_llano * float(t["llano_LV"])
                + kwh_valle_lv * float(t["valle_LV"])
                + kwh_valle_sd * float(t["valle_SD"])
            )

            # Potencia mensual (no *12)
            coste_potencia = (
                float(t["potencia_mes_valle"]) * float(potencia_kw_valle)
                + float(t["potencia_mes_punta_llano"]) * float(potencia_kw_punta_llano)
            )

            total = coste_energia + coste_potencia
            costes_mes.append(total)

            if mejor is None or total < mejor["coste_total_mes"]:
                mejor = {
                    "anio": int(anio),
                    "mes": int(mes),
                    "comercializadora": t["comercializadora"],
                    "tarifa": t["tarifa"],
                    "kwh_mes": total_kwh_mes,
                    "coste_energia_mes": coste_energia,
                    "coste_potencia_mes": coste_potencia,
                    "coste_total_mes": total,
                }

        # Referencias del mes: mediana y peor (sobre todas las tarifas)
        serie = pd.Series(costes_mes, dtype="float64")
        ref_mediana = float(serie.median())
        ref_peor = float(serie.max())

        # KPIs ahorro del ganador (convención: ref - coste)
        ahorro_vs_mediana = ref_mediana - float(mejor["coste_total_mes"])
        ahorro_vs_peor = ref_peor - float(mejor["coste_total_mes"])

        mejor.update({
            "ref_mediana_coste_total_mes": ref_mediana,
            "ref_peor_coste_total_mes": ref_peor,
            "ahorro_vs_mediana_eur_mes": ahorro_vs_mediana,
            "ahorro_vs_mediana_pct_mes": _safe_pct(ahorro_vs_mediana, ref_mediana),
            "ahorro_vs_peor_eur_mes": ahorro_vs_peor,
            "ahorro_vs_peor_pct_mes": _safe_pct(ahorro_vs_peor, ref_peor),
        })

        resultados.append(mejor)

    res = pd.DataFrame(resultados).sort_values(["anio", "mes"]).reset_index(drop=True)

    # KPI % energía vs potencia (mes)
    res = _anadir_pct_energia_potencia(
        res,
        col_energia="coste_energia_mes",
        col_potencia="coste_potencia_mes",
        col_total="coste_total_mes",
        sufijo="mes",
    )

    # Distribución de consumo por periodos (kWh y %), por cada mes
    # Se calcula por mes, no depende de la tarifa ganadora.
    # Para añadirlo, lo recalculamos a partir de consumo_df filtrado por mes.
    # (Esto se hace de forma explícita para mantener claridad metodológica.)
    filas = []
    for _, row in res.iterrows():
        anio = int(row["anio"])
        mes = int(row["mes"])
        sub = df[(df["anio"] == anio) & (df["mes"] == mes)]
        km = sub.groupby("periodo")["kwh"].sum().to_dict()

        kwh_punta = float(km.get("punta_LV", 0.0))
        kwh_llano = float(km.get("llano_LV", 0.0))
        kwh_valle_lv = float(km.get("valle_LV", 0.0))
        kwh_valle_sd = float(km.get("valle_SD", 0.0))
        total_kwh_mes = float(kwh_punta + kwh_llano + kwh_valle_lv + kwh_valle_sd)

        extra = {
            "kwh_punta_LV_mes": kwh_punta,
            "kwh_llano_LV_mes": kwh_llano,
            "kwh_valle_LV_mes": kwh_valle_lv,
            "kwh_valle_SD_mes": kwh_valle_sd,
            "pct_kwh_punta_LV_mes": _safe_pct(kwh_punta, total_kwh_mes),
            "pct_kwh_llano_LV_mes": _safe_pct(kwh_llano, total_kwh_mes),
            "pct_kwh_valle_LV_mes": _safe_pct(kwh_valle_lv, total_kwh_mes),
            "pct_kwh_valle_SD_mes": _safe_pct(kwh_valle_sd, total_kwh_mes),
        }
        filas.append(extra)

    extra_df = pd.DataFrame(filas)
    res = pd.concat([res.reset_index(drop=True), extra_df.reset_index(drop=True)], axis=1)

    return res
