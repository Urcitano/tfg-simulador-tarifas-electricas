# -*- coding: utf-8 -*-
"""
Configuración general
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ProjectPaths:
    """Rutas del proyecto: entradas (data) y salidas (out)."""
    project_root: Path

    @property
    def data_dir(self) -> Path:
        return self.project_root / "data"

    @property
    def out_dir(self) -> Path:
        return self.project_root / "out"

    @property
    def out_graficas_dir(self) -> Path:
        return self.out_dir / "graficas"

    @property
    def out_export_bi_dir(self) -> Path:
        return self.out_dir / "export_bi"


@dataclass(frozen=True)
class DatadisCSVConfig:
    """Configuración típica de CSV Datadis."""
    sep: str = ";"
    decimal: str = ","
    quotechar: str = '"'
    encoding: str = "utf-8"


@dataclass(frozen=True)
class NormalizacionConfig:
    """Reglas de normalización temporal."""
    normalizar_24h: bool = True
