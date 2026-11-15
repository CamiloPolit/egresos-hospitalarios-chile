"""Ejecuta el pipeline completo en orden. Uso: .venv/bin/python src/run_all.py"""
import subprocess
import sys
import time
from pathlib import Path

SRC = Path(__file__).resolve().parent
PASOS = [
    "01_cie10_lookup.py",
    "02_ingesta.py",
    "03_calidad.py",
    "04_dias_cama.py",
    "05_comparacion_regional.py",
    "06_covid.py",
    "graficos.py",
    "build_informe.py",
]
RUIDO = ("WARN", "setLogLevel", "NativeCodeLoader", "Setting default log level")


def main() -> int:
    t0 = time.time()
    for paso in PASOS:
        print(f"\n===== {paso} =====", flush=True)
        t = time.time()
        proc = subprocess.Popen([sys.executable, str(SRC / paso)], cwd=SRC,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for linea in proc.stdout:
            if not any(r in linea for r in RUIDO):
                print(linea, end="", flush=True)
        if proc.wait() != 0:
            print(f"\nERROR en {paso} (código {proc.returncode}). Pipeline detenido.")
            return proc.returncode
        print(f"({time.time() - t:.0f}s)")
    print(f"\nPipeline completo en {time.time() - t0:.0f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
