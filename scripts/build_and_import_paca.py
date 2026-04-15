#!/usr/bin/env python3
"""
Pipeline parallèle : build.py + import Supabase pour tous les depts PACA.

Lance chaque département dans un processus séparé (build.py → import_parcelles_supabase.py),
en parallèle, avec un max de workers configurable.

Usage :
    python scripts/build_and_import_paca.py                   # tous les depts
    python scripts/build_and_import_paca.py --dept 13 83      # seulement ces depts
    python scripts/build_and_import_paca.py --workers 3        # 3 depts en parallèle (défaut: auto)
    python scripts/build_and_import_paca.py --build-only       # sans import Supabase
    python scripts/build_and_import_paca.py --import-only      # sans rebuild (fichiers déjà présents)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

# Import des chemins GPKG depuis download_paca.py
from download_paca import OCSGE_GPKG_PATHS  # noqa: E402

PACA_DEPTS = {
    "04": "Alpes-de-Haute-Provence",
    "05": "Hautes-Alpes",
    "06": "Alpes-Maritimes",
    "13": "Bouches-du-Rhône",
    "83": "Var",
    "84": "Vaucluse",
}

DEFAULT_OWNERS = ROOT / "data" / "parcelles-des-personnes-morales.geojson"
PYTHON = str(ROOT / ".venv" / "bin" / "python")


# ──────────────────────────────────────────────────────────────────────────────
# Fonctions worker (exécutées dans des sous-processus séparés)
# ──────────────────────────────────────────────────────────────────────────────

def _run(cmd: list[str], dept: str, step: str) -> tuple[bool, str]:
    """Exécute une commande et retourne (succès, output)."""
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(ROOT),
    )
    ok = result.returncode == 0
    out = result.stdout + (f"\n[stderr]\n{result.stderr}" if result.stderr.strip() else "")
    return ok, out


def process_dept(
    dept: str,
    owners: str,
    build_only: bool,
    import_only: bool,
    keep_geojson: bool,
    split_communes: bool = False,
    min_area: int = 5000,
) -> tuple[str, bool, str]:
    """
    Worker : build + import pour un département.
    Retourne (dept, succès, log).
    """
    log_lines: list[str] = []
    output_path = str(ROOT / "data" / f"pasture_{dept}.geojson")

    def log(msg: str) -> None:
        log_lines.append(msg)

    # ── Build ──────────────────────────────────────────────────────────────
    if not import_only:
        gpkg = OCSGE_GPKG_PATHS.get(dept)
        if not gpkg or not gpkg.exists():
            log(f"❌ GPKG introuvable pour dept {dept}: {gpkg}")
            return dept, False, "\n".join(log_lines)

        cmd_build = [
            PYTHON, "scripts/build.py",
            "--gpkg",     str(gpkg),
            "--owners",   owners,
            "--output",   output_path,
            "--min-area", str(min_area),
        ]
        # Passer --dept seulement pour les depts hors AMP (13 = AMP)
        if dept != "13":
            cmd_build += ["--dept", dept]
        if split_communes:
            cmd_build += ["--split-communes"]

        log(f"[{dept}] 🔨 build.py…")
        t0 = time.time()
        ok, out = _run(cmd_build, dept, "build")
        elapsed = time.time() - t0
        log(out)
        if not ok:
            log(f"[{dept}] ❌ build.py échoué en {elapsed:.0f}s")
            return dept, False, "\n".join(log_lines)
        log(f"[{dept}] ✅ build terminé en {elapsed:.0f}s")

    # ── Import Supabase ────────────────────────────────────────────────────
    if not build_only:
        if not Path(output_path).exists():
            log(f"[{dept}] ❌ Fichier GeoJSON manquant : {output_path}")
            return dept, False, "\n".join(log_lines)

        cmd_import = [
            PYTHON, "scripts/import_parcelles_supabase.py",
            "--geojson", output_path,
        ]
        log(f"[{dept}] 📤 import_parcelles_supabase.py…")
        t0 = time.time()
        ok, out = _run(cmd_import, dept, "import")
        elapsed = time.time() - t0
        log(out)
        if not ok:
            log(f"[{dept}] ❌ import échoué en {elapsed:.0f}s")
            return dept, False, "\n".join(log_lines)
        log(f"[{dept}] ✅ import terminé en {elapsed:.0f}s")

        # Nettoyage du GeoJSON intermédiaire (gros fichiers ~300-400 Mo)
        if not keep_geojson and not import_only:
            Path(output_path).unlink(missing_ok=True)
            log(f"[{dept}] 🗑  {output_path} supprimé")

    return dept, True, "\n".join(log_lines)


# ──────────────────────────────────────────────────────────────────────────────
# CLI + orchestrateur
# ──────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dept", nargs="+", choices=list(PACA_DEPTS.keys()),
                        help="Depts à traiter (défaut : tous)")
    parser.add_argument("--workers", type=int, default=None,
                        help="Nombre de depts en parallèle (défaut : min(nb_depts, 3))")
    parser.add_argument("--owners", default=str(DEFAULT_OWNERS),
                        help="Fichier GeoJSON des propriétaires")
    parser.add_argument("--build-only", action="store_true",
                        help="Build seulement, sans import Supabase")
    parser.add_argument("--import-only", action="store_true",
                        help="Import seulement (pasture_<dept>.geojson déjà présents)")
    parser.add_argument("--keep-geojson", action="store_true",
                        help="Conserver les GeoJSON intermédiaires après import")
    parser.add_argument("--split-communes", action="store_true",
                        help="Exporter aussi un GeoJSON par commune dans docs/data/communes/")
    parser.add_argument("--min-area", type=int, default=5000,
                        help="Surface minimale des parcelles en m² (défaut: 5000)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    depts = args.dept or list(PACA_DEPTS.keys())
    # --split-communes écrit dans docs/data/communes/index.json : séquentiel obligatoire
    workers = 1 if args.split_communes else (args.workers or min(len(depts), 3))

    print(f"\n🚀 Pipeline PACA — {len(depts)} depts × (build+import) — {workers} workers parallèles")
    print(f"   Depts : {', '.join(f'{d} ({PACA_DEPTS[d]})' for d in depts)}")
    if args.build_only:
        print("   Mode : build seulement (pas d'import Supabase)")
    elif args.import_only:
        print("   Mode : import seulement (GeoJSON déjà présents)")
    print("=" * 70)

    t_global = time.time()
    results: dict[str, tuple[bool, str]] = {}

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                process_dept,
                dept,
                args.owners,
                args.build_only,
                args.import_only,
                args.keep_geojson,
                args.split_communes,
                args.min_area,
            ): dept
            for dept in depts
        }

        for future in as_completed(futures):
            dept, ok, log = future.result()
            results[dept] = (ok, log)
            status = "✅" if ok else "❌"
            print(f"\n{'═' * 70}")
            print(f"{status} Dept {dept} — {PACA_DEPTS[dept]}")
            print(log)

    # Résumé final
    elapsed_total = time.time() - t_global
    print(f"\n{'═' * 70}")
    print(f"📊 Résumé ({elapsed_total:.0f}s au total) :")
    ok_count = sum(1 for ok, _ in results.values() if ok)
    for dept in depts:
        ok, _ = results.get(dept, (False, ""))
        print(f"  {'✅' if ok else '❌'} Dept {dept} — {PACA_DEPTS[dept]}")

    if ok_count < len(depts):
        failed = [d for d in depts if not results.get(d, (False,))[0]]
        print(f"\n⚠️  {len(depts) - ok_count} dept(s) en échec : {', '.join(failed)}")
        sys.exit(1)
    else:
        print(f"\n🎉 {ok_count}/{len(depts)} depts importés avec succès !")


if __name__ == "__main__":
    main()
