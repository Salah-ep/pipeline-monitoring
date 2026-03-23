"""
data_pipeline.py
Pipeline de données Python simulant un ETL réaliste avec logging structuré.
Étapes : Extract → Transform → Load → Validation
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime

# ─── Configuration du logger (format JSON pour Filebeat) ─────────────────────

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline_id": getattr(record, "pipeline_id", None),
            "stage": getattr(record, "stage", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "records_count": getattr(record, "records_count", None),
            "error_type": getattr(record, "error_type", None),
        }
        # Nettoyer les champs None
        return json.dumps({k: v for k, v in log_entry.items() if v is not None})


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Handler fichier (lu par Filebeat)
    fh = logging.FileHandler("logs/pipeline.log")
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    # Handler console (pour le dev)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(ch)

    return logger


logger = get_logger("data_pipeline")


# ─── Simulation de données sources ───────────────────────────────────────────

SOURCES = ["api_crm", "api_sales", "db_postgres", "sftp_partner", "api_weather"]
REGIONS = ["EU-WEST", "US-EAST", "APAC", "AF-NORTH"]

def generate_raw_records(n: int) -> list[dict]:
    """Simule l'extraction de n enregistrements depuis une source."""
    records = []
    for _ in range(n):
        record = {
            "id": str(uuid.uuid4()),
            "source": random.choice(SOURCES),
            "region": random.choice(REGIONS),
            "value": round(random.uniform(-500, 10000), 2),
            "timestamp": datetime.utcnow().isoformat(),
            "corrupted": random.random() < 0.08,  # 8% de données corrompues
        }
        records.append(record)
    return records


# ─── Étapes du pipeline ───────────────────────────────────────────────────────

def extract(pipeline_id: str, source: str) -> list[dict]:
    """Étape Extract : simule la lecture depuis une source de données."""
    start = time.time()
    n = random.randint(80, 500)

    # Simule une latence réseau / base de données
    time.sleep(random.uniform(0.05, 0.3))

    # Simule une erreur de connexion aléatoire (5%)
    if random.random() < 0.05:
        raise ConnectionError(f"Impossible de se connecter à {source}")

    records = generate_raw_records(n)
    duration = round((time.time() - start) * 1000, 2)

    logger.info(
        f"Extract terminé depuis {source}",
        extra={
            "pipeline_id": pipeline_id,
            "stage": "extract",
            "duration_ms": duration,
            "records_count": len(records),
        },
    )
    return records


def transform(pipeline_id: str, records: list[dict]) -> list[dict]:
    """Étape Transform : nettoyage, validation, normalisation."""
    start = time.time()
    cleaned = []
    rejected = 0

    for r in records:
        if r.get("corrupted"):
            rejected += 1
            continue
        if r["value"] < 0:
            r["value"] = 0.0  # Normalisation
        r["value_normalized"] = round(r["value"] / 10000, 4)
        r["processed_at"] = datetime.utcnow().isoformat()
        cleaned.append(r)

    duration = round((time.time() - start) * 1000, 2)

    logger.info(
        f"Transform terminé : {len(cleaned)} valides, {rejected} rejetés",
        extra={
            "pipeline_id": pipeline_id,
            "stage": "transform",
            "duration_ms": duration,
            "records_count": len(cleaned),
        },
    )

    if rejected > len(records) * 0.15:
        logger.warning(
            f"Taux de rejet élevé : {rejected}/{len(records)} enregistrements",
            extra={"pipeline_id": pipeline_id, "stage": "transform"},
        )

    return cleaned


def load(pipeline_id: str, records: list[dict], destination: str) -> bool:
    """Étape Load : simule l'écriture vers une destination."""
    start = time.time()

    # Simule une erreur d'écriture (3%)
    if random.random() < 0.03:
        raise IOError(f"Échec d'écriture vers {destination} : timeout")

    time.sleep(random.uniform(0.02, 0.15))
    duration = round((time.time() - start) * 1000, 2)

    logger.info(
        f"Load terminé vers {destination}",
        extra={
            "pipeline_id": pipeline_id,
            "stage": "load",
            "duration_ms": duration,
            "records_count": len(records),
        },
    )
    return True


def validate(pipeline_id: str, records: list[dict]) -> dict:
    """Étape Validation : contrôles qualité post-load."""
    start = time.time()

    total = len(records)
    nulls = sum(1 for r in records if r["value_normalized"] is None)
    duplicates = total - len({r["id"] for r in records})
    if total == 0:
        logger.warning(
            "Aucun enregistrement à valider — pipeline vide",
            extra={"pipeline_id": pipeline_id, "stage": "validate"},
        )
        return {"quality_score": 0, "nulls": 0, "duplicates": 0}

    quality_score = round((1 - (nulls + duplicates) / total) * 100, 2)

    duration = round((time.time() - start) * 1000, 2)

    level = logging.INFO if quality_score >= 90 else logging.WARNING
    logger.log(
        level,
        f"Validation : score qualité {quality_score}%",
        extra={
            "pipeline_id": pipeline_id,
            "stage": "validate",
            "duration_ms": duration,
            "records_count": total,
        },
    )
    return {"quality_score": quality_score, "nulls": nulls, "duplicates": duplicates}


# ─── Orchestrateur principal ──────────────────────────────────────────────────

def run_pipeline(source: str, destination: str):
    pipeline_id = str(uuid.uuid4())[:8]
    logger.info(
        f"Démarrage du pipeline {pipeline_id} | {source} → {destination}",
        extra={"pipeline_id": pipeline_id, "stage": "start"},
    )

    try:
        records = extract(pipeline_id, source)
        records = transform(pipeline_id, records)
        load(pipeline_id, records, destination)
        validate(pipeline_id, records)
        logger.info(
            f"Pipeline {pipeline_id} terminé avec succès",
            extra={"pipeline_id": pipeline_id, "stage": "end"},
        )

    except (ConnectionError, IOError) as e:
        logger.error(
            str(e),
            extra={
                "pipeline_id": pipeline_id,
                "stage": "error",
                "error_type": type(e).__name__,
            },
        )


# ─── Boucle de simulation continue ───────────────────────────────────────────

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)

    print("🚀 Pipeline démarré — Ctrl+C pour arrêter\n")
    destinations = ["data_warehouse", "data_lake", "reporting_db", "ml_feature_store"]

    try:
        while True:
            source = random.choice(SOURCES)
            destination = random.choice(destinations)
            run_pipeline(source, destination)
            time.sleep(random.uniform(1, 4))  # Intervalle entre les runs
    except KeyboardInterrupt:
        print("Pipeline arrêté proprement.")
