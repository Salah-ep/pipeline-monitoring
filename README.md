# 📡 Pipeline Monitoring — Elasticsearch & Kibana

Surveillance en temps quasi réel d'un pipeline de données Python via la stack **Filebeat → Elasticsearch → Kibana**.

---

## 🏗️ Architecture

```
┌─────────────────┐     logs JSON      ┌──────────────┐     index      ┌───────────────────┐
│  Pipeline Python │ ────────────────► │   Filebeat   │ ─────────────► │  Elasticsearch    │
│  (ETL simulé)    │                   │  (collecte)  │                │  (stockage)       │
└─────────────────┘                   └──────────────┘                └────────┬──────────┘
                                                                                │
                                                                                ▼
                                                                       ┌───────────────────┐
                                                                       │      Kibana        │
                                                                       │  (dashboards)      │
                                                                       └───────────────────┘
```

---

## ⚙️ Stack technique

| Composant | Rôle |
|-----------|------|
| **Python 3.11** | Pipeline ETL (Extract, Transform, Load, Validate) |
| **Filebeat 8.12** | Collecte et parsing des logs JSON |
| **Elasticsearch 8.12** | Indexation et stockage des événements |
| **Kibana 8.12** | Visualisation, dashboards, alertes |
| **Docker Compose** | Orchestration des services |

---

## 📦 Structure du projet

```
pipeline-monitoring/
├── pipeline/
│   ├── data_pipeline.py      # Pipeline ETL principal
│   └── Dockerfile
├── filebeat/
│   └── filebeat.yml          # Config collecte & parsing
├── docker-compose.yml
├── requirements.txt          # Dépendances Python
├── .env.example
├── .gitignore
└── README.md
```

---

## 🚀 Lancement

### Prérequis
- Docker & Docker Compose installés
- ~2 Go de RAM disponibles

### Étapes

```bash
# 1. Cloner le repo
git clone https://github.com/Salah-ep/pipeline-monitoring.git
cd pipeline-monitoring

# 2. Configurer les variables d'environnement
cp .env.example .env
# Modifier .env si besoin (mot de passe Elasticsearch)

# 3. Lancer la stack complète
docker-compose up -d

# 4. Vérifier que tout est up
docker-compose ps
```

### Accès

| Service | URL | Identifiants |
|---------|-----|-------------|
| Kibana | http://localhost:5601 | elastic / *ton mot de passe* |
| Elasticsearch | http://localhost:9200 | elastic / *ton mot de passe* |

---

## 📊 Ce que monitore le pipeline

Le pipeline simule un ETL réaliste avec 4 étapes loguées :

| Étape | Métriques loguées |
|-------|------------------|
| **Extract** | Source, nombre d'enregistrements, durée (ms) |
| **Transform** | Enregistrements valides / rejetés, taux de rejet |
| **Load** | Destination, durée d'écriture |
| **Validate** | Score qualité (%), nulls, doublons |

### Exemple de log JSON produit

```json
{
  "timestamp": "2026-03-15T14:23:01.456Z",
  "level": "INFO",
  "logger": "data_pipeline",
  "message": "Transform terminé : 423 valides, 31 rejetés",
  "pipeline_id": "a1b2c3d4",
  "stage": "transform",
  "duration_ms": 12.4,
  "records_count": 423
}
```

---

## 📈 Dashboards Kibana suggérés

Une fois Kibana accessible, créer un **index pattern** `pipeline-logs-*` puis construire :

- **Vue globale** : volume de runs par heure, taux de succès/erreur
- **Performance** : durée moyenne par étape (Extract / Transform / Load)
- **Qualité des données** : score qualité moyen, taux de rejet par source
- **Alertes** : pic d'erreurs, latence anormale, score qualité < 85%

---

## 🛑 Arrêt

```bash
docker-compose down          # Arrêt sans supprimer les données
docker-compose down -v       # Arrêt + suppression des volumes
```

---

## 👤 Auteur

**Salah Eddine El Boukili**  
Élève ingénieur ISIMA — Spécialité SIAD  
[salaheddine00elboukili@gmail.com](mailto:salaheddine00elboukili@gmail.com)
