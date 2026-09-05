# Query Engine Over Raw Files

## 1. C'est quoi

Une stack lakehouse construite from scratch : des fichiers Parquet bruts posés sur du stockage objet deviennent une table SQL versionnée, sans base de données au milieu. Le problème résolu est celui-ci : on a des fichiers, on veut du SQL dessus, avec schéma, historique de versions et statistiques, sans copier les données dans un moteur propriétaire. Contrairement à une base traditionnelle, le stockage (MinIO) et le calcul (Trino) sont deux systèmes séparés et remplaçables indépendamment — les données restent des fichiers Parquet ouverts, interrogeables par n'importe quel moteur qui parle Iceberg.

## 2. Architecture

```
┌─────────────────┐
│  Raw Parquet     │  yellow_tripdata_2023-{01..12}.parquet
│  (data/raw/)     │  téléchargé depuis NYC TLC
└────────┬─────────┘
         │ boto3 (src/upload_to_minio.py)
         ▼
┌─────────────────┐
│      MinIO       │  stockage objet compatible S3 (port 9000)
│  s3://lakehouse  │  fichiers bruts + fichiers de données Iceberg
└────────┬─────────┘
         │ fichiers Parquet (data) + manifests Avro (métadonnées)
         ▼
┌──────────────────────────┐
│  Iceberg metadata         │  schéma, snapshots, manifest lists,
│  (sur MinIO)               │  partition spec (month, identity)
└────────┬───────────────────┘
         │ commits versionnés, une branche = un état
         ▼
┌──────────────────────────┐
│  Nessie (catalog REST)    │  fait autorité sur la version courante
│  :19120                   │  des métadonnées pour chaque table
└────────┬───────────────────┘
         │ PyIceberg REST / Trino discovery
         ▼
┌────────────────────────────────────────┐
│               Trino :8080                │
│  ┌──────────────┐     ┌───────────────┐ │
│  │ coordinator   │◄───►│    worker      │ │
│  │ (trino)       │     │ (trino-worker) │ │
│  └──────────────┘     └───────────────┘ │
└─────────────────┬─────────────────────────┘
                   │ SQL (trino CLI, client Python)
                   ▼
          ┌──────────────────┐
          │  Résultats SQL    │
          └──────────────────┘
```

- **MinIO** : stocke les Parquet bruts et les fichiers de données/métadonnées Iceberg, exposés en API S3.
- **Iceberg metadata** : couche de fichiers qui donne aux Parquet bruts un schéma typé, un historique de snapshots et des statistiques par fichier.
- **Nessie** : catalogue REST versionné façon Git, qui pointe vers la version courante des métadonnées Iceberg pour `nyc_taxi.yellow_trips`.
- **Trino coordinator** : reçoit le SQL, planifie les fragments, distribue les splits aux workers.
- **Trino worker** : exécute les tâches (lecture Parquet, agrégation) et renvoie les résultats au coordinator.
- **PyIceberg / client Python** : écrit les données (ingestion) et lit les résultats (benchmarks).

## 3. Stack

| Outil | Version | Rôle |
|---|---|---|
| MinIO | RELEASE.2025-09-07T16-13-09Z | Stockage objet S3 pour les fichiers bruts et les données Iceberg |
| Nessie | 0.108.4 | Catalogue REST Iceberg versionné (branches, commits) |
| Trino | 483 | Moteur SQL distribué (1 coordinator + 1 worker) |
| PyIceberg | 0.12.0 | Client Python pour créer/écrire les tables Iceberg |
| Python | 3.13.3 | Runtime des scripts d'ingestion et de benchmark |
| PySpark | 3.5.6 (bitnamilegacy/spark) | Second moteur SQL distribué, benchmarké contre Trino (voir section 6) |
| DuckDB | prévu, non installé | Moteur SQL local à comparer avec Trino (voir section 11) |
| pyarrow | 24.0.0 | Lecture/écriture Parquet, conversions et casts de schéma |
| boto3 | 1.43.89 | Upload des fichiers bruts vers MinIO |
| trino (client Python) | 0.339.0 | Exécution des requêtes de benchmark |
| pandas | 2.3.3 | Mise en forme des résultats de requêtes |
| Docker Compose | — | Orchestration locale des 5 services |

## 4. Données

- NYC TLC Yellow Taxi Trip Records, année 2023 complète (12 fichiers mensuels).
- **38 310 226 lignes** chargées dans `iceberg.nyc_taxi.yellow_trips`.
- Partitionnée par `month`, colonne entière dérivée de `tpep_pickup_datetime` au chargement — le fichier source n'a pas de colonne `month`.

Problèmes de qualité réels rencontrés (pas hypothétiques, ils ont fait planter les scripts) :

- **Dérive de casse** : `airport_fee` (janvier) devient `Airport_fee` dès le fichier de février. PyIceberg compare les colonnes par nom exact, donc l'`append` échouait avec `PyArrow table contains more columns`.
- **Dérive de type** : `passenger_count` et `RatecodeID` sont en `double` dans le fichier de janvier, en `bigint` (int64) dans les fichiers suivants. `long → double` n'est pas une promotion de type autorisée par Iceberg — il faut un cast explicite avant chaque `append`, pas juste un renommage de colonne.
- **Timestamps aberrants** : quelques lignes de janvier et février ont un `tpep_pickup_datetime` tombant sur un autre mois (ex. 42 lignes classées `month=3` avant même le chargement du fichier de mars). Pollution mineure mais réelle de toutes les partitions.

## 5. Résultats des benchmarks

Mesurés avec `EXPLAIN ANALYZE` sur le cluster réel (`docker exec trino trino --execute ...`), colonne "Temps" = phase Execution (hors queue/planning).

| Requête | Nœuds | Splits | Temps | Lignes scannées |
|---|---|---|---|---|
| `COUNT(*)` sans filtre | 1 | 63 | 0.183s | 38 310 226 (0 B — stats seules) |
| `COUNT(*)` sans filtre | 2 | 63 | 0.274s | 38 310 226 (0 B) |
| `COUNT(*) WHERE month=6` | 1 | 3 | 0.201s | 3 307 259 (0 B) |
| `COUNT(*) WHERE month=6` | 2 | 3 | 0.218s | 3 307 259 (0 B) |
| `AVG` fare+distance par mois | 1 | 94 | 0.699s | 38 310 226 (840 MB) |
| `AVG` fare+distance par mois | 2 | 94 | 0.887s | 38 310 226 (840 MB) |
| Top 10 zones de départ | 1 | 94 | 0.390s | 38 310 226 (329 MB) |
| Top 10 zones de départ | 2 | 94 | 1.150s | 38 310 226 (329 MB) |
| Top 10 zones, `WHERE month=6` | 2 | 6 | 3.210s* | 3 307 259 (28 MB) |

*mesuré juste après le test de panne worker (section 6) — latence probablement gonflée par la reprise du cluster, gardée telle quelle par honnêteté plutôt que ré-exécutée dans de bonnes conditions.

## 6. Comparaison des moteurs de requêtes

PySpark a été ajouté comme second moteur de requête, benchmarké sur les 4 mêmes requêtes que la section 5, contre les mêmes 38 310 226 lignes. Contrairement à Trino, Spark ne passe pas par Nessie/Iceberg : `src/benchmark_spark.py` fait un `spark.read.parquet()` direct sur les fichiers de données de la table (`s3a://lakehouse/nyc_taxi/yellow_trips_<uuid>/data/`), sans catalogue, sans manifests, sans statistiques par fichier.

Résultats mesurés (`make benchmark-all`, cluster réel, `EXPLAIN ANALYZE`/SQL metrics sur les deux moteurs) :

| Requête | Trino | Spark | Gagnant |
|---|---|---|---|
| Benchmark 1 — `COUNT(*)` sans filtre | 0.264s | 2.171s | Trino |
| Benchmark 2 — `COUNT(*) WHERE month=6` | 0.315s | 1.023s | Trino |
| Benchmark 3 — `AVG` fare+distance par mois | 2.636s | 2.588s | Spark |
| Benchmark 4 — Top 10 zones de départ | 1.049s | 1.316s | Trino |

Les deux moteurs renvoient des résultats identiques ligne pour ligne (mêmes moyennes par mois, même top 10 des zones) — validation croisée que les deux lisent bien les mêmes données.

**Rows/files scanned, mesurés côté Spark** (`numOutputRows`/`numFiles` du nœud `FileSourceScanExec`, extraits du plan d'exécution physique) :

| Requête | Fichiers ouverts | Lignes émises par le scan |
|---|---|---|
| Benchmark 1 (sans filtre) | 63 | 38 310 226 |
| Benchmark 2 (`month=6`) | 63 | 3 307 259 |

**Pourquoi Trino gagne sur les benchmarks 1, 2 et 4 :**

- **Pas de manifest côté Spark** : Trino/Iceberg répond au `COUNT(*)` sans filtre depuis les statistiques du manifest (0 octet de Parquet lu, section 7) et élague 63 → 3 fichiers sur `month=6` avant même de planifier la lecture. Spark, en lisant les Parquet bruts sans catalogue, doit lister et ouvrir les **63 fichiers à chaque requête**, filtre ou non — le tableau ci-dessus le montre : 63 fichiers ouverts aussi bien pour le scan complet que pour `month=6`. Le filtre réduit bien les lignes réellement décodées (3 307 259 contre 38 310 226, grâce au predicate pushdown Parquet au niveau row-group), mais pas le nombre de fichiers touchés — c'est exactement l'écart que l'Iceberg manifest est censé éliminer, et qu'on retrouve ici en négatif.
- **JVM froide à chaque run** : cette machine n'a pas de JVM installée, donc `benchmark_spark.py` ne peut pas tourner comme les autres scripts du projet (client Python sur l'hôte) — voir le commentaire dans le fichier. Il s'exécute via `spark-submit` **à l'intérieur** du conteneur `spark-master` (`make benchmark-spark` / `make benchmark-all`), ce qui veut dire une nouvelle `SparkSession`, un nouveau driver JVM et un plan Catalyst recompilé à froid pour chaque exécution du script. Trino, à l'inverse, tourne en service permanent déjà chaud (JIT compilé, connexions établies) auquel le client se contente de se connecter. Une partie de l'écart sur les requêtes courtes (benchmarks 1, 2, 4) est ce coût de démarrage, pas une différence de moteur de calcul pur.
- **Ressources non comparables** : `spark-worker` est explicitement capé à 2 cœurs / 2 Go de RAM dans `docker-compose.yml`, alors que `trino`/`trino-worker` n'ont aucune limite déclarée sur ce même laptop. Ce n'est pas un banc d'essai à ressources égales.

**Pourquoi Spark rattrape (et gagne légèrement) sur le benchmark 3 :** c'est la seule requête qui doit décoder `fare_amount` et `trip_distance` sur les 38 310 226 lignes complètes, quel que soit le moteur — l'avantage "metadata-only" de l'Iceberg manifest ne s'applique plus, puisqu'aucun filtre ne permet d'éluder des fichiers. Les deux moteurs finissent alors dans un mouchoir de poche (2.636s vs 2.588s, moins de 2% d'écart) : à volume de données égal et sans filtre exploitable, c'est le débit brut de décodage/agrégation qui domine, pas la couche catalogue.

**Limite reconnue de cette comparaison** : elle n'oppose pas "Trino" à "Spark" dans l'absolu, mais "Trino+Iceberg+Nessie" (catalogue, manifests, pruning) à "Spark sans catalogue, lisant du Parquet brut". Un vrai lecteur Iceberg pour Spark (`iceberg-spark-runtime`, catalogue Nessie) retrouverait probablement le même pruning manifest que Trino sur les benchmarks 1 et 2 — ce n'est pas ce qui a été testé ici.

## 7. Observations clés des tests

- **Partition pruning réel** : sur une requête metadata-only (`COUNT`), 63 → 3 splits (-95%) avec le filtre `month=6`. Sur une requête qui lit vraiment les colonnes (top 10 zones), 94 → 6 splits (-94%). Le filtre élimine directement la majorité des fichiers de données à la planification, avant toute lecture.
- **`COUNT(*)` sans filtre lit 0 B de Parquet**, avec 1 nœud comme avec 2. Trino répond depuis les statistiques du manifest Iceberg (nombre de lignes par fichier de données), sans ouvrir un seul fichier Parquet. Pas "juste le footer" — aucune lecture de données du tout.
- **2 nœuds plus lent qu'1 nœud** sur les 4 comparaisons directes (+50% à +195% selon la requête). Coordinator, worker, MinIO et Nessie tournent sur le même laptop et se partagent CPU/RAM/disque ; le coût de coordination réseau (échange de résultats entre tâches, `RemoteSource`) dépasse le gain de parallélisme sur des requêtes qui prennent déjà moins de 1,5s à un seul nœud. Le gain d'un second nœud ne se justifierait qu'à partir d'un volume où le calcul domine largement la coordination.
- **Panne du worker en pleine requête** : requête volontairement lourde lancée (`CROSS JOIN UNNEST` pour multiplier le volume traité), puis `docker kill trino-worker` pendant l'exécution. Résultat réel observé : la requête a échoué après **18 tentatives ratées** de contact avec le worker mort, **62.21s de durée d'échec**, 64.21s de temps de requête cumulé sur ces échecs, 83.36s de temps total, avec un message d'erreur explicite (`Encountered too many errors talking to a worker node... please retry your query`). Ce n'est pas de la tolérance aux pannes au sens `fault-tolerant-execution` de Trino — la requête échoue, elle n'est pas ré-exécutée automatiquement — mais l'échec est propre, borné dans le temps, et exploitable plutôt qu'un blocage silencieux.
- **Bugs data réels** (détaillés section 4) : dérive de casse `airport_fee` → `Airport_fee` dès février 2023, dérive de type `passenger_count`/`RatecodeID` `double` ↔ `int64` selon les mois, timestamps aberrants polluant chaque partition mensuelle de quelques dizaines de lignes.

## 8. Comment lancer

```bash
git clone <repo>
cd "Query engine over raw files"

# 1. Configurer les identifiants (ne jamais committer le vrai .env)
cp .env.example .env
# éditer .env avec vos propres MINIO_ROOT_USER / MINIO_ROOT_PASSWORD

# 2. Lancer la stack (minio, nessie, trino, trino-worker, spark-master, spark-worker)
docker compose up -d
docker compose ps   # attendre que minio et nessie soient healthy

# 3. Installer les dépendances Python
pip install -r requirements.txt

# 4. Récupérer le jeu de données (data/raw/ est gitignored)
#    télécharger yellow_tripdata_2023-01.parquet à -12.parquet depuis
#    https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
#    et les placer dans data/raw/

# 5. Pipeline complet : upload → table Iceberg → 12 mois → benchmarks
make pipeline
```

`make pipeline` enchaîne `upload_to_minio.py`, `register_iceberg_table.py` (crée la table avec janvier), `load_full_year.py` (charge février à décembre, idempotent) et `benchmark.py`. Chaque script reste exécutable seul. L'UI Trino est sur `http://localhost:8080`, la console MinIO sur `http://localhost:9001`, l'UI Spark master sur `http://localhost:8082`, l'UI applicative Spark (active pendant l'exécution d'un job) sur `http://localhost:4040`.

Benchmark PySpark, contre le même `iceberg.nyc_taxi.yellow_trips` :

```bash
make benchmark-spark   # benchmark_spark.py seul
make benchmark-all     # benchmark.py (Trino) + benchmark_spark.py (Spark), tableau comparatif
```

`benchmark_spark.py` ne se lance pas comme les autres scripts (`python src/...`) : cette machine n'a pas de JVM, or le driver PySpark en a besoin même en mode client contre un master distant. `make benchmark-spark` exécute donc le script via `spark-submit` **à l'intérieur** du conteneur `spark-master` (monté en lecture seule sur `./src:/app`), pas sur l'hôte. Voir section 6 pour le détail de ce que ça change dans la comparaison.

Tests unitaires (pas besoin de la stack Docker — tout est mocké : boto3, PyIceberg, trino) :

```bash
pip install -r requirements-dev.txt
make test
```

## 9. Structure du projet

```
.
├── docker-compose.yml         # 6 services : minio, nessie, trino, trino-worker, spark-master, spark-worker + volumes
├── Makefile                   # up/down/logs/restart/status/pipeline/benchmark-spark/benchmark-all/test
├── requirements.txt           # dépendances Python épinglées (runtime)
├── requirements-dev.txt       # + pytest, pour lancer les tests
├── .env.example                # gabarit d'identifiants (le vrai .env est gitignored)
├── data/
│   ├── raw/                   # Parquet source, gitignored (12 fichiers mensuels)
│   └── parquet/               # vide, réservé pour des exports futurs
├── docs/                      # vide
├── tests/
│   ├── conftest.py               # ajoute src/ au sys.path
│   ├── test_upload_to_minio.py   # object_exists/ensure_bucket/get_client, boto3 mocké
│   ├── test_register_iceberg_table.py  # partition spec, dérivation du mois, namespace
│   ├── test_load_full_year.py    # download, et le vrai bug casse+type (fixture dédiée)
│   ├── test_query_with_trino.py  # run_query avec un curseur trino mocké
│   └── test_benchmark.py         # run_benchmark avec un curseur trino mocké
├── trino/
│   ├── catalog/
│   │   ├── iceberg.properties.template   # gabarit versionné (placeholders ${VAR})
│   │   └── iceberg.properties            # config réelle, gitignored (identifiants)
│   └── worker/
│       ├── node.properties    # node.id=${ENV:HOSTNAME}, propre à chaque worker
│       └── config.properties  # coordinator=false, discovery.uri=http://trino:8080
└── src/
    ├── upload_to_minio.py        # upload d'un Parquet vers MinIO, idempotent
    ├── register_iceberg_table.py # crée le namespace/table Iceberg, charge janvier
    ├── load_full_year.py         # télécharge + charge février à décembre
    ├── query_with_trino.py       # 3 requêtes de démonstration via trino
    ├── benchmark.py              # 4 benchmarks Trino avec splits/temps/lignes scannées
    ├── benchmark_spark.py        # mêmes 4 benchmarks via PySpark, lecture Parquet directe (s3a://)
    └── benchmark_comparison.py   # lance les deux, tableau comparatif Trino vs Spark
```

## 10. Ce que j'ai appris

- **Séparation stockage/calcul** : MinIO et Trino sont deux systèmes indépendants — on peut redémarrer, mettre à l'échelle ou remplacer l'un sans toucher l'autre, tant que le catalogue (Nessie) reste la source de vérité sur l'état des tables.
- **Le partition pruning est une décision à l'écriture, pas à la requête** : le `PartitionSpec` (colonne `month`, transform identity) est fixé à la création de la table dans `register_iceberg_table.py`. Impossible de l'ajouter après coup sans réécrire les fichiers de données.
- **Nessie a des exigences de version pour le REST catalog** : l'image par défaut (`projectnessie/nessie` en v0.76.6) n'exposait aucun endpoint Iceberg REST — seulement l'API native utilisée par le connecteur Trino/Nessie. Il a fallu passer sur `ghcr.io/projectnessie/nessie:latest` (résolu en 0.108.4) et configurer explicitement `nessie.catalog.*` pour que PyIceberg puisse s'y connecter.
- **Permissions du volume RocksDB** : un volume Docker nommé est créé `root:root` par défaut. L'image Nessie tourne en uid 10000 non-root — `Permission denied` au démarrage. Corrigé avec un conteneur `busybox` jetable pour faire le `chown`.
- **Une coquille dans un nom de variable échoue silencieusement** : `NESSIE_VERSION_STORE_PERSIST_ROCKS_DB_DATABASE_PATH` (avec un `_DB` en trop) au lieu de `..._ROCKS_DATABASE_PATH` — Nessie n'a rien signalé, il est simplement retombé sur `/tmp/nessie-rocksdb-store`, un chemin éphémère. Ça a coûté une perte complète du catalogue au redémarrage suivant, découverte seulement en lisant les logs de boot ligne par ligne.
- **2 nœuds plus lent qu'1 nœud, mesuré et reproductible** sur ce hardware partagé (voir section 6) — un rappel qu'ajouter un worker n'est pas gratuit tant que la charge ne justifie pas le coût de coordination.
- **Les jeux de données publics réels ont une dérive de schéma entre fichiers** : casse de colonne et type qui changent d'un mois à l'autre sur le même dataset officiel (NYC TLC). Aucun pipeline d'ingestion sérieux ne peut supposer un schéma stable sans vérifier.
- **`bitnami/spark` a disparu de Docker Hub en 2025** : Bitnami a déplacé ses images gratuites vers `bitnamilegacy/*` (figées, non maintenues) en réservant `bitnami/*` à un catalogue payant. `docker-compose.yml` pointe donc sur `bitnamilegacy/spark:3.5.6`, pas `bitnami/spark:3.5` — le tag documenté dans la plupart des tutoriels encore en ligne ne résout plus.
- **L'emplacement d'une table Iceberg n'est pas `{namespace}/{table}`** : le catalogue REST de Nessie ajoute un UUID (`yellow_trips_f0a50da6-...`) pour éviter les collisions entre create/drop. Un lecteur qui passe par le catalogue (Trino) ne le voit jamais ; un lecteur qui lit le Parquet brut directement (notre benchmark Spark) doit le résoudre lui-même — `benchmark_spark.py` liste le répertoire du namespace via l'API Hadoop FileSystem plutôt que de coder en dur un chemin qui casserait à la prochaine recréation de table.
- **UID de conteneur sans entrée `/etc/passwd` → Hadoop plante à l'authentification** : `bitnamilegacy/spark` tourne en uid 1001 (`whoami: cannot find name for user ID 1001`). `UserGroupInformation.getCurrentUser()` de Hadoop passe par le `UnixLoginModule` de Java, qui échoue avant même que `HADOOP_USER_NAME` puisse s'appliquer (`NullPointerException: invalid null input: name`). Contourné en exécutant `spark-submit` en `-u 0` (root) via `docker compose exec` — acceptable pour un cluster de dev jetable sur un seul laptop, pas pour un déploiement multi-utilisateurs.

## 11. Ce qui suit

- **Compaction** : `load_full_year.py` fait un `append` par mois — réécrire les petits fichiers en taille optimale (`rewrite_data_files`) avant que le nombre de fichiers ne dégrade les performances de scan.
- **Benchmark DuckDB vs Trino** sur les mêmes requêtes, pour comparer un moteur embarqué mono-processus à un moteur distribué sur cette taille de données.
- **Spark avec un vrai lecteur Iceberg** (`iceberg-spark-runtime` + catalogue Nessie, au lieu de `spark.read.parquet()` brut) pour vérifier si le pruning manifest élimine l'écart observé section 6 sur les benchmarks 1 et 2.
- **Ajouter plus d'années** (2019-2023, ~180M lignes) pour un vrai test de montée en charge, au-delà des ~38M lignes actuelles.
- **Migration vers le cloud réel** : MinIO → S3, Trino sur EC2 — pour mesurer l'écart entre ce setup local et une exécution avec latence réseau réelle.
