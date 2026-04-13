"""
Download required JARs for the PySpark streaming consumer.

Downloads:
    - PostgreSQL JDBC driver 42.7.3 → jars/postgresql-42.7.3.jar

The JAR is also copied into PySpark's bundled jars directory so it is
automatically on the classpath without needing --jars in spark-submit.
This works around a Java 23 incompatibility where --jars triggers Hadoop's
FileSystem path resolution, which calls Subject.getSubject() — an API
removed in Java 23 (JEP 486).

Usage:
    python streaming/download_jars.py
"""

import logging
import shutil
import sys
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

JARS_DIR = Path(__file__).parent.parent / "jars"

_MVN = "https://repo1.maven.org/maven2"

JARS = [
    # PostgreSQL JDBC driver — used by PySpark foreachBatch to write to Postgres
    {
        "name": "postgresql-42.7.3.jar",
        "url": f"{_MVN}/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar",
    },
    # Kafka connector for PySpark Structured Streaming (Scala 2.13 / PySpark 4.x)
    {
        "name": "spark-sql-kafka-0-10_2.13-4.1.1.jar",
        "url": f"{_MVN}/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar",
    },
    {
        "name": "spark-token-provider-kafka-0-10_2.13-4.1.1.jar",
        "url": f"{_MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.1/spark-token-provider-kafka-0-10_2.13-4.1.1.jar",
    },
    {
        "name": "kafka-clients-3.9.1.jar",
        "url": f"{_MVN}/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar",
    },
    {
        "name": "commons-pool2-2.12.1.jar",
        "url": f"{_MVN}/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar",
    },
    {
        "name": "lz4-java-1.8.0.jar",
        "url": f"{_MVN}/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar",
    },
    {
        "name": "snappy-java-1.1.10.8.jar",
        "url": f"{_MVN}/org/xerial/snappy/snappy-java/1.1.10.8/snappy-java-1.1.10.8.jar",
    },
    {
        "name": "scala-parallel-collections_2.13-1.2.0.jar",
        "url": f"{_MVN}/org/scala-lang/modules/scala-parallel-collections_2.13/1.2.0/scala-parallel-collections_2.13-1.2.0.jar",
    },
]


def download_jar(name: str, url: str) -> Path:
    dest = JARS_DIR / name
    if dest.exists():
        log.info("%s already present (%.1f MB), skipping.", name, dest.stat().st_size / 1_048_576)
        return dest

    log.info("Downloading %s ...", name)
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with open(dest, "wb") as fh:
        for chunk in response.iter_content(chunk_size=8192):
            fh.write(chunk)

    log.info("Saved %s (%.1f MB)", dest, dest.stat().st_size / 1_048_576)
    return dest


def install_into_pyspark(jar_path: Path) -> None:
    """Copy the JAR into PySpark's bundled jars directory so it is always on the classpath."""
    try:
        import pyspark  # noqa: PLC0415  (local import — pyspark only available in streaming-venv)

        pyspark_jars = Path(pyspark.__file__).parent / "jars"
        dest = pyspark_jars / jar_path.name
        if dest.exists():
            log.info("%s already in PySpark jars dir, skipping copy.", jar_path.name)
        else:
            shutil.copy2(jar_path, dest)
            log.info("Copied %s → %s", jar_path.name, pyspark_jars)
    except ImportError:
        log.warning(
            "PySpark not importable from this venv — skipping pyspark/jars copy."
            " Run this script from the .streaming-venv if needed."
        )


def main() -> None:
    JARS_DIR.mkdir(parents=True, exist_ok=True)
    try:
        for jar in JARS:
            jar_path = download_jar(jar["name"], jar["url"])
            install_into_pyspark(jar_path)
        log.info("All JARs ready in %s/", JARS_DIR)
    except Exception as exc:
        log.error("Failed to download JARs: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
