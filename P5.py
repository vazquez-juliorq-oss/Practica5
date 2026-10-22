"""
Pipeline ETL automatizado con exportación a Parquet y CSV hacia AWS S3.

Este script ejecuta la extracción de datos, aplica feature engineering 
(discretización por rangos de edad y normalización MinMaxScaler) y genera 
archivos optimizados en formato Parquet y CSV para su almacenamiento en Amazon S3.
"""

import os
import boto3
import pandas as pd
from sklearn.preprocessing import MinMaxScaler


def pipeline_etl_completo():
    """Ejecuta las fases de Extracción, Transformación y Carga (ETL) a S3."""
    print("=== 1. FASE DE EXTRACCIÓN (Extract - Raw Data Staging) ===")
    raw_data = {
        "id_cliente": [101, 102, 103, 104, 105, 106, 107, 108],
        "cliente": [
            "Ana Gómez",
            "Carlos Ruiz",
            "Beatriz Peña",
            "David Soto",
            "Elena Lara",
            "Fernando Mas",
            "Gloria Ríos",
            "Hugo Vega",
        ],
        "edad": [20, 40, 60, 30, 22, 55, 35, 68],
        "monto_compras": [
            1200.0,
            50000.0,
            30000.0,
            15000.0,
            3500.0,
            82000.0,
            27000.0,
            45000.0,
        ],
    }
    df_raw = pd.DataFrame(raw_data)
    print(f"[OK] Total de registros extraídos: {len(df_raw)}")
    print("\nMuestra inicial de datos extraídos:")
    print(df_raw.head(4))
    print("\n")

    print("=== 2. FASE DE TRANSFORMACIÓN (Transform - Feature Engineering) ===")

    # Discretización (Binning) de edad
    print("--> Aplicando Discretización (Binning) en columna 'edad'...")
    df_raw["categoria_edad"] = pd.cut(
        df_raw["edad"],
        bins=[0, 25, 50, 100],
        labels=["Joven", "Adulto", "Senior"],
    )

    # Normalización con MinMaxScaler
    print("--> Aplicando Normalización MinMaxScaler sobre 'monto_compras'...")
    scaler = MinMaxScaler()
    df_raw["monto_normalizado"] = scaler.fit_transform(
        df_raw[["monto_compras"]]
    )

    # Cálculo de métrica derivada
    print("--> Derivando métrica de negocio 'score_promocional'...")
    df_raw["score_promocional"] = (df_raw["monto_normalizado"] * 100).round(2)

    print("\n>>> DATASET TRANSFORMADO Y PROCESADO <<<")
    print(df_raw.to_string(index=False))
    print("\n")

    print("=== 3. FASE DE CARGA (Load - Exportación a AWS S3 y Local) ===")

    # Lectura de variables de entorno para AWS
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "TU_AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv(
        "AWS_SECRET_ACCESS_KEY", "TU_AWS_SECRET_ACCESS_KEY"
    )
    AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN", "TU_AWS_SESSION_TOKEN")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "nombre-de-tu-bucket")

    # Exportación local de archivos
    parquet_path = "etl_output.parquet"
    df_raw.to_parquet(parquet_path, index=False, engine="pyarrow")
    size_parquet = os.path.getsize(parquet_path)
    print(
        f"[OK] Archivo Parquet generado localmente: '{parquet_path}'"
        f" ({size_parquet} bytes)"
    )

    csv_path = "etl_output.csv"
    df_raw.to_csv(csv_path, index=False)
    size_csv = os.path.getsize(csv_path)
    print(
        f"[OK] Archivo CSV generado localmente:     '{csv_path}' ({size_csv}"
        " bytes)"
    )

    # Transferencia a S3
    print(
        f"\n--> Conectando a AWS S3 y cargando al bucket: '{BUCKET_NAME}'..."
    )
    try:
        s3_client = boto3.client(
            "s3",
            region_name=AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
        )

        s3_client.upload_file(
            parquet_path, BUCKET_NAME, f"processed/{parquet_path}"
        )
        print(
            " [ÉXITO] Archivo Parquet subido a S3:"
            f" s3://{BUCKET_NAME}/processed/{parquet_path}"
        )

        s3_client.upload_file(csv_path, BUCKET_NAME, f"processed/{csv_path}")
        print(
            " [ÉXITO] Archivo CSV subido a S3:    "
            f" s3://{BUCKET_NAME}/processed/{csv_path}"
        )
    except Exception as e:
        print(f"[ERROR S3] Falló la carga a AWS S3: {e}")

    print("\n=== COMPARATIVA DE ALMACENAMIENTO ===")
    print(f"• Tamaño Parquet: {size_parquet} Bytes")
    print(f"• Tamaño CSV:     {size_csv} Bytes")


if __name__ == "__main__":
    pipeline_etl_completo()
