"""
colima_setup_minio.py — Upload landing files, configs, and Spark scripts to MinIO.

Usage:
    python scripts/colima_setup_minio.py
"""

import os
import subprocess
import sys

PROJECT_ROOT = os.path.join(os.path.dirname(__file__), "..")
BUCKET = "lakehouse"
BUCKET_PREFIX = "sg-life-idm"
MINIO_ALIAS = "local"


def run_cmd(cmd, check=True):
    """Run shell command via subprocess."""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if check and result.returncode != 0:
        print(f"[ERROR] {cmd}")
        print(result.stderr)
        sys.exit(1)
    return result.stdout.strip()


def main():
    print(f"[INFO] Using MinIO alias: {MINIO_ALIAS}")
    print(f"[INFO] Bucket: {BUCKET}")

    # 1. Create bucket if not exists
    print("\n[1/4] Creating bucket...")
    run_cmd(f"mc mb {MINIO_ALIAS}/{BUCKET}", check=False)
    print(f"      ✓ Bucket ready: {MINIO_ALIAS}/{BUCKET}")

    # 2. Upload config files
    print("\n[2/4] Uploading schema configs...")
    for org in ["ail", "lnl"]:
        config_dir = os.path.join(PROJECT_ROOT, "config", org)
        for file in os.listdir(config_dir):
            if file.endswith(".json"):
                local_path = os.path.join(config_dir, file)
                remote_path = f"{MINIO_ALIAS}/{BUCKET}/{BUCKET_PREFIX}/config/{org}/{file}"
                run_cmd(f"mc cp {local_path} {remote_path}")
                print(f"      ✓ {org}/{file}")

    # 3. Upload Spark scripts
    print("\n[3/4] Uploading Spark scripts...")
    scripts = [
        "colima_parse_csv.py",
        "colima_transform.py",
    ]
    for script in scripts:
        local_path = os.path.join(PROJECT_ROOT, "scripts", script)
        if os.path.exists(local_path):
            remote_path = f"{MINIO_ALIAS}/{BUCKET}/{BUCKET_PREFIX}/scripts/{script}"
            run_cmd(f"mc cp {local_path} {remote_path}")
            print(f"      ✓ {script}")
        else:
            print(f"      ⚠ {script} not found (will be created later)")

    # 4. Upload landing data (if exists in scratch/data/)
    print("\n[4/4] Uploading landing data...")
    for org in ["ail", "lnl"]:
        data_dir = os.path.join(PROJECT_ROOT, "scratch", "data", org)
        if os.path.exists(data_dir):
            for file in os.listdir(data_dir):
                if file.endswith(".csv"):
                    local_path = os.path.join(data_dir, file)
                    remote_path = f"{MINIO_ALIAS}/{BUCKET}/{BUCKET_PREFIX}/landing/{org}/{file}"
                    run_cmd(f"mc cp {local_path} {remote_path}")
                    print(f"      ✓ {org}/{file}")
        else:
            print(f"      ⚠ {org} data dir not found; run generate_data.py first")

    print("\n[OK] Upload complete.")
    print(f"\nVerify: mc ls {MINIO_ALIAS}/{BUCKET}/{BUCKET_PREFIX}/ --recursive")


if __name__ == "__main__":
    main()
