import argparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

from bench import model_loading


def generate_file(bucket_name: str, prefix: str, size: int):
    """Generate a single file on S3."""
    filename = prefix + "/" + model_loading.FILE_NAME_TEMPLATE.format(size)
    print(f"   -> Generating {filename}...")
    subprocess.run([
        "rclone", "test", "makefile",
        f"{size}G", f"s3:{bucket_name}/{filename}",
        "--ascii"
    ], check=True)
    return filename


def populate_bucket(bucket_name: str, workers: int = 4):
    """Generates dummy files directly into the S3 bucket using rclone test makefile."""
    print(f"📦 Populating bucket 's3:{bucket_name}' with dummy models...")

    tasks = [
        (bucket_name, prefix, size)
        for prefix in model_loading.PREFIXES
        for size in model_loading.MODEL_SIZES_GB
    ]

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(generate_file, *task) for task in tasks]
        for future in as_completed(futures):
            future.result()  # raises if failed


def main():
    parser = argparse.ArgumentParser(
        description="Populate an S3 bucket with dummy model files for benchmarking."
    )
    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="Name of the S3 bucket to populate with dummy model files."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel uploads (default: 4)."
    )
    args = parser.parse_args()
    populate_bucket(args.bucket, args.workers)


if __name__ == "__main__":
    main()
