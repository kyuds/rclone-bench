import argparse
import subprocess

from bench import bench_utils, model_loading


def generate_file(bucket_name: str, prefix: str, size: int):
    """Generate a single file on S3 using rclone rcat."""
    filename = prefix + "/" + model_loading.FILE_NAME_TEMPLATE.format(size)
    print(f"   -> Generating {filename} ({size}GB)...")
    # Generate random ASCII via base64-encoded urandom
    # base64 expands by 4/3, so we read 3/4 of target size from urandom
    size_bytes = size * 1024 * 1024 * 1024
    urandom_bytes = (size_bytes * 3) // 4
    # Use 100MB chunks to support files up to ~976GB (S3 allows max 10,000 parts)
    cmd = (
        f"head -c {urandom_bytes} /dev/urandom | base64 -w0 | "
        f"head -c {size_bytes} | rclone rcat --s3-chunk-size 100M s3:{bucket_name}/{filename}"
    )
    subprocess.run(cmd, shell=True, check=True)
    return filename


def populate_bucket(bucket_name: str):
    """Generates dummy files directly into the S3 bucket sequentially."""
    print(f"📦 Populating bucket 's3:{bucket_name}' with dummy models...")

    for prefix in model_loading.PREFIXES:
        for size in model_loading.MODEL_SIZES_GB:
            generate_file(bucket_name, prefix, size)


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
    args = parser.parse_args()
    bench_utils.configure_rclone_s3()
    populate_bucket(args.bucket)


if __name__ == "__main__":
    main()
