import argparse
import random
import string
import subprocess
import time

from bench import bench_utils
from bench.dataset_read import NUM_FILES, SHARDS

# --- Config for Dataset Ingestion ---
FILE_SIZE_BYTES = 4096  # 4KB
FILES_PER_SHARD = NUM_FILES // SHARDS

def generate_small_file_rcat(bucket_name, shard_index, file_index, data_buffer):
    """Generates a small 4KB file on S3 using rclone rcat."""
    filename = f"dataset/shard_{shard_index}/d_{file_index}.bin"
    remote_path = f"s3:{bucket_name}/{filename}"
    
    # We pipe the pre-generated buffer into rcat
    # shell=True is used here to support the echo/pipe pattern
    cmd = f"echo '{data_buffer}' | rclone rcat {remote_path}"
    
    subprocess.run(cmd, shell=True, check=True, capture_output=True)
    return filename

def populate_dataset(bucket_name):
    """Generates 10,000 small files into the S3 bucket across multiple shards."""
    print(f"📦 Populating bucket 's3:{bucket_name}' for Dataset Ingestion...")
    
    # Pre-generate a 4KB random alphanumeric string
    chars = string.ascii_letters + string.digits
    data_buffer = ''.join(random.choices(chars, k=FILE_SIZE_BYTES))
    
    start_total = time.time()
    
    for s in range(SHARDS):
        shard_start = time.time()
        print(f"📂 Shard {s}/{SHARDS-1}: Generating {FILES_PER_SHARD} files...")
        
        for i in range(FILES_PER_SHARD):
            generate_small_file_rcat(bucket_name, s, i, data_buffer)
            
            # Print progress every 100 files
            if (i + 1) % 100 == 0:
                print(f"   -> {i + 1}/{FILES_PER_SHARD} files uploaded...")
                data_buffer = ''.join(random.choices(chars, k=FILE_SIZE_BYTES))
        
        shard_duration = time.time() - shard_start
        print(f"✅ Shard {s} complete in {shard_duration:.2f}s ({FILES_PER_SHARD/shard_duration:.2f} files/sec)")

    total_duration = time.time() - start_total
    print(f"\n✨ Dataset generation complete!")
    print(f"📊 Total Time: {total_duration:.2f}s")
    print(f"📊 Average Throughput: {NUM_FILES / total_duration:.2f} files/sec")

if __name__ == "__main__":
    # Ensure this matches your confirmed bucket name
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
    populate_dataset(args.bucket)
