import dataclasses
import subprocess
import time
import os

from bench import bench_utils
from bench.bench_utils import RCloneOptions

# --- Configuration ---
MODEL_SIZES_GB = [10, 50, 100, 250, 500]
PREFIXES = ["model/b1", "model/b2", "model/b3", "model/b4"]
FILE_NAME_TEMPLATE = "model_{}gb.bin"
MOUNT_PATH = "./mnt_model_loading"
LOG_FILE = "model_loading_results.jsonl"


def run_benchmark(mount_path, size_gb):
    """Simulates loading a model by reading the entire file sequentially."""
    filename = f"model_{size_gb}gb.bin"
    file_path = os.path.join(mount_path, filename)
    
    print(f"🧪 Testing Load: {filename}...")
    
    # Wait for file to appear in mount (S3 eventual consistency check)
    timeout = 30
    while not os.path.exists(file_path) and timeout > 0:
        time.sleep(1)
        timeout -= 1

    start_time = time.time()
    bytes_read = 0
    chunk_size = 1024 * 1024 * 16 # 16MB read chunks
    
    try:
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                bytes_read += len(chunk)
    except Exception as e:
        print(f"❌ Error reading {filename}: {e}")
        return None

    end_time = time.time()
    duration = end_time - start_time
    throughput_mbps = (bytes_read * 8) / (duration * 1024 * 1024)
    
    # Collect internal Rclone VFS stats during/after run
    vfs_stats = bench_utils.get_rclone_stats()

    kpis = {
        "timestamp": time.time(),
        "model_size_gb": size_gb,
        "duration_sec": round(duration, 2),
        "throughput_mbps": round(throughput_mbps, 2),
        "bytes_read": bytes_read,
        "vfs_stats": vfs_stats
    }
    
    print(f"✅ KPI: {size_gb}GB loaded in {duration:.2f}s ({throughput_mbps:.2f} Mbps)")
    return kpis


def main():
    # 1. Setup
    options = RCloneOptions.from_env()
    bucket = manage_bucket("create")
    os.makedirs(MOUNT_PATH, exist_ok=True)
    
    try:
        # 2. Prepare Data
        populate_bucket(bucket, MODEL_SIZES_GB)

        # 3. Mount Rclone
        mount_cmd = get_mount_cmd(bucket, MOUNT_PATH, options)
        print(f"sh Mounting: {' '.join(mount_cmd)}")
        mount_proc = subprocess.Popen(mount_cmd)
        
        # Give mount time to initialize
        time.sleep(5)

        # 4. Run Tests
        for size in MODEL_SIZES_GB:
            result = run_benchmark(MOUNT_PATH, size)
            if result:
                # Merge options into log for reproducibility
                result["config"] = dataclasses.asdict(options)
                log_to_jsonl(result)

    finally:
        # 5. Cleanup
        print("🧹 Cleaning up...")
        subprocess.run(["umount", "-l", MOUNT_PATH]) # Lazy unmount
        if 'mount_proc' in locals():
            mount_proc.terminate()
        manage_bucket("delete", bucket)
        print("✨ Done.")


if __name__ == "__main__":
    main()
