import dataclasses
import json
import os
import subprocess
import textwrap
import uuid
from pathlib import Path

import requests


RCLONE_CONFIG_DIR = Path.home() / ".config" / "rclone"
RCLONE_CONFIG_PATH = RCLONE_CONFIG_DIR / "rclone.conf"


@dataclasses.dataclass
class RCloneOptions:
    # Transfer parallelism
    transfers: int = 4
    multi_thread_streams: int = 4

    # Buffer settings
    buffer_size: str = "16M"

    # VFS cache settings
    vfs_cache_max_size: str = "10G"
    vfs_cache_max_age: str = "1h"
    vfs_write_back: str = "5s"

    # VFS read settings
    vfs_read_ahead: str = "128M"
    vfs_read_chunk_size: str = "128M"
    vfs_read_chunk_streams: int = 4

    # Other options
    fast_list: bool = False

    @classmethod
    def from_env(cls) -> "RCloneOptions":
        """Create RCloneOptions from environment variables."""
        def get_bool(key: str, default: bool) -> bool:
            val = os.environ.get(key, "").lower()
            if val in ("1", "true", "yes"):
                return True
            elif val in ("0", "false", "no"):
                return False
            return default

        return cls(
            transfers=int(os.environ.get("RCLONE_TRANSFERS", cls.transfers)),
            multi_thread_streams=int(os.environ.get("RCLONE_MULTI_THREAD_STREAMS", cls.multi_thread_streams)),
            buffer_size=os.environ.get("RCLONE_BUFFER_SIZE", cls.buffer_size),
            vfs_cache_max_size=os.environ.get("RCLONE_VFS_CACHE_MAX_SIZE", cls.vfs_cache_max_size),
            vfs_cache_max_age=os.environ.get("RCLONE_VFS_CACHE_MAX_AGE", cls.vfs_cache_max_age),
            vfs_write_back=os.environ.get("RCLONE_VFS_WRITE_BACK", cls.vfs_write_back),
            vfs_read_ahead=os.environ.get("RCLONE_VFS_READ_AHEAD", cls.vfs_read_ahead),
            vfs_read_chunk_size=os.environ.get("RCLONE_VFS_READ_CHUNK_SIZE", cls.vfs_read_chunk_size),
            vfs_read_chunk_streams=int(os.environ.get("RCLONE_VFS_READ_CHUNK_STREAMS", cls.vfs_read_chunk_streams)),
            fast_list=get_bool("RCLONE_FAST_LIST", cls.fast_list),
        )


def configure_rclone_s3(profile_name: str = "s3",
                        region: str = "us-east-1",
                        use_env_auth: bool = True) -> None:
    """Configure rclone with an S3 remote profile.

    This mimics how SkyPilot sets up rclone for MOUNT_CACHED mode.

    Args:
        profile_name: Name of the rclone profile (default: "s3")
        region: AWS region (default: "us-east-1")
        use_env_auth: If True, use AWS environment credentials (recommended).
                      If False, reads from ~/.aws/credentials.
    """
    RCLONE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    if use_env_auth:
        # Use environment-based auth (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        # or IAM role credentials. This is SkyPilot's default approach.
        config = textwrap.dedent(f"""\
            [{profile_name}]
            type = s3
            provider = AWS
            env_auth = true
            region = {region}
            acl = private
            """)
    else:
        # Read credentials from boto3/AWS SDK
        import boto3
        session = boto3.Session()
        credentials = session.get_credentials().get_frozen_credentials()
        config = textwrap.dedent(f"""\
            [{profile_name}]
            type = s3
            provider = AWS
            access_key_id = {credentials.access_key}
            secret_access_key = {credentials.secret_key}
            region = {region}
            acl = private
            """)

    # Check if profile already exists
    if RCLONE_CONFIG_PATH.exists():
        existing = RCLONE_CONFIG_PATH.read_text()
        if f"[{profile_name}]" in existing:
            print(f"rclone profile '{profile_name}' already exists, skipping.")
            return

    # Append config
    with open(RCLONE_CONFIG_PATH, "a") as f:
        f.write(config)
    print(f"Configured rclone profile '{profile_name}' for S3 (region={region})")


def get_rclone_stats():
    """Queries rclone RC for internal VFS stats."""
    try:
        r = requests.post("http://localhost:5572/vfs/stats")
        return r.json()
    except:
        return {}


def manage_bucket(action="create", bucket_name=None):
    """Automates S3 bucket lifecycle via rclone commands."""
    if action == "create":
        name = f"rclone-bench-{uuid.uuid4().hex[:8]}"
        subprocess.run(["rclone", "mkdir", f"s3:{name}"], check=True)
        return name
    elif action == "delete":
        subprocess.run(["rclone", "purge", f"s3:{bucket_name}"], check=True)


def get_mount_cmd(bucket: str, mount_path: str, options: RCloneOptions) -> list[str]:
    """Constructs the mount command with your specific flags."""
    cmd = [
        "rclone", "mount", f"s3:{bucket}", mount_path,
        "--vfs-cache-mode", "full",
        "--transfers", str(options.transfers),
        "--checkers", str(options.transfers * 2),
        "--multi-thread-streams", str(options.multi_thread_streams),
        "--buffer-size", options.buffer_size,
        "--vfs-cache-max-size", options.vfs_cache_max_size,
        "--vfs-cache-max-age", options.vfs_cache_max_age,
        "--vfs-read-ahead", options.vfs_read_ahead,
        "--vfs-read-chunk-size", options.vfs_read_chunk_size,
        "--vfs-read-chunk-streams", str(options.vfs_read_chunk_streams),
        "--vfs-write-back", options.vfs_write_back,
        "--rc",  # Required for stats
    ]
    if options.fast_list:
        cmd.append("--fast-list")
    return cmd


def log_to_jsonl(filename: str, data: dict):
    """Appends benchmark results to a JSONL file."""
    with open(filename, "a") as f:
        f.write(json.dumps(data) + "\n")
