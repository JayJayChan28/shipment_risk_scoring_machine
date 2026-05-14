import os
from dataclasses import dataclass, field
from dotenv import load_dotenv

load_dotenv()


@dataclass
class AISConfig:
    """Configuration for the AIS stream ingestion pipeline.

    All values are read from environment variables with sensible defaults.
    Copy .env.example to .env and fill in the required values before running.
    """

    # Required – no defaults; will be empty string if not set
    ais_api_key: str = field(default_factory=lambda: os.getenv("AIS_API_KEY", ""))
    s3_bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET", ""))

    # AWS settings
    aws_region: str = field(default_factory=lambda: os.getenv("AWS_REGION", "us-east-1"))
    s3_prefix: str = field(default_factory=lambda: os.getenv("S3_PREFIX", "raw/ais"))

    # Batching / flush settings
    batch_size: int = field(
        default_factory=lambda: int(os.getenv("BATCH_SIZE", "500"))
    )
    flush_interval_sec: int = field(
        default_factory=lambda: int(os.getenv("FLUSH_INTERVAL_SEC", "60"))
    )
    reconnect_delay_sec: int = field(
        default_factory=lambda: int(os.getenv("RECONNECT_DELAY_SEC", "5"))
    )

    # WebSocket endpoint (unlikely to change, but overridable)
    ws_url: str = field(
        default_factory=lambda: os.getenv(
            "AIS_WS_URL", "wss://stream.aisstream.io/v0/stream"
        )
    )
