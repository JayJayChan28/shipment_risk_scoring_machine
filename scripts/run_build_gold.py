import os

from dotenv import load_dotenv

from src.ETL.Gold_Layer import run_gold_build


def main() -> None:
    load_dotenv()

    bucket = os.getenv("S3_BUCKET", "shipment-risk-scoring")
    silver_prefix = os.getenv("S3_SILVER_PREFIX", "silver/ais")
    gold_prefix = os.getenv("S3_GOLD_PREFIX", "gold/ais")

    position_path = os.getenv(
        "SILVER_POSITION_PATH", f"s3://{bucket}/{silver_prefix}/position"
    )
    static_path = os.getenv(
        "SILVER_STATIC_PATH", f"s3://{bucket}/{silver_prefix}/static"
    )
    gold_summary_path = os.getenv(
        "GOLD_SUMMARY_PATH", f"s3://{bucket}/{gold_prefix}/vessel_day_summary"
    )
    gold_static_path = os.getenv(
        "GOLD_STATIC_PATH", f"s3://{bucket}/{gold_prefix}/vessel_static_latest"
    )
    gold_enriched_path = os.getenv(
        "GOLD_ENRICHED_PATH", f"s3://{bucket}/{gold_prefix}/enriched_positions"
    )

    print(f"[gold] silver position : {position_path}")
    print(f"[gold] silver static   : {static_path}")
    print(f"[gold] gold summary    : {gold_summary_path}")
    print(f"[gold] gold static     : {gold_static_path}")
    print(f"[gold] gold enriched   : {gold_enriched_path}")

    stats = run_gold_build(
        position_path=position_path,
        static_path=static_path,
        gold_summary_path=gold_summary_path,
        gold_static_path=gold_static_path,
        gold_enriched_path=gold_enriched_path,
    )
    print(f"[done] {stats}")


if __name__ == "__main__":
    main()
