from __future__ import annotations

import logging
import sys

from awsglue.utils import getResolvedOptions

from src.spark_processor import SparkProcessor
from src.writer import write_output

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run(input_path: str, output_path: str) -> None:
    processor = SparkProcessor(input_path)
    logger.info("Starting Glue job | input=%s | output=%s", input_path, output_path)

    revenue_map = processor.process()

    result = write_output(revenue_map, output_path)
    logger.info("Glue job complete → %s", result)


if __name__ == "__main__":
    args = getResolvedOptions(sys.argv, ["INPUT_PATH", "OUTPUT_PATH"])
    run(input_path=args["INPUT_PATH"], output_path=args["OUTPUT_PATH"])