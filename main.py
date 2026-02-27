from __future__ import annotations
import logging
import os
import sys

from src.writer import write_output

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

PROCESSOR = os.getenv("PROCESSOR", "chunked")
#PROCESSOR = os.getenv("PROCESSOR", "spark")

def resolve_path(raw: str) -> str:

    if os.path.exists(raw):
        return raw

    candidate = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "data", os.path.basename(raw)
    )
    if os.path.exists(candidate):
        logger.info("Resolved '%s' -> '%s'", raw, candidate)
        return candidate
    #exception handling if the file is not found then it should show up a error message and print the path checked.
    print(f"\nError: file not found: {raw}")
    print(f"  Tried: {raw}")
    print(f"  Tried: {candidate}")
    print(f"\n data file is at  data{os.sep}data.sql")
    print(f" python main.py data{os.sep}data.sql")
    sys.exit(1)


def main() -> None:
    if len(sys.argv) != 2:
        print(f"python main.py data{os.sep}data.sql")
        sys.exit(1)
        #this is the development requirement number 3 of the assignment that the code should run with single argument
    input_path = resolve_path(sys.argv[1])

    if PROCESSOR == "spark":
        from src.spark_processor import SparkProcessor
        processor = SparkProcessor(input_path)
    else:
        from src.processor import ChunkedProcessor
        processor = ChunkedProcessor(input_path)

    logger.info("Back-end: %s", processor.describe())

    revenue_map = processor.process()
    output_path = write_output(revenue_map, input_path)

    print(f"Output: {output_path}")


if __name__ == "__main__":
    main()
