from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from pathlib import Path

from src.config import OUTPUT_SUFFIX, OUTPUT_HEADER, TSV_DELIMITER

logger = logging.getLogger(__name__)


def _is_s3(path: str) -> bool:
    return path.startswith("s3://")


def _write_local(content: str, output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        fh.write(content)


def _write_s3(content: str, s3_path: str) -> None:
    import boto3
    path = s3_path[len("s3://"):]
    bucket, key = path.split("/", 1)
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=content.encode("utf-8"),
        ContentType="text/plain",
    )


def write_output(
    revenue_map: dict[tuple[str, str], float],
    input_path: str,
) -> str:

    #date_str    = datetime.now().strftime("%Y-%m-%d")
    date_str   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"{date_str}{OUTPUT_SUFFIX}"

    if _is_s3(input_path):
        base = input_path.rstrip("/")
        output_path = f"{base}/{filename}"
    else:
        output_path = str(Path(input_path).parent / filename)

    sorted_rows = sorted(revenue_map.items(), key=lambda x: x[1], reverse=True)

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=TSV_DELIMITER)
    writer.writerow(OUTPUT_HEADER)
    for (domain, keyword), revenue in sorted_rows:
        writer.writerow([domain, keyword, f"{revenue:.2f}"])
    content = buf.getvalue()

    if _is_s3(output_path):
        _write_s3(content, output_path)
    else:
        _write_local(content, output_path)

    logger.info("Output -> %s  (%d rows)", output_path, len(sorted_rows))
    return output_path

'''
#older logic working fine for local execution and on ec2 instance without spark changes and glue job as it doesnt require the s3 bucket path


import csv
import logging
from datetime import datetime
from pathlib import Path

from src.config import OUTPUT_SUFFIX, OUTPUT_HEADER, TSV_DELIMITER

logger = logging.getLogger(__name__)


def write_output(
    revenue_map: dict[tuple[str, str], float],
    input_path: str,
) -> str:
    #date_str    = datetime.now().strftime("%Y-%m-%d")
    date_str   = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_path = str(Path(input_path).parent / f"{date_str}{OUTPUT_SUFFIX}")

    sorted_rows = sorted(revenue_map.items(), key=lambda x: x[1], reverse=True)

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=TSV_DELIMITER)
        writer.writerow(OUTPUT_HEADER)
        for (domain, keyword), revenue in sorted_rows:
            writer.writerow([domain, keyword, f"{revenue:.2f}"])

    logger.info("Output → %s  (%d rows)", output_path, len(sorted_rows))
    return output_path
'''




"""
This change which i made is specific when we scale up to run on glue job  and it 

Supports both local filesystem and S3 paths  i.e Local: input_path = /tmp/data.sql        
and  S3:    input_path = s3://bucket/input/
"""

