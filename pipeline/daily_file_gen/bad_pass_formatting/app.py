import json
import logging
from typing import Dict
import pandas as pd
from utilities.aws_utils import aws_manager


def add_bad_passes(df: pd.DataFrame, item: Dict) -> pd.DataFrame:
    new_bad_passes = pd.DataFrame(
        [
            {
                "source": item["source"],
                "date": item["date"],
                "cycle": bad_pass["cycle"],
                "pass": bad_pass["pass_num"],
            }
            for bad_pass in item["bad_passes"]
        ]
    )
    df = pd.concat([df, new_bad_passes], ignore_index=True)
    return df


def remove_good_passes(df: pd.DataFrame, item: Dict) -> pd.DataFrame:
    logging.info(
        f"Checking if {item['source']} on {item['date']} was previously flagged..."
    )
    rows_to_drop = df.loc[
        (df["source"] == item["source"]) & (df["date"] == item["date"])
    ]
    if rows_to_drop.size > 0:
        logging.info(
            f'Removing {len(rows_to_drop)} previously flagged bad pass from {item["source"]} on {item["date"]}'
        )
        df = df.drop(index=rows_to_drop.index, axis=1)
    else:
        logging.info(
            f"No previously flagged passes for {item['source']} on {item['date']}"
        )
    return df


def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    try:
        # Load up existing bad pass document
        bad_pass_path = "s3://example-bucket/aux_files/bad_pass_list.csv"
        og_df = pd.read_csv(aws_manager.stream_obj(bad_pass_path))

        logging.info(f"There are currently {og_df.shape[0]} bad passes being flagged.")
        for i, row in og_df.iterrows():
            logging.info(f"{','.join(row.values.astype(str))}")

        df = og_df.copy()

        for item in event:
            # Bad passes have been flagged and need to be added
            if item["bad_passes"]:
                df = add_bad_passes(df, item)
            # Check if date had any previously detected bad passes - we want to remove them as they were fixed upstream
            else:
                df = remove_good_passes(df, item)

        # We need to remove duplicate rows - the same bad pass can be identified from multiple days
        duplicates = df.astype(str).duplicated()
        if duplicates.size > 0:
            df = df[~duplicates]

        logging.info(f"There are now {df.shape[0]} bad passes being flagged.")
        for i, row in df.iterrows():
            logging.info(f"{','.join(row.values.astype(str))}")

        # Upload updated file back to S3
        if not df.astype(str).equals(og_df.astype(str)):
            logging.info("Updates made to bad pass list. Pushing updates to S3...")
            df.to_csv("/tmp/bad_pass_list.csv", index=False)
            aws_manager.upload_obj("/tmp/bad_pass_list.csv", bad_pass_path)

        return {"statusCode": 200, "body": json.dumps("Hello from Lambda!")}

    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        print(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))
