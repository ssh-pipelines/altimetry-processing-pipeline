import json
import logging
from typing import Dict, List, Optional, Set, Tuple
import pandas as pd
from utilities.aws_utils import aws_manager


def collect_bad_passes_to_add(items: List[Dict]) -> List[Dict]:
    """Collect all bad passes that need to be added from all items."""
    bad_passes_to_add = []
    
    for item in items:
        if item.get("bad_passes"):
            for bad_pass in item["bad_passes"]:
                bad_passes_to_add.append({
                    "source": item["source"],
                    "date": item["date"],
                    "cycle": bad_pass["cycle"],
                    "pass": bad_pass["pass_num"],
                })
    
    return bad_passes_to_add


def collect_passes_to_remove(items: List[Dict], existing_df: pd.DataFrame) -> Set[Tuple]:
    """
    Collect indices of passes that should be removed (fixed upstream).
    Returns a set of (source, date) tuples to remove.
    """
    passes_to_remove = set()
    
    for item in items:
        # If bad_passes is empty or missing, this date/source is now good
        if not item.get("bad_passes"):
            source = item["source"]
            date = item["date"]
            
            # Check if this source/date exists in the current data
            matching_rows = existing_df.loc[
                (existing_df["source"] == source) & (existing_df["date"] == date)
            ]
            
            if len(matching_rows) > 0:
                logging.info(
                    f"Marking {len(matching_rows)} previously flagged pass(es) "
                    f"from {source} on {date} for removal"
                )
                passes_to_remove.add((source, date))
            else:
                logging.info(f"No previously flagged passes for {source} on {date}")
    
    return passes_to_remove


def load_existing_bad_passes(bucket: str) -> Optional[pd.DataFrame]:
    """Load existing bad pass list from S3, return None if doesn't exist."""
    bad_pass_path = f"s3://{bucket}/aux_files/bad_pass_list.csv"
    
    if not aws_manager.key_exists(bad_pass_path):
        logging.info("No existing bad pass list found. Starting fresh.")
        return None
    
    df = pd.read_csv(aws_manager.stream_obj(bad_pass_path))
    logging.info(f"Loaded existing bad pass list with {len(df)} entries.")
    for _, row in df.iterrows():
        logging.info(f"  {','.join(row.values.astype(str))}")
    
    return df


def build_updated_dataframe(
    existing_df: Optional[pd.DataFrame],
    passes_to_add: List[Dict],
    passes_to_remove: Set[Tuple]
) -> pd.DataFrame:
    """
    Build the updated DataFrame in one pass.
    
    Strategy:
    1. Start with existing data (or empty DataFrame)
    2. Filter out passes marked for removal
    3. Append all new passes at once
    4. Remove duplicates
    """
    # Start with existing data or empty DataFrame
    if existing_df is not None and len(existing_df) > 0:
        # Filter out passes marked for removal
        if passes_to_remove:
            mask = ~existing_df.apply(
                lambda row: (row["source"], row["date"]) in passes_to_remove,
                axis=1
            )
            df = existing_df[mask].copy()
            removed_count = len(existing_df) - len(df)
            if removed_count > 0:
                logging.info(f"Removed {removed_count} previously flagged passes")
        else:
            df = existing_df.copy()
    else:
        df = pd.DataFrame(columns=["source", "date", "cycle", "pass"])
    
    # Add all new bad passes at once (single concatenation)
    if passes_to_add:
        new_passes_df = pd.DataFrame(passes_to_add)
        df = pd.concat([df, new_passes_df], ignore_index=True)
        logging.info(f"Added {len(passes_to_add)} new bad passes")
    
    # Remove duplicates
    original_len = len(df)
    df = df.drop_duplicates(subset=["source", "date", "cycle", "pass"], ignore_index=True)
    duplicates_removed = original_len - len(df)
    
    if duplicates_removed > 0:
        logging.info(f"Removed {duplicates_removed} duplicate entries")
    
    return df


def save_bad_passes(df: pd.DataFrame, bucket: str, original_df: Optional[pd.DataFrame]) -> None:
    """Save updated bad pass list to S3 if changes were made."""
    bad_pass_path = f"s3://{bucket}/aux_files/bad_pass_list.csv"
    
    # Check if there are actual changes
    if original_df is not None:
        # Sort both for comparison to ignore row order differences
        df_sorted = df.sort_values(by=["source", "date", "cycle", "pass"]).reset_index(drop=True)
        orig_sorted = original_df.sort_values(by=["source", "date", "cycle", "pass"]).reset_index(drop=True)
        
        if df_sorted.astype(str).equals(orig_sorted.astype(str)):
            logging.info("No changes to bad pass list. Skipping upload.")
            return
    
    logging.info(f"Final bad pass list contains {len(df)} entries:")
    for _, row in df.iterrows():
        logging.info(f"  {','.join(row.values.astype(str))}")
    
    # Save to S3
    logging.info("Pushing updates to S3...")
    df.to_csv("/tmp/bad_pass_list.csv", index=False)
    aws_manager.upload_obj("/tmp/bad_pass_list.csv", bad_pass_path)


def handler(event, context):
    """Lambda handler for managing bad pass list."""
    # Configure logging
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO",
        format="[%(levelname)s] %(asctime)s - %(message)s",
        handlers=[logging.StreamHandler()],
    )
    
    # Validate input
    bucket = event.get("bucket")
    if bucket is None:
        raise ValueError("bucket job parameter missing.")
    
    # Extract items (filter out non-dict entries like 'bucket')
    items = [item for item in event if isinstance(item, dict) and "source" in item]
    
    if not items:
        logging.warning("No valid items to process.")
        return {"statusCode": 200, "body": json.dumps("No items to process.")}
    
    try:
        # Load existing bad pass list
        original_df = load_existing_bad_passes(bucket)
        
        # Collect all changes to make (no DataFrame operations yet)
        passes_to_add = collect_bad_passes_to_add(items)
        passes_to_remove = collect_passes_to_remove(
            items, 
            original_df if original_df is not None else pd.DataFrame()
        )
        
        # Build updated DataFrame in one efficient pass
        updated_df = build_updated_dataframe(original_df, passes_to_add, passes_to_remove)
        
        # Save if changes were made
        save_bad_passes(updated_df, bucket, original_df)
        
        return {"statusCode": 200, "body": json.dumps("Bad pass list updated successfully.")}
        
    except Exception as e:
        error_response = {
            "status": "error",
            "errorType": type(e).__name__,
            "errorMessage": str(e),
            "input": event,
        }
        logging.error(f"Error: {error_response}")
        raise Exception(json.dumps(error_response))