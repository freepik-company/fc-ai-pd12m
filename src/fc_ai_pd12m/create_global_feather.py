import argparse
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import boto3
import polars as pl
import s3fs
from PIL import Image
from tqdm import tqdm

from fc_ai_pd12m.utils import safe_write_ipc

# To avoid "Image has too many pixels" error
Image.MAX_IMAGE_PIXELS = None


def parse_args():
    parser = argparse.ArgumentParser(description="Create a global feather file from pd12m dataset")

    # Filepath arguments
    parser.add_argument(
        "--input_folder",
        type=str,
        help="Path to the input folder containing parquet files. It can be a local path or a S3 path. If it is a S3 path, include s3:// and the bucket name on it",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        default="global_pd12m_data.feather",
        help="Path to the output feather file. It can be a local path or a S3 path. If it is a S3 path, include s3:// and the bucket name on it",
    )

    # Image arguments
    parser.add_argument(
        "--image_path_column",
        type=str,
        default="image_path",
        help="Name of the column that contains the image path",
    )
    parser.add_argument(
        "--image_extension",
        type=str,
        default=".jpg",
        help="Image extension. Please include the dot",
    )
    parser.add_argument(
        "--max_files",
        type=int,
        default=None,
        help="Maximum number of parquet files to process",
    )
    parser.add_argument(
        "--max_items",
        type=int,
        default=None,
        help="Maximum number of items to process per parquet file",
    )

    # Parallelism arguments
    parser.add_argument(
        "--num_workers",
        type=int,
        default=16,
        help="Number of workers to process parquet files in parallel",
    )

    # S3 arguments
    parser.add_argument(
        "--aws_region",
        type=str,
        default="gra",
        help="AWS region. Only used if --is_s3_folder is set",
    )
    parser.add_argument(
        "--aws_endpoint_url",
        type=str,
        default="https://s3.gra.io.cloud.ovh.net",
        help="AWS endpoint URL. Only used if --is_s3_folder is set",
    )
    return parser.parse_args()


def validate_args(opts: argparse.Namespace) -> argparse.Namespace:
    # Check that input folder exists
    if "s3://" in opts.input_folder:
        # TODO: fs.exists check fails with directories
        # if not s3_fs.exists(opts.input_folder):
        #     raise ValueError(f"Input folder {opts.input_folder} does not exist")
        pass
    else:
        if not Path(opts.input_folder).exists():
            raise ValueError(f"Input folder {opts.input_folder} does not exist")

    # Check that output file parent folder exists
    if "s3://" in opts.output_path:
        # TODO: fs.exists check fails with directories
        # if not s3_fs.exists(Path(opts.output_path).parent):
        #     raise ValueError(f"Output folder {Path(opts.output_path).parent} does not exist")
        pass
    else:
        if not Path(opts.output_path).parent.exists():
            Path(opts.output_path).parent.mkdir(parents=True, exist_ok=True)

    # Check that output file has a valid extension
    if Path(opts.output_path).suffix.lower() not in [".feather", ".fth"]:
        raise ValueError(f"Output file {opts.output_path} must have a .feather or .fth extension")

    # Check that max files is greater than 0
    if opts.max_files is not None and opts.max_files < 1:
        raise ValueError(f"Max files {opts.max_files} must be greater than 0")

    return opts


def get_ovh_s3_filesystem(opts: argparse.Namespace) -> tuple[s3fs.S3FileSystem, dict]:
    # Initialize boto3 S3 client
    try:
        session = boto3.session.Session(profile_name="default")
        credentials = session.get_credentials().get_frozen_credentials()
    except Exception as e:
        raise ValueError(f"Error authenticating with OVH S3: {e}")  # noqa: B904

    s3 = s3fs.S3FileSystem(
        key=credentials.access_key,
        secret=credentials.secret_key,
        endpoint_url=opts.aws_endpoint_url,
    )
    s3_storage_options = {
        "aws_access_key_id": credentials.access_key,
        "aws_secret_access_key": credentials.secret_key,
        "endpoint_url": opts.aws_endpoint_url,
        "aws_region": opts.aws_region,
    }

    return s3, s3_storage_options


def get_parquet_files(
    ds_folder: str,
    s3_fs: Optional[s3fs.S3FileSystem],
) -> list[str]:
    """
    Get a list of parquet files from a folder
    """

    # For S3
    if "s3://" in ds_folder:
        if s3_fs is not None:
            parquet_files = list(s3_fs.glob(ds_folder + "/*.parquet"))
            parquet_files = [f"s3://{file}" for file in parquet_files if s3_fs.exists(file)]
        else:
            raise ValueError("s3_fs is None, but an S3 path was provided.")
    else:
        parquet_files = list(Path(ds_folder).glob("*.parquet"))

    if len(parquet_files) == 0:
        raise ValueError(f"No parquet files found in {ds_folder}")
    print(f"Found {len(parquet_files)} parquet files in {ds_folder}")

    return parquet_files


def get_image_path_from_id(
    item_id: str,
    ds_folder: str,
    image_extension: str = ".jpg",
) -> str:
    return f"{ds_folder}/{item_id[:5]}/{item_id}{image_extension}"


def get_image_dimensions(image_path: str, image_folder: str, s3_fs: Optional[s3fs.S3FileSystem]) -> Optional[dict]:
    if image_path is None:
        return None

    abs_image_path = f"{image_folder}/{image_path}"
    img = None
    try:
        if "s3://" in abs_image_path:
            if s3_fs is not None:
                with s3_fs.open(abs_image_path, "rb") as f:
                    img = Image.open(f)
        else:
            img = Image.open(abs_image_path)
    except Exception as e:
        print(f"Error getting image dimensions for {image_path}: {e}")
        return None

    if img is not None:
        width, height = img.width, img.height
        ar = round(width / height, 2)
        return {"width": width, "height": height, "ar": ar}
    else:
        return None


def process_parquet(
    parquet_file: str,
    s3_fs: Optional[s3fs.S3FileSystem],
    s3_storage_options: Optional[dict],
    opts: argparse.Namespace,
) -> pl.DataFrame:
    """
    Process a single parquet file
    """

    def _get_image_path(item_id: str, image_extension: str = ".jpg") -> str:
        return f"{item_id[:5]}/{item_id}{image_extension}"

    # Read parquet file
    df = pl.read_parquet(parquet_file, storage_options=s3_storage_options)
    pd_df = df.to_pandas()

    if opts.max_items is not None and len(pd_df) > opts.max_items:
        pd_df = pd_df.sample(opts.max_items)

    # Rename columns
    pd_df = pd_df.rename(
        columns={
            "key": "item_id",
            "width": "image_width",
            "height": "image_height",
        }
    )

    # Get image path
    pd_df["image_path"] = pd_df.apply(lambda row: _get_image_path(row["item_id"]), axis=1)
    image_paths = pd_df["image_path"].tolist()

    # Process image dimensions in parallel
    start_time = time.time()
    with ThreadPoolExecutor(max_workers=opts.num_workers) as executor:
        futures = [
            executor.submit(get_image_dimensions, image_path, opts.input_folder, s3_fs) for image_path in image_paths
        ]
        image_dimensions = []
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing images", position=1, leave=True):
            if future.result():
                image_dimensions.append(future.result())

    elapsed_time = time.time() - start_time
    avg_time_per_image = elapsed_time / len(image_dimensions)
    print(
        f"Processed {parquet_file} in {elapsed_time:.2f} seconds. Average time per image: {avg_time_per_image:.2f} seconds"
    )

    # Add dimensions to dataframe
    pd_df["dimensions"] = image_dimensions
    pd_df = pd_df.dropna(subset=["dimensions"])

    pd_df["image_width"] = pd_df["dimensions"].apply(lambda x: x["width"] if x else None)
    pd_df["image_height"] = pd_df["dimensions"].apply(lambda x: x["height"] if x else None)
    pd_df["aspect_ratio"] = pd_df["dimensions"].apply(lambda x: x["ar"] if x else None)
    pd_df = pd_df.drop("dimensions", axis=1)

    # Convert back to polars
    return pl.from_pandas(pd_df)


def create_global_polars(
    s3_fs: Optional[s3fs.S3FileSystem],
    s3_storage_options: Optional[dict],
    opts: argparse.Namespace,
) -> None:
    # Read files from S3 or local folder
    print(f"Reading parquet files from {opts.input_folder}")
    parquet_files = get_parquet_files(
        ds_folder=opts.input_folder,
        s3_fs=s3_fs,
    )
    print(f"Found {len(parquet_files)} parquet files")

    # Calculate how many parquet files to read
    parquet_size = pl.read_parquet(parquet_files[0], storage_options=s3_storage_options).height
    print(f"Average rows per parquet file: {parquet_size}")

    if opts.max_files is not None and len(parquet_files) > opts.max_files:
        parquet_files = random.sample(parquet_files, opts.max_files)
        print(f"Processing {len(parquet_files)} parquet files")

    # Process parquet files
    total_df = pl.DataFrame()

    for parquet_file in tqdm(parquet_files, desc="Processing parquet files", position=0, leave=True):
        df = process_parquet(parquet_file, s3_fs, s3_storage_options, opts)
        if len(df) > 0:
            total_df = pl.concat([total_df, df])
    print(f"Total rows: {total_df.height}")

    # Write the DataFrame to a feather file
    try:
        safe_write_ipc(
            df,
            dest_path=opts.output_path,
            s3_fs=s3_fs,
        )
        print(f"Global Polars feather file created: {opts.output_path}")
    except Exception as e:
        print(f"Error writing to {opts.output_path}: {e}")
        raise e


def main() -> None:
    opts = parse_args()

    # Get OVH S3 filesystem
    if "s3://" in opts.input_folder or "s3://" in opts.output_path:
        s3_fs, s3_storage_options = get_ovh_s3_filesystem(opts)
    else:
        s3_fs, s3_storage_options = None, None

    # Validate arguments
    opts = validate_args(opts=opts)

    # Create global polars file
    tic = time.time()
    create_global_polars(s3_fs, s3_storage_options, opts)
    toc = time.time()
    print(f"Total time: {toc - tic:.2f} seconds")


if __name__ == "__main__":
    main()
