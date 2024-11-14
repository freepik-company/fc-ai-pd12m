import argparse
import math
from pathlib import Path
from typing import Optional

import boto3
import polars as pl
import s3fs
from PIL import Image

from fc_ai_pd12m.utils import safe_write_ipc


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

    # Processing arguments
    # parser.add_argument(
    #     "--skip_existing",
    #     action="store_true",
    #     help="Skip existing files",
    # )
    parser.add_argument(
        "--max_items",
        type=int,
        default=None,
        help="Maximum number of items to process",
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

    # Check that max items is greater than 0
    if opts.max_items is not None and opts.max_items < 1:
        raise ValueError(f"Max items {opts.max_items} must be greater than 0")

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


def w_pbar(pbar, func):
    def foo(*args, **kwargs):
        pbar.update(1)
        return func(*args, **kwargs)

    return foo


def read_dataframe_from_parquet_files(
    ds_folder: str,
    s3_fs: Optional[s3fs.S3FileSystem],
    s3_storage_options: Optional[dict],
    max_items: Optional[int] = None,
) -> pl.DataFrame:
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

    # Open first one to get the schema
    df = pl.read_parquet(parquet_files[0], storage_options=s3_storage_options)
    df_schema = df.schema
    parquet_size = df.height
    del df

    # # To know the total number of rows, open last parquet file and get the number of rows
    # last_df = pl.read_parquet(parquet_files[-1], storage_options=s3_storage_options)
    # last_parquet_size = last_df.height
    # del last_df

    # # Get total number of rows
    # total_rows = parquet_size * (len(parquet_files) - 1) + last_parquet_size

    # Calculate how many parquet files to read
    if max_items is not None:
        num_parquet_files = min(math.ceil(max_items / parquet_size), len(parquet_files))
    else:
        num_parquet_files = len(parquet_files)

    # TODO: Randomly select parquet files to read without opening all of them
    parquet_files_to_read = parquet_files[:num_parquet_files]

    # Read polars
    df = pl.concat(
        [
            pl.read_parquet(
                file,
                schema=df_schema,
                allow_missing_columns=True,
                storage_options=s3_storage_options,
            )
            for file in parquet_files_to_read
        ]
    )

    # If max items is set, slice the dataframe
    if max_items is not None and len(df) > max_items:
        df = df.slice(0, max_items)

    return df


def get_image_path_from_id(
    item_id: str,
    ds_folder: str,
    image_extension: str = ".jpg",
) -> str:
    return f"{ds_folder}/{item_id[:5]}/{item_id}{image_extension}"


def get_image_dimensions(image_path: str, image_folder: str, s3_fs: Optional[s3fs.S3FileSystem]) -> dict:
    """
    Get image dimensions and aspect ratio

    Args:
        image_path (str): Path to the image
        image_folder (str): Folder containing the images
        s3_fs (s3fs.S3FileSystem): S3 filesystem

    Returns:
        dict: Image width, image height, and aspect ratio
    """

    if image_path is None:
        return {"width": None, "height": None, "ar": None}

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
        return {"width": None, "height": None, "ar": None}

    if img is not None:
        width, height = img.width, img.height
        ar = round(width / height, 2)
        return {"width": width, "height": height, "ar": ar}
    else:
        return {"width": None, "height": None, "ar": None}


def create_global_polars(
    s3_fs: Optional[s3fs.S3FileSystem],
    s3_storage_options: Optional[dict],
    opts: argparse.Namespace,
) -> None:
    # Read files from S3 or local folder
    df = read_dataframe_from_parquet_files(
        ds_folder=opts.input_folder,
        s3_fs=s3_fs,
        s3_storage_options=s3_storage_options,
        max_items=opts.max_items if opts.max_items is not None else None,
    )

    # # Skip existing files not to be processed
    # if opts.skip_existing:
    #     if "s3://" in opts.output_path:
    #         if s3_fs.exists(opts.output_path):
    #             orig_df = pl.read_ipc(opts.output_path, storage_options=s3_storage_options)
    #         else:
    #             orig_df = None
    #     else:
    #         if Path(opts.output_path).exists():
    #             orig_df = pl.read_ipc(opts.output_path)
    #         else:
    #             orig_df = None

    #     if orig_df is not None and len(orig_df) > 0 and opts.image_path_column in orig_df.columns:
    #         df = df.filter(~pl.col(opts.image_path_column).is_in(orig_df[opts.image_path_column]))

    # Rename key column to item_id
    df = df.rename(
        {
            "key": "item_id",
            "width": "image_width",
            "height": "image_height",
        }
    )

    # Add parquet_id column
    df = df.with_columns(
        pl.col("item_id").cast(pl.Utf8).str.slice(0, 5).alias("parquet_id"),
    )
    # Concatenate "parquet_id" and "item_id" into a new column opts.image_path_column
    df = df.with_columns(
        (pl.col("parquet_id") + "/" + pl.col("item_id") + opts.image_extension).alias(opts.image_path_column)
    )

    # Get image width, height and aspect ratio using a single map_elements call
    df = df.with_columns(
        pl.col(opts.image_path_column)
        .map_elements(
            lambda x: get_image_dimensions(x, opts.input_folder, s3_fs),
            return_dtype=pl.Struct(
                [
                    pl.Field("width", pl.Int64),
                    pl.Field("height", pl.Int64),
                    pl.Field("ar", pl.Float64),
                ]
            ),
        )
        .alias("image_dimensions")
    ).unnest("image_dimensions")

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
    create_global_polars(s3_fs, s3_storage_options, opts)


if __name__ == "__main__":
    main()
