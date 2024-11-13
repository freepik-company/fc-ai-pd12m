import argparse
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Generator, Optional

import orjson  # Faster JSON parser
import polars as pl
from fc_ai_yepop.utils import safe_write_ipc
from PIL import Image
from tqdm import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description="Create a global feather file from yepop dataset")
    parser.add_argument("--input_folder", type=Path, help="Path to the input folder containing JSON files")
    parser.add_argument(
        "--output_file",
        type=Path,
        default=Path("global_yepop_data.feather"),
        help="Path to the output feather file",
    )
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of files to process at a time")
    parser.add_argument(
        "--ignore_keys",
        type=str,
        nargs="+",
        default=[],
        help="Keys to ignore in the JSON files",
    )
    parser.add_argument(
        "--skip_existing",
        action="store_true",
        help="Skip existing files",
    )
    parser.add_argument(
        "--max_workers",
        type=int,
        default=None,
        help="Number of worker threads for parallel processing",
    )
    parser.add_argument(
        "--image_path_key",
        type=str,
        default="filename",
        help="Key in the JSON file that contains the image path",
    )
    parser.add_argument(
        "--max_items",
        type=int,
        default=None,
        help="Maximum number of items to process",
    )
    return parser.parse_args()


def validate_args(opts: argparse.Namespace) -> argparse.Namespace:
    if not opts.input_folder.exists():
        raise ValueError(f"Input folder {opts.input_folder} does not exist")
    if not opts.output_file.exists():
        opts.output_file.parent.mkdir(parents=True, exist_ok=True)
    if opts.output_file.suffix.lower() not in [".feather", ".fth"]:
        raise ValueError(f"Output file {opts.output_file} must have a .feather or .fth extension")
    if opts.max_items is not None and opts.max_items < 1:
        raise ValueError(f"Max items {opts.max_items} must be greater than 0")
    return opts


def read_files(
    input_folder: Path,
    output_file: Path,
    skip_existing: bool = False,
) -> list[Path]:
    json_files = list(input_folder.rglob("*.json"))
    if len(json_files) == 0:
        raise ValueError(f"No JSON files found in {input_folder}")

    # Read polars file if it exists
    if skip_existing and output_file.exists():
        orig_df = pl.read_ipc(output_file)

        # Skip files that are already in the polars file
        json_files = [file for file in json_files if file not in orig_df["file_path"].to_list()]
        if len(json_files) == 0:
            print(f"All {len(json_files)} files already in the polars file")
            return []

    return json_files


@lru_cache(maxsize=10000)
def get_image_dimensions(image_path: str) -> tuple:
    try:
        with Image.open(image_path) as img:
            return img.width, img.height
    except Exception as e:
        print(f"Error getting image dimensions for {image_path}: {e}")
        return None, None


def process_file(
    file_data: dict,
    images_folder: Optional[Path] = None,
    ignore_keys: Optional[set] = None,
    image_path_key: str = "filename",
) -> dict:
    ignore_keys = ignore_keys or set()
    ignore_keys.add("item")

    for key in ignore_keys:
        file_data.pop(key, None)

    if "image_width" not in file_data or "image_height" not in file_data:
        if image_path_key not in file_data:
            raise ValueError(
                f"Image path key {image_path_key} not found in {file_data} and image dimensions are not available"
            )
        if images_folder is None:
            raise ValueError("Images folder not provided and image dimensions are not available")

        abs_image_path = images_folder / file_data[image_path_key]
        width, height = get_image_dimensions(abs_image_path)
        file_data["image_width"] = width
        file_data["image_height"] = height

    return file_data


def process_data_in_batches(
    data_list: list[dict],
    images_folder: Optional[Path] = None,
    image_path_key: str = "filename",
    batch_size: int = 1000,
    show_progress: bool = True,
    ignore_keys: Optional[set] = None,
    max_workers: int = 16,
) -> Generator[pl.DataFrame, None, None]:
    total_files = len(data_list)
    if batch_size > total_files:
        batch_size = total_files

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for i in tqdm(range(0, total_files, batch_size), desc="Processing batches", disable=not show_progress):
            batch_data = data_list[i : i + batch_size]
            futures = [
                executor.submit(process_file, item_data, images_folder, ignore_keys, image_path_key)
                for item_data in batch_data
            ]
            data = [future.result() for future in as_completed(futures) if future.result()]
            if data:
                try:
                    df = pl.DataFrame(data, infer_schema_length=None)
                    # Ensure all dataframes have the same columns
                    df = df.select(sorted(df.columns))
                    yield df
                except Exception as e:
                    print(f"Error processing {data[0]}: {e}")
                    raise e


def create_global_polars(opts: argparse.Namespace) -> None:
    json_files = read_files(
        input_folder=opts.input_folder,
        output_file=opts.output_file,
        skip_existing=opts.skip_existing,
    )

    # Read all JSON files
    total_data = []
    for json_path in tqdm(json_files, desc="Reading JSON files", total=len(json_files)):
        images_folder = json_path.parent / json_path.stem.split(".")[0]
        if not images_folder.exists():
            raise FileNotFoundError(f"Images folder {images_folder} does not exist")

        with open(json_path, "r") as f:
            json_data = orjson.loads(f.read())

        # Convert into list
        json_data = list(json_data.values())

        # Add images folder to each item
        for item in json_data:
            if opts.image_path_key not in item:
                raise ValueError(f"Image path key {opts.image_path_key} not found in {item}")
            item[opts.image_path_key] = str(images_folder / item[opts.image_path_key])

            # Rename width and height to image_width and image_height
            item["image_width"] = item.pop("width")
            item["image_height"] = item.pop("height")

        # Add to total data
        total_data.extend(json_data)

    if opts.max_items is not None and len(total_data) > opts.max_items:
        total_data = random.sample(total_data, opts.max_items)

    batches = process_data_in_batches(
        data_list=total_data,
        images_folder=images_folder,
        image_path_key=opts.image_path_key,
        batch_size=opts.batch_size,
        show_progress=True,
        ignore_keys=set(opts.ignore_keys),
        max_workers=opts.max_workers,
    )

    # Create a Polars DataFrame
    try:
        df = pl.concat(batches)
    except Exception as e:
        print(f"Error concatenating batches: {e}")
        raise e

    # Write the DataFrame to a feather file
    try:
        safe_write_ipc(df, opts.output_file)
        print(f"Global Polars feather file created: {opts.output_file}")
    except Exception as e:
        print(f"Error writing to {opts.output_file}: {e}")
        raise e


def main() -> None:
    opts = parse_args()
    opts = validate_args(opts)
    create_global_polars(opts)


if __name__ == "__main__":
    main()
