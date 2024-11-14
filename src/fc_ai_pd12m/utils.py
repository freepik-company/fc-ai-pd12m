import os
import shutil
import tempfile
from typing import Optional

import polars as pl
import s3fs


def safe_write_ipc(
    df: pl.DataFrame,
    dest_path: str,
    s3_fs: Optional[s3fs.S3FileSystem],
):
    # Write to a temporary file first
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        df.write_ipc(tmp.name)
        tmp.flush()
        os.fsync(tmp.fileno())

    if "s3://" in dest_path:
        if s3_fs is not None:
            try:
                s3_fs.put(tmp.name, dest_path)
            except Exception as e:
                os.unlink(tmp.name)
                raise e
            finally:
                os.unlink(tmp.name)
        else:
            os.unlink(tmp.name)
            raise ValueError("s3_fs is None, but an S3 path was provided.")
    else:
        try:
            shutil.move(tmp.name, dest_path)
        except Exception as e:
            os.unlink(tmp.name)
            raise e

    # Open it and check that has the same number of rows as the original dataframe
    if "s3://" in dest_path:
        if s3_fs is not None:
            # TODO: With Polars 1.13.0, we cannot use the storage_options parameter instead of cloud_options
            with s3_fs.open(dest_path, "rb") as f:
                df_ipc = pl.read_ipc(f)
        else:
            raise ValueError("s3_fs is None, but an S3 path was provided.")
    else:
        df_ipc = pl.read_ipc(dest_path)

    if len(df_ipc) != len(df):
        raise ValueError(
            f"The number of rows in the IPC file ({len(df_ipc)}) does not match the number of rows in the original dataframe ({len(df)})"
        )
    print(f"Successfully written feather file to {dest_path}")
    del df_ipc
