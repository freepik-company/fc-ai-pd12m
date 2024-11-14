export HF_TRANSFER=True
huggingface-cli download --repo-type dataset Spawning/PD12M --include "metadata/*.parquet" --local-dir /ssd/datasets/pd12m
