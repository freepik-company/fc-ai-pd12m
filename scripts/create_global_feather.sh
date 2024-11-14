export AWS_CONFIG_FILE=".aws/config"

python src/fc_ai_pd12m/create_global_feather.py \
    --input_folder "s3://fc-gra-alejandria/ds/public/PD12M" \
    --output_folder "output/PD12M" \
    --image_path_column "image_path" \
    --image_extension ".jpg" \
    --num_workers 64