export AWS_CONFIG_FILE=".aws/config"

python src/fc_ai_pd12m/create_global_feather.py \
    --input_folder "s3://fc-gra-alejandria/ds/public/PD12M" \
    --output_path "s3://fc-gra-alejandria/ds/public/PD12M/global_pd12m_data.feather" \
    --image_path_column "image_path" \
    --image_extension ".jpg" \
    --num_workers 64