import os
from pathlib import Path

import boto3
from tqdm import tqdm


output_path = Path('fotos/output')


def main(folder_path=output_path):
    local_files = set(map(str, folder_path.glob("*/*")))
    uploaded_files = filter_relevant_files('serenata-de-amor-data', f"perfil-politico/{folder_path.relative_to(output_path)}/")
    files_in_sync = local_files.intersection(uploaded_files)

    for file in tqdm(files_in_sync, desc=f"Syncing files from {folder_path.name}"):
        Path(file).unlink()


def filter_relevant_files(bucket, key_prefix):
    digital_ocean_spaces_info = {
        "region_name": os.environ["REGION_NAME"],
        "endpoint_url": os.environ["ENDPOINT_URL"],
        "aws_access_key_id": os.environ["ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["SECRET_ACCESS_KEY"],
    }
    s3 = boto3.client('s3', **digital_ocean_spaces_info)
    next_token = ''
    relevant_files = set()

    with tqdm(desc=f"Listing files from bucket") as progress_bar:
        while True:
            if next_token:
                res = s3.list_objects_v2(
                    Bucket=bucket,
                    ContinuationToken=next_token,
                    Prefix=key_prefix)
            else:
                res = s3.list_objects_v2(
                    Bucket=bucket,
                    Prefix=key_prefix)

            if 'Contents' not in res:
                break

            if res['IsTruncated']:
                next_token = res['NextContinuationToken']
            else:
                next_token = ''

            for file in res['Contents']:
                if ".jpg" not in file['Key']: 
                    print(f"IGNORE: {obj.key}")
                    continue

                relevant_files.add(f"{output_path / file['Key'][16:]}")
                progress_bar.update(1)
                
            if not next_token:
                break

    return relevant_files


if __name__ == "__main__":
    main()
