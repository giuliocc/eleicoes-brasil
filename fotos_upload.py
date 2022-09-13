import asyncio
import itertools
import os
from pathlib import Path

import aioboto3
import aiofiles
from more_itertools import chunked
from PIL import Image
from tqdm import tqdm


output_path = Path('fotos/output')


async def main(upload_folder=output_path):
    files = [convert_to_jpg(f) for f in upload_folder.glob("**/*") if f.is_file()]
    await chunked_upload(files)


def convert_to_jpg(file):
    extension = file.suffixes[-1]
    new_file = file.with_suffix('.jpg')

    if extension == '.jpg':
        pass
    elif extension == '.jpeg':
        file.rename(new_file)
    else:
        pillowobj = Image.open(file)
        pillowobj.save(new_file)
        file.unlink()

    return new_file


async def chunked_upload(files, chunk_size=100):
    with tqdm(total=len(files), desc="Uploading files") as progress_bar:
        for chunk in chunked(files, chunk_size):
            tasks = [asyncio.ensure_future(upload(file)) for file in chunk]
            await asyncio.gather(*tasks)
            progress_bar.update(len(tasks))


async def upload(file):
    upload_path = file.relative_to(output_path)

    digital_ocean_spaces_info = {
        "region_name": os.environ["REGION_NAME"],
        "endpoint_url": os.environ["ENDPOINT_URL"],
        "aws_access_key_id": os.environ["ACCESS_KEY_ID"],
        "aws_secret_access_key": os.environ["SECRET_ACCESS_KEY"],
    }
    session = aioboto3.Session()
    async with session.client('s3', **digital_ocean_spaces_info) as s3:
        try:
            async with aiofiles.open(file, "rb") as spfp:
                await s3.upload_fileobj(spfp, 'serenata-de-amor-data', f'perfil-politico/{upload_path}', ExtraArgs={"ACL": "public-read"})
        except Exception as e:
            print(f"Unable to upload {upload_path}: {e} ({type(e)})")
            return

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
