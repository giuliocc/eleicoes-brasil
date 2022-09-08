from urllib.parse import urljoin
from pathlib import Path
from zipfile import ZipFile

from rows.utils import download_file
from tqdm import tqdm

from utils import is_municipal_elections_year


data_path = Path("fotos")
download_path = data_path / "download"
output_path = data_path / "output"
for path in (data_path, download_path, output_path):
    if not path.exists():
        path.mkdir(parents=True)

STATES = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "BR",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]


def main():
    for year in range(2014, 2022 + 1, 2):
        download_photos(year)


def download_photos(year):
    base_url = f"https://cdn.tse.jus.br/estatistica/sead/eleicoes/eleicoes{year}/fotos/"
    download_filenames = get_download_filenames(year)

    for download_filename in download_filenames:
        filepath = download_path / str(year) / download_filename
        url = urljoin(base_url, download_filename)
        download(url, filepath)

        photo_path = output_path / str(year)
        extract(filepath, photo_path)


def get_download_filenames(year):
    if year == 2014:
        return [f"foto_cand{year}_div.zip"]

    download_filenames = []
    for state in STATES:
        if state in ["BR", "DF"] and is_municipal_elections_year(year):
            continue

        download_filenames.append(f"foto_cand{year}_{state}_div.zip")

    return download_filenames


def download(download_url, download_path):
    print(f"Downloading {download_path.name}", end="")
    if download_path.exists():
        print(" - downloaded already, skipping.")
    else:
        if not download_path.parent.exists():
            download_path.parent.mkdir(parents=True)
        print()
        download_file(
            download_url, progress=True, filename=download_path, user_agent="Mozilla/4"
        )
        print(f"  saved: {download_path}")


def extract(source_path, extract_path):
    if not extract_path.exists():
        extract_path.mkdir(parents=True)

    print(f"  Exporting to: {extract_path}")
    zf = ZipFile(source_path)

    for file_info in tqdm(zf.filelist, desc="Exporting pictures"):
        internal_name = file_info.filename
        internal_path = Path(internal_name)
        extension = internal_path.name.split(".")[-1].lower()
        info = internal_path.name.split(".")[0].split("_")[0]
        state, sequence_number = info[1:3], info[3:]
        new_filename = extract_path / state / f"{sequence_number}.{extension}"

        if not new_filename.parent.exists():
            new_filename.parent.mkdir(parents=True)

        zfobj = zf.open(internal_name)

        with open(new_filename, mode="wb") as fobj:
            fobj.write(zfobj.read())


if __name__ == "__main__":
    main()
