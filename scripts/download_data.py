#!/usr/bin/python
#-*- coding: utf-8 -*-


import pathlib
import requests


# Source
url = "http://corpus.leeds.ac.uk/frqc/internet-ru.num"

# Target
filename_data = "internet-ru-num.txt"
folder_project_root = pathlib.Path(__name__).absolute().parents[1]
folder_data_raw = folder_project_root / "data" / "raw"
filepath_data = folder_data_raw / filename_data


def download_text_data(url=None, filepath=None):
    assert pathlib.Path(filepath).parent.exists()
    r = requests.get(url)
    assert r.status_code == requests.codes.ok
    with open(filepath, "w") as file:
        file.write(r.text)


def main():
    download_text_data(url=url, filepath=filepath_data)


if __name__ == "__main__":
    # execute only if run as a script
    main()
