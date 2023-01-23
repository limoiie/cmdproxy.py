import os
import zipfile
from typing import IO, Union


def zip_dir(dst: Union[str, os.PathLike, IO], path: Union[str, os.PathLike]):
    with zipfile.ZipFile(dst, 'w') as zip_file:
        for root, dirs, files in os.walk(path):
            for file in files:
                zip_file.write(os.path.join(root, file),
                               os.path.relpath(os.path.join(root, file), path))
