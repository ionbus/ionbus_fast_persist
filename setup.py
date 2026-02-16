"""used to package ionbus_fast_persist"""

from __future__ import annotations

import os
import re
import subprocess
from glob import glob

from setuptools import setup


def get_latest_tag() -> str | None:
    """Gets latest tag from git; returns None if not found."""
    try:
        result = subprocess.run(
            ["git", "describe", "--tags"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return None


avoid_regexes = [
    re.compile(f"^{x}$")
    for x in [
        r"\d+",
        "build",
        "egg-info",
        "__pycache__",
        "log",
        ".+egg-info",
        "dist",
    ]
]

python_version_re = re.compile(
    r';\s(python_version)\s*([<>=!]+)\s*["\']?(\d+\.\d+)["\']?'
)


def ok_dir(name: str) -> bool:
    """Returns ok if we should keep"""
    for regex in avoid_regexes:
        if regex.search(name):
            return False
    return True


# update _version.py
if latest_tag := get_latest_tag():
    with open("_version.py", "w", encoding="utf-8") as ver_file:
        ver_file.write(f'__version__ = "{latest_tag}"\n')

all_dirs = [x for x in glob("*") if os.path.isdir(x) and ok_dir(x)]
packages = ["ionbus_fast_persist"] + [
    f"ionbus_fast_persist/{x}" for x in all_dirs
]
package_dirs = {
    "ionbus_fast_persist": ".",
}
package_dirs.update({f"ionbus_fast_persist/{x}": f"./{x}" for x in all_dirs})
print(f"{package_dirs=}")
extra_reqs = {}
requirements = []
with open("requirements.txt", "r", encoding="utf-8") as req:
    for line in req.readlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = python_version_re.search(line)
        if match:
            before = line[: match.start()].strip()
            operator = match.group(2).strip()
            version = match.group(3).strip()
            extra_reqs.setdefault(
                f':python_version{operator}"{version}"', []
            ).append(before)
        else:
            requirements.append(line.replace("= ", "="))

print(f"{requirements=}\n{extra_reqs=}")

# Read the pip-specific readme for PyPI
with open("readme_pip.md", "r", encoding="utf-8") as readme_file:
    long_description = readme_file.read()

setup(
    name="ionbus-fast-persist",
    packages=packages,
    package_dir=package_dirs,
    python_requires=">=3.9",
    install_requires=requirements,
    extras_require=extra_reqs,
    long_description=long_description,
    long_description_content_type="text/markdown",
    package_data={
        "ionbus_fast_persist": [
            # "resources/*.pem",
            # "resources/*.pkl.gz",
            "*.md",
            "*/*.md",
        ]
    },
)
