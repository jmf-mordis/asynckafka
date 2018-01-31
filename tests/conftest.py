import subprocess
from subprocess import run

import pytest


@pytest.fixture(scope='session', autouse=True)
def compile_project():
    try:
        run(
            'python setup.py build_ext --inplace --force',
            shell=True, check=True, stderr=subprocess.STDOUT
        )
    except Exception:
        pytest.fail("Error building asynckafka package")
