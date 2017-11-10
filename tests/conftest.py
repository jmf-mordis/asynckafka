import subprocess
from subprocess import run

import pytest


@pytest.fixture(scope='session', autouse=True)
def compile_project():
    print('Compiling python project: ')
    run('venv/bin/python setup.py build_ext --inplace', shell=True, check=True, stderr=subprocess.STDOUT)
