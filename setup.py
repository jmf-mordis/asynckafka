from distutils.core import setup
from distutils.extension import Extension

import os
from Cython.Build import cythonize

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()

setup(
    name="asynckafka",
    description='Fast python kafka library for asyncio.',
    long_description=readme,
    url='http://github.com/jmf-mordis/asynckafka',
    license='MIT',
    author='José Melero Fernández',
    author_email='jmelerofernandez@gmail.com',
    platforms=['*nix'],
    version="0.0.1",
    packages=['asynckafka'],
    ext_modules=cythonize([
        Extension(
            "asynckafka.producer",
            ["asynckafka/producer.pyx"],
            libraries=["rdkafka"]
        ),
        Extension(
            "asynckafka.consumer",
            ["asynckafka/consumer.pyx"],
            libraries=["rdkafka"]
        ),
        Extension(
            "asynckafka.utils",
            ["asynckafka/utils.pyx"],
            libraries=["rdkafka"]
        ),
    ]),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Framework :: AsyncIO'
    ],
    test_suite='pytest'
)
