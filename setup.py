from distutils.core import setup
from distutils.extension import Extension

import os

import sys
from Cython.Build import cythonize

with open(os.path.join(os.path.dirname(__file__), 'readme.rst')) as f:
    readme = f.read()

extensions = [
    "asynckafka.consumers.rd_kafka_consumer",
    "asynckafka.consumers.consumer_thread",
    "asynckafka.consumers.consumers",
    "asynckafka.callbacks",
    "asynckafka.utils",
    "asynckafka.producer.producer",
]

if "--tests" in sys.argv:
    extensions.append("tests.asynckafka_tests")
    sys.argv.remove("--tests")


module_list = [
    Extension(
        extension,
        [extension.replace('.', '/') + '.pyx'],
        libraries=['rdkafka']
    )
    for extension in extensions
]

setup(
    name="asynckafka",
    packages=['asynckafka'],
    description='Fast python kafka library for asyncio.',
    long_description=readme,
    url='http://github.com/jmf-mordis/asynckafka',
    license='mit',
    author='José Melero Fernández',
    author_email='jmelerofernandez@gmail.com',
    platforms=['*nix'],
    version="0.0.0",
    ext_modules=cythonize(
        module_list
    ),
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Framework :: AsyncIO'
    ],
    keywords=['asyncio', 'kafka', 'cython'],
    test_suite='unittest'
)
