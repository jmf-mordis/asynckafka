from distutils.core import setup
from distutils.extension import Extension

import os
from Cython.Build import cythonize

with open(os.path.join(os.path.dirname(__file__), 'readme.rst')) as f:
    readme = f.read()

setup(
    name="asynckafka",
    description='Fast python kafka library for asyncio.',
    long_description=readme,
    url='http://github.com/jmf-mordis/asynckafka',
    license='mit',
    author='José Melero Fernández',
    author_email='jmelerofernandez@gmail.com',
    platforms=['*nix'],
    version="0.0.0",
    packages=['asynckafka'],
    ext_modules=cythonize(
        [
            Extension(
                "asynckafka.consumers.rd_kafka_consumer",
                ["asynckafka/consumers/rd_kafka_consumer.pyx"],
                libraries=["rdkafka"]
            ),
            Extension(
                "asynckafka.consumers.consumer_thread",
                ["asynckafka/consumers/consumer_thread.pyx"],
                libraries=["rdkafka"]
            ),
            Extension(
                "asynckafka.consumers.consumers",
                ["asynckafka/consumers/consumers.pyx"],
                libraries=["rdkafka"]
            ),
            Extension(
                "asynckafka.utils",
                ["asynckafka/utils.pyx"],
                libraries=["rdkafka"]
            ),
            Extension(
                "asynckafka.producer.producer",
                ["asynckafka/producer/producer.pyx"],
                libraries=["rdkafka"]
            ),
            Extension(
                "tests.asynckafka_tests",
                ["tests/asynckafka_tests.pyx"],
                libraries=["rdkafka"]
            ),
        ]
    ),
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
