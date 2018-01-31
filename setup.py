from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize


setup(
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
    ])
)
