from distutils.core import setup
from distutils.extension import Extension
from Cython.Build import cythonize

extension = Extension(
    name="asyncio_kafka.protocol.protocol",
    sources=[
        "asyncio_kafka/protocol/protocol.pyx",
    ]
)

cythonized_extension = cythonize(extension)

setup(ext_modules=cythonized_extension)

