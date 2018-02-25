import os
import sys
from distutils.core import setup
from distutils.extension import Extension

from Cython.Build import cythonize

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()

version = '0.1.1'
module_name = 'asynckafka'
github_username = 'jmf-mordis'

extensions = [
    'asynckafka.settings',
    'asynckafka.callbacks',
    'asynckafka.utils',
    'asynckafka.consumer.message',
    'asynckafka.consumer.rd_kafka_consumer',
    'asynckafka.consumer.consumer',
    'asynckafka.producer.rd_kafka_producer',
    'asynckafka.producer.producer',
]

if '--tests' in sys.argv:
    extensions.append('tests.asynckafka_tests')
    sys.argv.remove('--tests')


module_list = [
    Extension(
        extension,
        [extension.replace('.', '/') + '.pyx'],
        libraries=['rdkafka']
    )
    for extension in extensions
]

setup(
    name=module_name,
    packages=[module_name],
    description='Fast python kafka client for asyncio.',
    long_description=readme,
    url=f'http://github.com/{github_username}/{module_name}',
    license='mit',
    author='José Melero Fernández',
    author_email='jmelerofernandez@gmail.com',
    platforms=['*nix'],
    version=version,
    download_url=f'https://github.com/{github_username}/{module_name}/archive/'
                 f'{version}.tar.gz',
    ext_modules=cythonize(
        module_list,
        compiler_directives={'embedsignature': True}
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
