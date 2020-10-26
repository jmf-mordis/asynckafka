# coding=utf-8
import os
import sys

from setuptools import setup, Extension
from setuptools.command.build_ext import build_ext

with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()

version = '0.2.0'
module_name = 'asynckafka'
github_username = 'jmf-mordis'
language_level = '3'

extensions = [
    'asynckafka.settings',
    'asynckafka.callbacks',
    'asynckafka.utils',
    'asynckafka.consumer.message',
    'asynckafka.consumer.topic_partition',
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
        libraries=['rdkafka'],
    )
    for extension in extensions
]

requirements = []


class LazyCommandClass(dict):
    """
    Lazy command class that defers operations requiring Cython until
    they've actually been downloaded and installed by setup_requires.
    """
    def __contains__(self, key):
        return (
            key == 'build_ext'
            or super(LazyCommandClass, self).__contains__(key)
        )

    def __setitem__(self, key, value):
        if key == 'build_ext':
            raise AssertionError("build_ext overridden!")
        super(LazyCommandClass, self).__setitem__(key, value)

    def __getitem__(self, key):
        if key != 'build_ext':
            return super(LazyCommandClass, self).__getitem__(key)

        class asynckafka_build_ext(build_ext):
            user_options = build_ext.user_options + [
                ('cython-always', None,
                 'run cythonize() even if .c files are present'),
                ('cython-annotate', None,
                 'Produce a colorized HTML version of the Cython source.'),
                ('cython-directives=', None,
                 'Cythion compiler directives'),
            ]

            boolean_options = build_ext.boolean_options + [
                'cython-always',
                'cython-annotate',
            ]

            def initialize_options(self):
                # initialize_options() may be called multiple times on the
                # same command object, so make sure not to override previously
                # set options.
                if getattr(self, '_initialized', False):
                    return

                super().initialize_options()
                self.cython_always = False
                self.cython_annotate = None
                self.cython_directives = None

            def finalize_options(self):
                # finalize_options() may be called multiple times on the
                # same command object, so make sure not to override previously
                # set options.
                if getattr(self, '_initialized', False):
                    return

                need_cythonize = self.cython_always
                cfiles = {}

                for extension in self.distribution.ext_modules:
                    for i, sfile in enumerate(extension.sources):
                        if sfile.endswith('.pyx'):
                            prefix, ext = os.path.splitext(sfile)
                            cfile = prefix + '.c'

                            if os.path.exists(cfile) and not self.cython_always:
                                extension.sources[i] = cfile
                            else:
                                if os.path.exists(cfile):
                                    cfiles[cfile] = os.path.getmtime(cfile)
                                else:
                                    cfiles[cfile] = 0
                                need_cythonize = True

                if need_cythonize:
                    try:
                        import Cython
                    except ImportError:
                        raise RuntimeError(
                            'please install Cython to compile uvloop from source')

                    if Cython.__version__ < '0.28':
                        raise RuntimeError(
                            'uvloop requires Cython version 0.28 or greater')

                    from Cython.Build import cythonize

                    directives = {'language_level' : language_level}
                    if self.cython_directives:
                        for directive in self.cython_directives.split(','):
                            k, _, v = directive.partition('=')
                            if v.lower() == 'false':
                                v = False
                            if v.lower() == 'true':
                                v = True

                            directives[k] = v

                    self.distribution.ext_modules[:] = cythonize(
                        self.distribution.ext_modules,
                        compiler_directives=directives,
                        annotate=self.cython_annotate)

                super().finalize_options()
                self._initialized = True

            def build_extensions(self):
                self.compiler.add_library('pthread')
                super().build_extensions()
        return asynckafka_build_ext


setup(
    name=module_name,
    packages=[module_name],
    description='Fast python kafka client for asyncio.',
    long_description=readme,
    url='http://github.com/{github_username}/{module_name}'.format(
        github_username=github_username, module_name=module_name
    ),
    license='MIT',
    author='José Melero Fernández',
    author_email='jmelerofernandez@gmail.com',
    platforms=['*nix'],
    version=version,
    download_url='https://github.com/{github_username}/{module_name}/archive/'
                 '{version}.tar.gz'.format(
        github_username=github_username, module_name=module_name,
        version=version
    ),
    cmdclass=LazyCommandClass(),
    setup_requires=['cython'],
    install_requires=[],
    ext_modules=module_list,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Framework :: AsyncIO'
    ],
    keywords=['asyncio', 'kafka', 'cython'],
    test_suite='unittest'
)
