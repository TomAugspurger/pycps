from setuptools import setup, find_packages
import os
import re

here = os.path.abspath(os.path.dirname(__file__))

def find_version(*file_paths):
    with open(os.path.join(here, *file_paths)) as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


# Get the long description from the relevant file
# with open('DESCRIPTION.rst', encoding='utf-8') as f:
#     long_description = f.read()

setup(
    name="python-cps",
    version=find_version('pycps', '__init__.py'),
    description="A python package for working with the"
    "[Current Population Survey](http://www.census.gov/cps/).",
    # long_description=long_description,

    # The project URL.
    url='http://github.com/TomAugspurger/pycps',

    # Author details
    author='Tom Augspurger',
    author_email='tom.w.augspurger@gmail.com',

    # Choose your license
    license='MIT',

    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Indicate who your project is intended for
        'Intended Audience :: Developers',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.1',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7'
    ],

    # What does your project relate to?
    keywords='pycps, cps',

    # Don't directly use numexpr or Cython but PyTables depends on them
    # and I think they messed up their setup.py file.
    install_requires = ['arrow>=0.4.0',
                        'requests>=2.4.0',
                        'pathlib>=1.0.1',
                        'lxml>=3.3.5',
                        'numpy>=1.8.0',
                        'pandas>=0.14.1'],
    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages.
    packages=find_packages(exclude=["contrib", "docs", "tests*"]),
    # If there are data files included in your packages, specify them here.
    package_data={
        'pycps': ['data.json', 'settings.json'],
    },

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            'pycps=pycps:main',
        ],
    },
)
