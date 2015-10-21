import os
import re
from setuptools import setup, find_packages


PROJECT_NAME = "prkng_process"

here = os.path.abspath(os.path.dirname(__file__))

requirements = (
    'psycopg2==2.5.4',
    'click==3.3',
    'requests==2.5.1',
    'geojson==1.0.9',
    'pytest==2.6.4',
    'boto==2.38.0',
    'pyrq==0.4.1',
    'rq-scheduler==0.5.1',
    'demjson==2.2.3',
    'redis==2.10.3'
)


def find_version(*file_paths):
    """
    see https://github.com/pypa/sampleproject/blob/master/setup.py
    """
    with open(os.path.join(here, *file_paths), 'r') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string. "
                       "Should be at the first line of __init__.py.")

setup(
    name=PROJECT_NAME,
    version=find_version('prkng_process', '__init__.py'),
    description="prkng data processing",
    url='https://prk.ng/',
    author='Prkng Inc',
    author_email='hey@prk.ng',
    license='',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 2.7',
        'License :: Other/Proprietary License'
    ],
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    entry_points={
        'console_scripts': ['prkng-process = prkng_process.commands:main'],
    }
)
