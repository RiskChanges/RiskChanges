from setuptools import setup
import os

here = os.path.abspath(os.path.dirname(__file__))

about = dict()
with open(os.path.join(here, 'RiskChanges', "__version__.py"), "r") as f:
    exec(f.read(), about)

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='RiskChanges',
    packages=['RiskChanges', "RiskChanges.DataManage",
              "RiskChanges.RiskChangesOps"],
    version=about["__version__"],
    # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    license='cc-by-4.0',
    description='Library for computation of Loss and risk for Changing Natural Hazards',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author=about["__author__"],
    author_email=about["__email__"],
    url='https://github.com/RiskChanges/RiskChanges',
    keywords=['Risk', 'Natural Hazard', 'Loss', "RiskChanges",
              "SDSS", "Spatial Disaster Support System"],
    install_requires=[
        'gdal',
        'rasterio',
        'numpy',
        'rasterstats',
        'shapely',
        'owslib',
        'geoalchemy2',
        'sqlalchemy',
        'geopandas',
        'pandas'
    ],
    classifiers=[
        # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
        'Development Status :: 3 - Alpha',
        # Define that your audience are developers
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Common Public License',   # Again, pick a license
        # Specify which pyhton versions that you want to support
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
)
