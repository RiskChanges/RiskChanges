from distutils.core import setup
setup(
    name='RiskChanges',         # How you named your package folder (MyLib)
    packages=['RiskChanges', "RiskChanges.DataManage",
              "RiskChanges.RiskChangesOps"],   # Chose the same as "name"
    version='2.1.0',      # Start with a small number and increase it with every change you make
    # Chose a license from here: https://help.github.com/articles/licensing-a-repository
    license='cc-by-4.0',
    # Give a short description about your library
    description='Library for computation of Loss and risk for Changing Natural Hazards',
    author='Ashok Dahal ',                   # Type in your name
    author_email='ashokdahal.geo@gmail.com',      # Type in your E-Mail
    # Provide either the link to your github or to your website
    url='https://github.com/ashokdahal',
    # I explain this later on
    download_url='https://github.com/ashokdahal/RiskChanges/archive/refs/tags/v2.zip',
    # Keywords that define your package best
    keywords=['Risk', 'Natural Hazard', 'Loss'],
    install_requires=[            # I get to this in a second
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
