from distutils.core import setup
setup(
  name = 'RiskChanges',         # How you named your package folder (MyLib)
  packages = ['RiskChanges'],   # Chose the same as "name"
  version = '1.92',      # Start with a small number and increase it with every change you make
  license='cc-by-4.0',        # Chose a license from here: https://help.github.com/articles/licensing-a-repository
  description = 'Library for computation of Loss and risk for Changing Natural Hazards',   # Give a short description about your library
  author = 'Ashok Dahal ',                   # Type in your name
  author_email = 'ashokdahal.geo@gmail.com',      # Type in your E-Mail
  url = 'https://github.com/ashokdahal',   # Provide either the link to your github or to your website
  download_url = 'https://github.com/ashokdahal/RiskChanges/archive/0.9.tar.gz',    # I explain this later on
  keywords = ['Risk', 'Natural Hazard', 'Loss'],   # Keywords that define your package best
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
    'Development Status :: 3 - Alpha',      # Chose either "3 - Alpha", "4 - Beta" or "5 - Production/Stable" as the current state of your package
    'Intended Audience :: Developers',      # Define that your audience are developers
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: Common Public License',   # Again, pick a license
    'Programming Language :: Python :: 3',      #Specify which pyhton versions that you want to support
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
  ],
)
