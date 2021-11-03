from setuptools import setup, find_packages

setup(
    name='data_loading_package',
    version='1.0.0',
    url='https://github.com/balusai-itv/hs-energy-dl.git',
    packages=find_packages(),
    author='ITVERSITY',
    author_email='dheeraj.pasupuleti@itversity.in',
    description='Scrapping and Data loading of energy field resources data in monthly interval into database',
    install_requires=[
        'pandas==1.3.2',
        'camelot-py[cv]==0.10.1',
        'psycopg2-binary==2.9.1',
        'SQLAlchemy==1.4.23',
        'python-dotenv>=0.5.1',
        'numpy==1.21.1',
        'bs4==0.0.1',
        'requests==2.26.0',
    ],
    package_data={'': ['*']},
    entry_points={
        'console_scripts': [
            'hs_dl = app.energy_dl_main:main',
        ],
    },
    extras_require={},
    zip_safe=False,
)
