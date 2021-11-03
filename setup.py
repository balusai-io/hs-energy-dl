from setuptools import setup

setup(
    name='Data_loading_package',
    version='1.0.0',
    packages=['app', 'app.etl', 'app.utils'],
    url='',
    license='',
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
    ]
)
