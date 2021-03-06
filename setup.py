import setuptools

setuptools.setup(
    name='database-alchemy',
    version='0.1.0',
    url='https://github.com/16967143/database-alchemy',

    author='Nonchalant',
    author_email='davistodt@hotmail.com',

    description='Learning databases using sqlalchemy, sqlite3 and alembic for unknown reasons',
    long_description=open('README.md').read(),

    packages=['database_alchemy'],

    install_requires=[
        'click',
        'pandas',
    ],

    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
    entry_points={
        'console_scripts': [
            'db-create = database_alchemy.db_create:main',
            'db-insert = database_alchemy.db_insert:main',
            'db-query = database_alchemy.db_query:main',
        ],
    },
)