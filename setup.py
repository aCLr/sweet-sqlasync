from setuptools import setup, find_packages

setup(
    name='sweet-sqlasync',
    version='0.1.0',
    packages=find_packages(),
    url='',
    license='',
    author='antonio_antuan',
    author_email='a.ch.clr@gmail.com',
    description='',
    test_suite='tests',
    test_requires=[
        'pytest',
        'sqlalchemy-stubs',
        'pytest-runner',
    ],
    install_requires=[
        'psycopg2==2.8.4',
        'aiopg',
        'sqlalchemy',
    ]
)
