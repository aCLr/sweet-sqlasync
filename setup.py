from setuptools import setup, find_packages

setup(
    name='sweet-sqlasync',
    version='0.0.1b2',
    packages=find_packages(),
    url='https://github.com/aCLr/sweet-sqlasync',
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ],
    python_requires='>=3.7',
    author='antonio_antuan',
    author_email='a.ch.clr@gmail.com',
    description='Syntax sugar for SQLAlchemy + aiopg',
    install_requires=[
        'psycopg2==2.8.4',
        'aiopg',
        'sqlalchemy',
    ]
)
