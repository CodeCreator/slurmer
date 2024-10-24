from setuptools import setup

setup(
    name='slurmer',
    version='0.1',
    packages=['slurmer'],
    install_requires=[
    ],
    entry_points={
        'console_scripts': [
            'slurmer=slurmer.run:main',
        ]
    },
    python_requires='>=3.9',
)
