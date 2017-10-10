from setuptools import find_packages
from setuptools import setup

from clusterman import __version__

setup(
    name='clusterman',
    version=__version__,
    provides=['clusterman'],
    author='Distsys-Processing',
    author_email='distsys-processing@yelp.com',
    description='Mesos cluster scaling and management tools',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    include_package_data=True,
    install_requires=[
    ],
    entry_points={
        'console_scripts': [
            'clusterman=clusterman.run:main',
        ],
    },
)
