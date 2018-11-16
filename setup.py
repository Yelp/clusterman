from setuptools import find_packages
from setuptools import setup

from clusterman import __version__

setup(
    name='clusterman',
    version=__version__,
    provides=['clusterman'],
    author='Distsys Compute',
    author_email='compute-infra@yelp.com',
    description='Mesos cluster scaling and management tools',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    include_package_data=True,
    install_requires=[
    ],
    scripts=[
        'clusterman/supervisord/fetch_clusterman_signal',
        'clusterman/supervisord/run_clusterman_signal',
    ],
    entry_points={
        'console_scripts': [
            'clusterman=clusterman.run:main',
        ],
    },
)
