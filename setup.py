from setuptools import setup, find_packages


def readme():
    with open('README.rst') as f:
        return f.read()


def license():
    with open('LICENSE') as f:
        return f.read()


reqs = [line.strip() for line in open('requirements.txt')]


setup(
    name='glos-qartod',
    version='0.0.1',
    description='GLOS QARTOD Utility',
    long_description=readme(),
    author='Luke Campbell',
    author_email='luke.campbell at rpsgroup.com',
    url='https://github.com/lukecampbell',
    entry_points={
        'console_scripts': ['glos-qartod=glos_qartod.cli:main']
    },
    packages=find_packages(),
    install_requires=reqs,
    tests_require=['pytest'],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Topic :: Scientific/Engineering',
        'Topic :: Utilities'
    ],
    include_package_data=True
)


