from setuptools import setup

setup(
    name='pbs-utils',
    version='0.1',
    description='Package for submitting and running PBS jobs by using Python API',
    py_moddules=['pbs-utils'],
    package_dir={'': 'src'},
    install_requires=['petname'],
    author='Oz Mendelsohn',
    author_email='ozyosef.mendelsohn@weizmann.ac.il',
    license='LICENSE.txt',
)
