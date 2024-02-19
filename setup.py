from setuptools import setup, find_packages

# navigate to your project directory, and run the following command:
# python setup.py sdist

setup(
    name='abn-assesment',
    version='1.0',
    packages=find_packages(),
    author='Guillermo Romero',
    author_email='guille.8.romero@gmail.com',
    description='Programming Exercise using PySpark',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/WillyRomero8/ABN-assesment.git',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3.8.10'
    ],
    install_requires=[
        "chispa==0.9.4",
        "colorama==0.4.6",
        "exceptiongroup==1.2.0",
        "iniconfig==2.0.0",
        "packaging==23.2",
        "pluggy==1.4.0",
        "py4j==0.10.9.7",
        "pyspark==3.5.0",
        "pytest==8.0.1",
        "tomli==2.0.1"]
)