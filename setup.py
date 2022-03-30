from setuptools import find_packages, setup


install_requires = [
    'certifi==2021.10.8',
    'charset-normalizer==2.0.12',
    'idna==3.3',
    'packaging==21.3',
    'PyJWT==2.3.0',
    'pyparsing==3.0.7',
    'python-arango==7.3.1',
    'requests==2.27.1',
    'requests-toolbelt==0.9.1',
    'setuptools-scm==6.4.2',
    'tomli==2.0.1',
    'urllib3==1.26.9',
]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='python-arango-mapper',
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    python_requires=">=3.7",
    install_requires=install_requires,
    version='0.1.8',
    description='fast and easy-to-use python-arango mapper library',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='lee ui dam',
    author_email='ud803da@gmail.com',
    url='https://github.com/ud803/python-arango-mapper',
    license='GNU General Public License v3.0'
)
