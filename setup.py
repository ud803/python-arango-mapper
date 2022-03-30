from setuptools import find_packages, setup
import setuptools

install_requires = [
    'bleach==4.1.0',
    'certifi==2021.10.8',
    'cffi==1.15.0',
    'charset-normalizer==2.0.9',
    'colorama==0.4.4',
    'cryptography==36.0.1',
    'dataclasses==0.8',
    'docutils==0.18.1',
    'idna==3.3',
    'importlib-metadata==4.8.3',
    'jeepney==0.7.1',
    'keyring==23.4.0',
    'packaging==21.3',
    'pkginfo==1.8.2',
    'pycparser==2.21',
    'Pygments==2.10.0',
    'PyJWT==2.3.0',
    'pyparsing==3.0.6',
    'python-arango==7.3.0',
    'readme-renderer==32.0',
    'requests==2.26.0',
    'requests-toolbelt==0.9.1',
    'rfc3986==1.5.0',
    'SecretStorage==3.3.1',
    'setuptools-scm==6.3.2',
    'six==1.16.0',
    'tomli==1.2.3',
    'tqdm==4.62.3',
    'twine==3.7.1',
    'typing_extensions==4.0.1',
    'urllib3==1.26.7',
    'webencodings==0.5.1',
    'zipp==3.6.0'
]

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='python-arango-mapper',
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    python_requires=">=3.6",
    install_requires=install_requires,
    version='0.1.7',
    description='fast and easy-to-use python-arango mapper library',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='lee ui dam',
    author_email='ud803da@gmail.com',
    url='https://github.com/ud803/python-arango-mapper',
    license='GNU General Public License v3.0'
)
