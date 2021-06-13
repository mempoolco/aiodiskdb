from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='aiodiskdb',
    version='0.2.3',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/mempoolco/aiodiskdb/',
    license='MIT',
    author='Guido Dassori',
    author_email='guido.dassori@gmail.com',
    python_requires='>=3.8',
    description='Embeddable minimal asynchronous on disk DB',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    include_package_data=True,
    packages=['aiodiskdb'],
    package_dir={'aiodiskdb': 'aiodiskdb'},
)
