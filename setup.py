from setuptools import setup


setup(
    name='aiodiskdb',
    version='0.1',
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
    include_package_data=True
)
