# Setup file for redis_clone

'''
Current Dev Dependencies:
    - pytest
'''

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name="redis_clone",
    version="0.0.1",
    author="Prakash",
    author_email="yogipra2003@gmail.com",
    description="A redis clone",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License"
    ],
    python_requires='>=3.6',
    # Dev dependencies
    extras_require={
        'dev': [
            'pytest',
        ],
    },
)