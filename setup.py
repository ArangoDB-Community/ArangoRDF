from setuptools import setup

with open("./README.md") as fp:
    long_description = fp.read()

setup(
    name="arango_rdf",
    author="Anthony Mahanna",
    author_email="anthony.mahanna@arangodb.com",
    description="Convert ArangoDB graphs to RDF & vice-versa.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ArangoDB-Community/ArangoRDF",
    keywords=["arangodb", "rdf", "adapter"],
    packages=["arango_rdf"],
    include_package_data=True,
    python_requires=">=3.7",
    license="Apache Software License",
    install_requires=[
        "rdflib>=6.0.0",
        "python-arango>=7.4.1",
        "cityhash>=0.4.6",
        "requests>=2.27.1",
        "rich>=12.5.1",
        "setuptools>=45",
    ],
    extras_require={
        "dev": [
            "black",
            "mypy",
            "flake8>=3.8.0",
            "isort>=5.0.0",
            "pytest>=6.0.0",
            "pytest-cov>=2.0.0",
            "coveralls>=3.3.1",
        ],
    },
    classifiers=[
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
