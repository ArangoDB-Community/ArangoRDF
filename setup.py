from setuptools import setup

with open("./README.md") as fp:
    long_description = fp.read()

setup(
    name="arango_rdf",
    author="ArangoDB-MSG",
    author_email="hackers@arangodb.com",
    description="Import RDF graphs into ArangoDB",
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
        "requests>=2.27.1",
        "tqdm>=4.64.0",
        "setuptools>=45",
    ],
    extras_require={
        "dev": [
            "black",
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
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
