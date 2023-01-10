import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="hubspot_to_ryl",
    version="0.0.1",
    description="A CDK Python app to create the Hubspot to RYL connection",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Jessica Lynch",
    package_dir={"": "src"},
    packages=setuptools.find_packages(where="src"),
    install_requires=[
        "aws-cdk.core==1.41.0",
    ],
    python_requires=">=3.9",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
