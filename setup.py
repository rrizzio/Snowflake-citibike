import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="Snowflake-citibike-RRIZZIO", 
    version="1.0",
    author="RRIZZIO",
    author_email="rrizzio@rrizzio.com",
    description="Snowflake to solve citibike data analysis",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rrizzio/Snowflake-citibike",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='=3.8',
)
