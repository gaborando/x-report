from setuptools import setup, find_packages

setup(
    name="x-report",
    version="0.1.10",
    license="MIT",
    description="Create a pipeline to generate reports with a visual description of each extraction and transformation phase.",
    long_description=open('README.md').read(),
    long_description_content_type="text/markdown",
    author="Gabor Galazzo",
    author_email="gabor.galazzo@gmail.com",
    url="https://github.com/gaborando/x-report",  # Project URL
    packages=find_packages(where="src"),           # Find packages in src
    package_dir={"": "src"},                       # Tell setuptools where to look for the packages
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",  # Specify minimum Python version
    install_requires=[
        "pandas",
        "jinja2"
    ],
)