# Makefile for automating build and publish of Python library

.PHONY: all build testpypi pypi clean

# Default target
all: build

# Build the package
build:
	@echo "Building the package..."
	python setup.py sdist bdist_wheel

# Upload to Test PyPI
testpypi: build
	@echo "Uploading to Test PyPI..."
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

# Upload to main PyPI
pypi: build
	@echo "Uploading to PyPI..."
	twine upload dist/*

# Clean build artifacts
clean:
	@echo "Cleaning up build artifacts..."
	rm -rf build dist *.egg-info