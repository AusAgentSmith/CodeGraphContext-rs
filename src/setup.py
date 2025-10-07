from setuptools import setup, find_packages

setup(
    name="codegraphcontext",
    version="0.1.0",
    # Look for packages inside the 'src' folder
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    # Define the command line entry point
    entry_points={
        "console_scripts": [
            "cgc=codegraphcontext.cli.main:app",
        ],
    },
)