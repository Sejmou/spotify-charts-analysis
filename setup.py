from setuptools import find_packages, setup

setup(
    name="helpers",
    version="0.1",
    description="A package with helpers for this project that also contains all packages used inside the helpers package or notebooks/script files in this repo.",
    author="Samo Kolter",
    author_email="samo.kolter@gmail.com",  # picked arbitrary group member email
    license="MIT",
    install_requires=[
        "pandas",
        "ipykernel",
        "matplotlib",
        "seaborn",
        "pyarrow",
        "python-dotenv",
        "tqdm",
        "selenium",
    ],
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
    ],
    zip_safe=False,
)
