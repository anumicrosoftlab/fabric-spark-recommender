from setuptools import setup, find_packages

setup(
    name="spark_app_analyzer",
    version="0.1.1",
    description="Analyze Spark app event logs to extract performance metrics and recommendations",
    author="Anu Venkataraman",
    author_email="anuve@microsoft.com",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.0.0",
        "pandas>=1.0.0"
    ],
    python_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent"
    ],
)