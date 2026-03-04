from setuptools import setup, find_packages

setup(
    name="trishul_watch",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi>=0.95.1",
        "uvicorn>=0.22.0",
        "psycopg2-binary>=2.9.6",
        "requests>=2.30.0",
        "pydantic>=1.10.7",
    ],
    extras_require={
        "dev": [
            "python-dotenv>=1.0.0",
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="Trishul Watch: a standalone application for monitoring cryptocurrency prices in real-time",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/Sharex508/trishul-watch",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
