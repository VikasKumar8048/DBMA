from setuptools import setup, find_packages

setup(
    name="dbma",
    version="1.0.0",
    description="Database Management Agent — AI-powered MySQL CLI",
    packages=find_packages(exclude=["tests*", "*.egg-info"]),
    python_requires=">=3.10",
    install_requires=[
        "textual>=0.47.0",
        "rich>=13.7.0",
        "mysql-connector-python>=8.3.0",
        "psycopg2-binary>=2.9.9",
        "ollama>=0.1.8",
        "langchain>=0.1.0",
        "langchain-community>=0.0.20",
        "langchain-core>=0.1.0",
        "python-dotenv>=1.0.0",
        "pydantic>=2.5.0",
        "pydantic-settings>=2.1.0",
        "loguru>=0.7.2",
        "prompt_toolkit>=3.0.43",
        "click>=8.1.7",
        "tabulate>=0.9.0",
    ],
    entry_points={
        "console_scripts": [
            "dbma=main:cli",
        ],
    },
)