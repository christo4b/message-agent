from setuptools import setup, find_packages

setup(
    name="message-agent",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "langchain",
        "langchain-community",
        "langchain-core",
        "langchain-ollama",
        "pydantic>=2.0",
        "click",
    ],
    entry_points={
        'console_scripts': [
            'message-agent=src.cli:cli',
        ],
    },
) 