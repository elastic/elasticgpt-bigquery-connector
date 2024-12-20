from setuptools import setup, find_packages

setup(
    name="bq-es-project",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery",
        "bigframes",
        "python-dotenv",
        "elasticsearch",
        "langchain-openai",
        "langchain-text-splitters",
        "markdownify",
        "tqdm",
    ],
    python_requires=">=3.8",
) 