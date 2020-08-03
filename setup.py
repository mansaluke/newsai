from setuptools import setup, find_packages


setup(
    name="newsai",
    version="0.9",
    author="Luke Mcleary",
    author_email="lukemcleary95@gmail.com",
    description="News API",
    long_description=open("README.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    keywords="News API webscraper asyncio NLP deep learning transformer pytorch",
    license="",
    url="https://github.com/mansaluke/newsai",
    package_dir={"": "src"},
    packages=find_packages("src"),
    include_package_data=True,
    # package_data={"": ["../data/*.csv"]},
    install_requires=[
        "aiohttp",
        "bs4",
        "lxml",
        "nltk"
    ],
    # scripts=["async_download"],
    python_requires=">=3.6.0",  # 3.7 needed for download functionality
    classifiers=[
        "Development Status :: Development/Unstable",
        'Programming Language :: Python',
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
    ],
)
