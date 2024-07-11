from setuptools import setup, find_packages

setup(
    name='webscraper',
    version='0.1.11',
    description='Web Scraper designed to scrape any website',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/FLippifyDev/webscraper',
    author='REN',
    author_email='dev@flippify.com',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        "aiohttp",
        "beautifulsoup4",
        "fake_headers",
        "setuptools"
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)