from setuptools import setup, find_packages

setup(
    name='your_project_name',
    version='0.1.0',
    description='A brief description of your project',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/FLippifyDev/webscraper',
    author='REN',
    author_email='dev@flippify.com',
    license='MIT',
    packages=find_packages(),
    install_requires=[
        # List your project's dependencies here
        # Example: 'numpy', 'requests',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)