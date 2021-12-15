"""
setuptools setup module.
"""

from setuptools import setup, find_packages
import pathlib

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')

setup(
    # Project Name, Required
    name='pysetl',

    # Package version, Required
    version='0.1.0',

    # One line description, Optional
    description='Python Simple ETL framework',

    # PyPI Front Page body, Optional
    long_description=long_description, 

    # Long description content
    # Valid options:
    # text/plain, text/x-rst, and text/markdown, Optional
    long_description_content_type='text/markdown', 

    # Project main page, Optional
    url='https://gitlab.com/algorila/pysetl',

    # Developer name, Optional
    author='Algorila',

    # Author email, Optional
    author_email='mcromanceva@outlook.com',

    # Classifiers: https://pypi.org/classifiers/, Optional
    classifiers=[  
        # Project maturity
        'Development Status :: 1 - Planning',

        # Audience
        'Intended Audience :: Developers',

        # Topics
        'Topic :: Software Development :: Libraries :: Python Modules',

        # License
        'License :: OSI Approved :: MIT License',

        # Python version support
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        "Programming Language :: Python :: 3.10",
        'Programming Language :: Python :: 3 :: Only',
    ],

    # Keywords, Optional
    keywords="spark, etl, databases",

    # Project source code, Optional
    package_dir={'': '.'}, 

    # Packages directories, Required
    packages=find_packages(where='.'), 

    # Python version check
    python_requires='>=3.8, <4',

    # Insalation requirements, Optional
    install_requires=[ ], 

    # Extra requirements, Optional
    extras_require={  
        'dev': [ ],
        'test': [ ],
    },

    # Data and additional files, Optional
    # package_data={},
    # data_files=[], 

    # Exdcutable scripts, Optional
    entry_points={},

    # Aditional URLs, Optional
    project_urls={ 
        'Bug Reports': 'https://gitlab.com/algorila/pysetl/-/issues',
        'Source': 'https://gitlab.com/algorila/pysetl/-/tree/main'
    },
)