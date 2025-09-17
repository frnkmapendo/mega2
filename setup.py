#!/usr/bin/env python3
"""
Setup script for MEGA 2.0 CLI application.
This provides backward compatibility for systems that don't support pyproject.toml.
"""

from setuptools import setup, find_packages
import os


def read_long_description():
    """Read the long description from README.md if it exists."""
    here = os.path.abspath(os.path.dirname(__file__))
    readme_path = os.path.join(here, 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "ODK Central Data Downloader and PDF Report Generator"


def read_requirements():
    """Read requirements from requirements.txt if it exists."""
    here = os.path.abspath(os.path.dirname(__file__))
    req_path = os.path.join(here, 'requirements.txt')
    if os.path.exists(req_path):
        with open(req_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return [
        'pandas>=1.3.0',
        'requests>=2.25.0',
        'reportlab>=3.6.0',
        'matplotlib>=3.3.0',
        'seaborn>=0.11.0',
        'PyYAML>=5.4.0',
        'openpyxl>=3.0.0'
    ]


setup(
    name="mega2-cli",
    version="1.0.0",
    description="ODK Central Data Downloader and PDF Report Generator",
    long_description=read_long_description(),
    long_description_content_type="text/markdown",
    author="MEGA 2.0 Team",
    author_email="contact@example.com",
    url="https://github.com/frnkmapendo/mega2",
    project_urls={
        "Bug Tracker": "https://github.com/frnkmapendo/mega2/issues",
        "Documentation": "https://github.com/frnkmapendo/mega2#readme",
        "Source Code": "https://github.com/frnkmapendo/mega2",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    include_package_data=True,
    package_data={
        "": ["*.yaml", "*.yml", "*.json", "*.txt"],
    },
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0",
            "pytest-cov>=2.12",
            "black>=21.0",
            "flake8>=3.9",
            "mypy>=0.910",
        ],
    },
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "mega2-cli=mega2_cli.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Scientific/Engineering",
        "Topic :: Utilities",
    ],
    keywords="odk central data download pdf report generator cli",
    zip_safe=False,
)