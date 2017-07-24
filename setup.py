"""
# setup.py

Revision 0.1.2

Clive Gross
Schneider Electric
2017

## License

## Description
Setup script for building the msi using cx_Freeze

## Example

    ```
    $ python setup.py bdist_msi
    ```

## Todo

 * Write the docstring

"""
import sys
from cx_Freeze import setup, Executable

COMPANY_NAME = 'Schneider Electric'
PRODUCT_NAME = 'Medusa System Agent'
VERSION = '0.1.2'
DESCRIPTION = "Runs scheduled jobs to backup files to the cloud."
EXECUTABLE = 'medusa-agent.py'
TARGET = 'medusa-agent.exe'

INCLUDE_FILES = [
    'sample-config.ini',
    'README.md',
    'docs',
    'LICENSE',
    'dbexporter.py',
    'logger.py',
    'statedb.py'
]

# Dependencies are automatically detected, but it might need fine tuning.
PACKAGES = [
    "os",
    "sys",
    "configparser",
    "pymssql",
    "csv",
    "logging",
    "decimal",
    "_mssql",
    "uuid",
    "codecs"
]

BUILD_EXE_OPTIONS = {
    "packages": PACKAGES,
    "excludes": ["tkinter"],
    'include_files': INCLUDE_FILES
}

BDIST_MSI_OPTIONS = {
    'add_to_path': True,
    'initial_target_dir': r'[ProgramFilesFolder]\%s\%s' % (COMPANY_NAME, PRODUCT_NAME),
}

OPTIONS = {
        'build_exe': BUILD_EXE_OPTIONS,
        'bdist_msi': BDIST_MSI_OPTIONS
}

# GUI applications require a different base on Windows (the default is for a
# console application).
base = None

executables = [
    Executable(
        EXECUTABLE,
        base=base,
        targetName=TARGET
    )
]

setup(
    name = PRODUCT_NAME,
    version = VERSION,
    description = DESCRIPTION,
    options = OPTIONS,
    executables=executables
)