"""
A standalone script for use by PyInstaller.

Users should not be running this script.
"""

import sys
from flintrock.flintrock import main

if __name__ == '__main__':
    sys.exit(main())
