from dlfi import DLFI
from dlfi.logger import setup_logging
import logging

setup_logging(level = logging.DEBUG)

def main() -> None:
	dlfi = DLFI(".archive/my-archive", password="COMPLEX_PASSWORD")

main()