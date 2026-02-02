from dlfi import DLFI
from dlfi.logger import setup_logging
import logging

def main() -> None:
	setup_logging(level = logging.DEBUG)
	dlfi = DLFI(".archive/my-archive", password="COMPLEX_PASSWORD")

main()