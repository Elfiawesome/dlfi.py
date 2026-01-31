import logging, sys

def setup_logging(level = logging.INFO):
	root_logger = logging.getLogger()
	root_logger.setLevel(level)

	handler = logging.StreamHandler(sys.stdout)
	
	formatter = logging.Formatter(
		"[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
		datefmt="%H:%M:%S"
	)
	handler.setFormatter(formatter)
	root_logger.addHandler(handler)