import argparse
import logging
from dlfi.logger import setup_logging
from dlfi.server import DLFIServer


def main():
	parser = argparse.ArgumentParser(description="DLFI Archive Manager")
	parser.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
	parser.add_argument("--port", "-p", type=int, default=8080, help="Server port (default: 8080)")
	parser.add_argument("--debug", action="store_true", help="Enable debug logging")
	
	args = parser.parse_args()
	
	setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
	
	server = DLFIServer(host=args.host, port=args.port)
	
	try:
		server.start(blocking=True)
	except KeyboardInterrupt:
		logging.info("Shutting down...")
	finally:
		server.stop()


if __name__ == "__main__":
	main()