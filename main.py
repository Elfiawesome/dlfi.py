import argparse
import logging
from dlfi.logger import setup_logging


def main():
	parser = argparse.ArgumentParser(description="DLFI Archive Manager")
	parser.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
	parser.add_argument("--port", "-p", type=int, default=8080, help="Server port (default: 8080)")
	parser.add_argument("--debug", action="store_true", help="Enable debug logging")
	parser.add_argument("--vaults-dir", "-d", default=".archive", help="Default directory for vaults (default: .vaults)")
	
	args = parser.parse_args()
	
	setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
	
	from pathlib import Path
	from dlfi_server import run_server
	from dlfi_server.config import ServerConfig
	
	config = ServerConfig(
		host=args.host,
		port=args.port,
		debug=args.debug,
		default_vaults_dir=Path(args.vaults_dir).resolve()
	)
	
	run_server(config)


if __name__ == "__main__":
	main()