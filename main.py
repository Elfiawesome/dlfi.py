import argparse
import logging
from dlfi import DLFI
from dlfi.logger import setup_logging
from dlfi.server import DLFIServer
from dlfi.job import Job, JobConfig

ARCHIVE_DIR = ".archive"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8080


def main():
	parser = argparse.ArgumentParser(description="DLFI Archive Manager")
	parser.add_argument("--archive", "-a", default=ARCHIVE_DIR, help="Archive directory path")
	parser.add_argument("--password", "-p", default=None, help="Vault password (for encrypted vaults)")
	parser.add_argument("--host", default=DEFAULT_HOST, help="Server host")
	parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="Server port")
	parser.add_argument("--scrape", "-s", nargs="*", help="URLs to scrape before starting server")
	parser.add_argument("--cookies", "-c", default=None, help="Cookie file for scraping")
	parser.add_argument("--no-server", action="store_true", help="Don't start web server (scrape only)")
	parser.add_argument("--static-only", action="store_true", help="Generate static site and exit")
	parser.add_argument("--debug", action="store_true", help="Enable debug logging")
	
	args = parser.parse_args()
	
	setup_logging(level=logging.DEBUG if args.debug else logging.INFO)
	
	db = None
	try:
		# Initialize archive
		db = DLFI(args.archive, password=args.password)
		logging.info(f"Opened archive: {args.archive}")
		logging.info(f"Encrypted: {db.config.encrypted}, Partition size: {db.config.partition_size}")
		
		# Run scrape jobs if specified
		if args.scrape:
			job = Job(JobConfig(args.cookies))
			job.db = db
			
			for url in args.scrape:
				logging.info(f"Scraping: {url}")
				job.run(url)
		
		# Generate static site if requested
		if args.static_only:
			db.generate_static_site()
			logging.info(f"Static site generated at {args.archive}/index.html")
			return
		
		# Start server unless disabled
		if not args.no_server:
			server = DLFIServer(db, host=args.host, port=args.port)
			logging.info(f"Starting server at http://{args.host}:{args.port}")
			logging.info("Press Ctrl+C to stop")
			server.start(blocking=True)
	
	except KeyboardInterrupt:
		logging.info("Shutting down...")
	except Exception as e:
		logging.critical(f"Fatal error: {e}", exc_info=True)
	finally:
		if db:
			db.close()
			logging.info("Archive closed")


if __name__ == "__main__":
	main()