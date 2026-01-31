import logging
from dlfi import DLFI
from dlfi.logger import setup_logging
from dlfi.job import Job, JobConfig

ARCHIVE_DIR = ".archive"

if __name__ == "__main__":
	setup_logging()
	
	db = None
	try:
		# Initialize archive with optional encryption
		# Pass password=None for unencrypted, or password="your_password" for encrypted
		db = DLFI(ARCHIVE_DIR, password=None)  # Change to enable encryption
		
		# Example: Enable encryption on existing vault
		# db.config_manager.enable_encryption("my_secret_password")
		
		# Example: Change partition size (50MB default, set to 0 to disable)
		# db.config_manager.change_partition_size(25 * 1024 * 1024)  # 25MB chunks
		
		# Example: Change password
		# db.config_manager.change_password("old_password", "new_password")
		
		# Run extraction jobs
		job = Job(JobConfig(".secret/cookies.txt"))
		job.db = db
		
		job.run("https://poipiku.com/379309/1806892.html")
		# job.run("https://poipiku.com/10085584/", {"password": "yes"})

		# Generate static site (index.html + manifest.json in archive root)
		# Blobs are shared - no duplication
		db.generate_static_site()
		
		logging.info(f"Archive ready. Open {ARCHIVE_DIR}/index.html to view.")

	except Exception as e:
		logging.critical("Unhandled application error", exc_info=True)
	finally:
		if db:
			db.close()