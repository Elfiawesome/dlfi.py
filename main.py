import logging
from dlfi import DLFI
from dlfi.logger import setup_logging
from dlfi.job import Job, JobConfig

ARCHIVE_DIR = ".archive"

if __name__ == "__main__":
	setup_logging()
	
	db = None
	try:
		# Initialize archive
		# For a NEW encrypted archive: pass password
		# For an EXISTING encrypted archive: pass the same password
		# For unencrypted: pass None or omit
		db = DLFI(ARCHIVE_DIR, password=None)
		
		# ============================================================
		# RUN EXTRACTION JOBS
		# ============================================================
		
		# job = Job(JobConfig(".secret/cookies.txt"))
		# job.db = db
		# job.run("https://poipiku.com/379309/1806892.html")
		# job.run("https://poipiku.com/10085584/", {"password": "yes"})

		# db.create_record("yo")
		# db.append_file("yo", "C:\\Users\\elfia\\OneDrive\\Desktop\\DLFI.py\\.archive\\vid.mp4")

		# ============================================================
		# ENCRYPTION EXAMPLES
		# ============================================================
		
		# Enable encryption on an existing unencrypted vault:
		# db.config_manager.enable_encryption("my_secret_password")
		
		# Disable encryption (requires current password):
		# db.config_manager.disable_encryption("my_secret_password")
		
		# Change password (requires old password):
		# db.config_manager.change_password("old_password", "new_password")
		
		# ============================================================
		# PARTITION SIZE EXAMPLES
		# ============================================================
		
		# Change to 25MB chunks (for smaller GitHub-friendly files):
		# db.config_manager.change_partition_size(25 * 1024 * 1024)
		
		# Change to 100MB chunks:
		# db.config_manager.change_partition_size(100 * 1024 * 1024)
		
		# Disable partitioning entirely:
		# db.config_manager.change_partition_size(0)
		
		# ============================================================
		# COMBINED RECONFIGURATION
		# ============================================================
		
		# Change multiple settings at once:
		# db.config_manager.reconfigure(
		#     password="current_password",        # Required if currently encrypted
		#     new_password="new_password",        # Optional: change password
		#     enable_encryption=True,             # Optional: True/False/None
		#     partition_size=25 * 1024 * 1024     # Optional: new chunk size
		# )

		# Generate static site (index.html + manifest.json in archive root)
		# Blobs are shared - no duplication
		db.generate_static_site()
		
		logging.info(f"Archive ready at: {ARCHIVE_DIR}/")
		logging.info(f"Open {ARCHIVE_DIR}/index.html in a browser to view.")
		logging.info(f"Encrypted: {db.config.encrypted}")
		logging.info(f"Partition size: {db.config.partition_size} bytes")

	except Exception as e:
		logging.critical("Unhandled application error", exc_info=True)
	finally:
		if db:
			db.close()