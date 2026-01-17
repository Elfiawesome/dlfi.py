from dlfi import DLFI

ARCHIVE_DIR = ".archive/archive"
EXPORT_DIR = ".archive/export"

if __name__ == "__main__":
	db = DLFI(ARCHIVE_DIR)



	db.export(EXPORT_DIR)