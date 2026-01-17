import logging
from dlfi import DLFI
from dlfi.logger import setup_logging
from dlfi.job import Job, JobConfig

ARCHIVE_DIR = ".archive/archive"
EXPORT_DIR = ".archive/export"

if __name__ == "__main__":
    setup_logging()
    
    try:
        db = DLFI(ARCHIVE_DIR)

        job = Job(JobConfig("C:/Users/elfia/OneDrive/Desktop/DLFI.py/.archive/cookies/cookies.txt"))
        job.db = db
        
        job.run("https://poipiku.com/379309/1806892.html")
        # job.run("https://poipiku.com/10085584/", {"password":"yes"})
        # job.run("https://poipiku.com/11581691/12396628.html")
        # job.run("https://poipiku.com/10085584/11726312.html", {"password_list": ["","","","","","","","","","","","","","","","","","","","","","","","yes"]})

        db.export(EXPORT_DIR)
    except Exception as e:
        logging.critical("Unhandled application error", exc_info=True)
    finally:
        # Ensure DB is closed even if errors occur
        if 'db' in locals():
            db.close()