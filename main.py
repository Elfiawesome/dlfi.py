from dlfi import DLFI
from dlfi.job import Job, JobConfig

ARCHIVE_DIR = ".archive/archive"
EXPORT_DIR = ".archive/export"

if __name__ == "__main__":
    db = DLFI(ARCHIVE_DIR)

    job = Job(JobConfig("C:/Users/elfia/OneDrive/Desktop/DLFI.py/.archive/cookies/cookies.txt"))
    job.db = db
    job.run("https://poipiku.com/11581691/12396628.html")

    db.export(EXPORT_DIR)
    db.close()

