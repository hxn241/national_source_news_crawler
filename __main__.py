import os
import datetime as dt
import argparse
import sys

from ingestaweb.modules.db import Db
from common.basic_utils import setup_logger
from ingestaweb.modules.ftp import FTPClient
from ingestaweb.settings import ROOT_DIR, sources_config, Config

from locale import setlocale, LC_TIME
from ingestaweb.modules.main_helpers import load_root_sources_from_config, extract_sources_from_root, \
    set_sources_to_be_donwloaded, download_pdf_files
# setlocale(LC_TIME, "es_ES")
setlocale(LC_TIME, "es_ES.utf8")

logger = setup_logger(
    log_filename=f"{dt.datetime.now():%Y%m%d}ftp_ingestaweb.log",
    src_path=os.path.join(ROOT_DIR, "data")
)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--source_recurrence',
                        help='Set sources frequency to be scraped',
                        type=str,
                        choices=['all', 'daily', 'monthly', 'weekly'],
                        default='all', required=False)

    args = parser.parse_args()
    source_recurrence = args.source_recurrence

    logger.info("Process started\n")
    # Init Db
    db = Db()
    # Init ftp session
    ftp = FTPClient(default_path=Config.FTP_DEFAULT_PATH)
    # Load root sources from gsheets
    root_sources = load_root_sources_from_config(sources_config, ftp)

    # From all sources get the ones for today that are not downloaded yet.
    # Extract all sources from root sources
    all_sources = extract_sources_from_root(root_sources)

    # Load sources with download status to db (if not done yet)
    db.load_sources(all_sources)

    # Query db and set sources to be downloaded within each root source.
    root_sources = set_sources_to_be_donwloaded(root_sources, source_recurrence, db)

    # Download process
    download_pdf_files(root_sources, db, ftp)

    db.session.close()

    logger.info(
        "Process finished\n" + "=" * 60 + "\n\n"
    )





















