import logging
import datetime as dt
from ingestaweb.modules.ftp import create_daily_base_directories_ftp
from ingestaweb.modules.sources import RootSource

logger = logging.getLogger(__name__)


def is_relevant_source(source):
    """
    filtered sources by "start_at" parameter that allows to load sources after specific hour. default is 0
    """
    if dt.datetime.now().hour >= source.start_at:
        return True
    else:
        return False


def is_recurrent(source, source_recurrence):
    """
    filtered sources by parameter argparse source_recurrence passed on main (daily, monthly, all).
    """
    if source.frequency == source_recurrence or source_recurrence == 'all':
        return True
    return False


def load_root_sources_from_config(sources_config, ftp=None, create_base_directories=True):
    root_sources = []
    raw_root_sources = sources_config.get('root_sources')
    for rsource_data in raw_root_sources:
        root_source = RootSource(rsource_data)
        root_sources.append(root_source)
    active_root_sources = [root_source_active for root_source_active in root_sources if root_source_active.is_active]
    if create_base_directories:
        create_daily_base_directories_ftp(active_root_sources, ftp)
    return active_root_sources


def extract_sources_from_root(root_sources):
    """
    Determine source to be scraped based on recurrence type.
        source_recurrence = all --> returns all sources
        source_recurrence = daily/monthly/weekly returns only this source type.
    """
    return list(set(
        source for root_source in root_sources for source in root_source.sources
    ))


def set_sources_to_be_donwloaded(root_sources, source_recurrence, db):
    """
    - Get sources to be downloaded (today) from database
    - For each source to be downloaded:
        i. Find the source in root sources
        ii. When found, set whole class object as attribute
            This is necessary since we will want to update state
            in database item (not downloaded -> downloaded)
    """
    sources_to_be_donwloaded = db.get_sources_to_be_downloaded(report=False)
    for to_be_donwloaded in sources_to_be_donwloaded:
        for root_source in root_sources:
            found_source = root_source.get_source_by_name(to_be_donwloaded.source_name)
            if found_source:
                if is_relevant_source(found_source) and is_recurrent(found_source, source_recurrence):
                    found_source.dbsource = to_be_donwloaded
    return root_sources


def download_pdf_files(root_sources, db, ftp):
    downloaded = 'failed'
    logger.info(f"ROOT_SOURCES: {len(root_sources)}")
    to_be_donwloaded_all = [[source for source in root_source.sources if source.has_to_be_downloaded()] for root_source
                            in root_sources]
    lengths = sum([len(item) for item in to_be_donwloaded_all])
    logger.info(f"TOTAL SOUCES TO BE DOWNLOADED: {lengths}\n")
    for root_source in root_sources:
        # to_be_donwloaded = filtered_sources(root_source)
        to_be_donwloaded = [source for source in root_source.sources if source.has_to_be_downloaded()]
        logger.info(f"ROOT_SOURCE: {root_source.name}")
        logger_to_be_downloaded = f'Sources with pdf to be downloaded: {len(to_be_donwloaded)}'
        logger.info(f'{logger_to_be_downloaded}') if len(to_be_donwloaded) > 0 else logger.info(
            f'{logger_to_be_downloaded}\n')
        if to_be_donwloaded:
            root_source.login(root_source.login_info.get('type'))
            login_status = root_source.handle_login_status()
            for source in to_be_donwloaded:
                try:
                    logger.info(f"- SOURCE: {source.name}")
                    downloaded = root_source.download(root_source.download_conf.get('type'), source, ftp) #failed, downloaded, unavailable
                    # status_download = 'Download sucessfull' if downloaded else 'Download failed'
                    logger.info(f"Download status: {downloaded}")
                except Exception as e:
                    logger.error(f"\n Failed --> Login --> \n\tError: {e}") if 'failed' in login_status else None
                    logger.error(f"\n Failed --> Download --> \n\tError: {e}") if downloaded == 'failed' else None
                source.dbsource.downloaded = downloaded
                if downloaded == 'downloaded':
                    source.dbsource.downloaded_at = source.dbsource.downloaded_at = dt.datetime.now().replace(
                        microsecond=0, second=0)
                    db.session.commit()
        root_source.close_webdriver_session()
