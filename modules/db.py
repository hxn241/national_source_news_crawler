import logging
import os
import datetime as dt
from sqlalchemy import create_engine, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DateTime

from ingestaweb.settings import DownloadStatus
from settings import ROOT_DIR

Base = declarative_base()

logger = logging.getLogger(__name__)


class DbSource(Base):
    __tablename__ = "source"
    id = Column(Integer, autoincrement=True, primary_key=True)
    source_name = Column(String)
    date = Column(Date)
    is_relevant = Column(String)
    downloaded = Column(String, default=DownloadStatus.DEFAULT)
    downloaded_at = Column(DateTime(timezone=False), default=None)


class Db:
    db_path = os.path.join(ROOT_DIR, 'ingestaweb', 'descargaweb.db')

    def __init__(self):
        self.session = self.connect(self.db_path)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    def connect(self, db_path):
        engine = create_engine(f"sqlite:///{db_path}")
        Session = sessionmaker(bind=engine)
        Base.metadata.create_all(engine)
        return Session()

    def load_sources(self, sources):
        """
        load sources
        """
        try:
            source = self.session.query(DbSource).order_by(desc('date')).first()
            last_day = source.date if source else dt.datetime(1970, 1, 1).date()
            if dt.datetime.now().date() > last_day:
                self.load_sources_by_frequency(sources)
        except Exception as e:
            logger.error(f'Error while loading and create dbsource object: \n {e}')

    def load_sources_by_frequency(self, sources):
        for source in sources:
            create = True if source.frequency in ['monthly', 'weekly'] and dt.datetime.now().day == 1 \
                             or source.frequency == 'daily' else False
            if create:
                dbsource = DbSource(
                    source_name=source.name,
                    date=dt.datetime.now().date(),
                    is_relevant=source.is_weekday_relevant
                )
                self.session.add(dbsource)
            self.session.commit()

    def get_sources_to_be_downloaded(self, report=False):
        to_be_donwloaded = self.session.query(DbSource) \
            .filter(DbSource.is_relevant == 1) \
            .all() \
            if report else\
            self.session.query(DbSource) \
            .filter(DbSource.is_relevant == 1, DbSource.downloaded.in_(['failed', 'unavailable', 'unprocessed'])) \
            .all()
        # to_be_donwloaded = self.session.query(DbSource).filter(DbSource.is_relevant == 0, DbSource.downloaded == "False").all()
        return to_be_donwloaded if to_be_donwloaded else []

    # @staticmethod
    # def set_downloaded_codes_to_text(report_list):
    #     for sublist in report_list:
    #         if sublist[3] in ['failed', 'unavailable']:
    #             sublist[3] = 'pending'
    #     return report_list

    def get_sources_filtered_report(self, root_sources):
        sources_reported = []
        sources_to_be_donwloaded = self.get_sources_to_be_downloaded(report=True)
        for dbsource in sources_to_be_donwloaded:
            for root_source in root_sources:
                found_source = root_source.get_source_by_name(dbsource.source_name)
                if found_source:
                    sources_reported.append([
                        dbsource.source_name,
                        found_source.frequency,
                        str(dbsource.date),
                        dbsource.downloaded,
                        dbsource.downloaded_at
                    ])

        # sources_reported = self.set_downloaded_codes_to_text(sources_reported)
        return sources_reported


