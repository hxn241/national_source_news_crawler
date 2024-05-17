import logging
import time

from ingestaweb.modules.scraping import Scraper
import datetime as dt

from ingestaweb.modules.utils import create_folder_name
from ingestaweb.settings import DownloadStatus

logger = logging.getLogger(__name__)


class Source:
    def __init__(self, source):
        self.name = source.get('name')
        self.dirname = create_folder_name(source.get('name'))
        self.weekdays = source.get('weekdays')
        self.edition = source.get('edition')
        self.is_weekday_relevant = self.set_weekday_relevant()
        self.frequency = source.get('frequency')
        self.start_at = int(source.get('start_at', '0'))
        self.downloaded = DownloadStatus.DEFAULT

    def set_weekday_relevant(self):
        weekday = dt.datetime.now().weekday() + 1
        if weekday in self.weekdays:
            return True
        else:
            return False

    def is_downloaded(self):
        return True if self.db.downloaded == DownloadStatus.SUCCESS else False

    def has_to_be_downloaded(self):
        if hasattr(self, 'dbsource'):
            return True
        return False


class RootSource(Scraper):
    def __init__(self, rsource):
        super().__init__(webdriver=False)
        self.id = rsource.get('id')
        self.is_active = rsource.get('is_active')
        self.name = rsource.get('name')
        self.dirname = create_folder_name(rsource.get('name'))
        self.login_info = rsource.get('login')
        # self.sources = {}
        self.sources = self.set_sources(rsource.get('sources'))
        self.download_conf = rsource.get('download_config')

    @staticmethod
    def set_sources(raw_sources):
        return [Source(raw_source) for raw_source in raw_sources]

    def get_source_by_name(self, source_name):
        return next(filter(lambda x: x.name == source_name, self.sources), None)

    def get_source_by_id(self, source_id):
        return self.sources.get(source_id)

    def load_sources(self, raw_sources):
        for source_data in raw_sources:
            if source_data.get('root_id') == self.id:
                self.add_source(source_data)

    def handle_login_status(self):
        if self.login_info.get('type') == 'nologin':
            status_logged_in = 'No login needed'
        elif self.logged_in:
            status_logged_in = 'Logged in'
        else:
            status_logged_in = 'Login failed'
        return status_logged_in

    def navigate_to_page(self, url, validation=None):
        try:
            self.is_webdriver_up(root_source_name=self.name)
            self.chrome.get(url)
            if validation:
                page_reached = self.page_validation(validation)
                assert page_reached is True
        except Exception as e:
            print(e)
            logger.error(f'Failed navigate to url {url}. \n Error: {e}')

    def navigate_to_login(self, url, cookies=None, validation=None, iframe_before_cookies=None):
        try:
            self.is_webdriver_up(root_source_name=self.name)
            self.chrome.get(url)
            if iframe_before_cookies:
                self.move_to_iframe(iframe_before_cookies)
            if cookies:
                self.accept_notifications(cookies)
            if validation:
                page_reached = self.page_validation(validation)
                assert page_reached is True
            self.chrome.switch_to.default_content()
        except Exception as e:
            print(e)
            logger.error(f'Failed navigate to url {url}. \n Error: {e}')
            
