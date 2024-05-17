import os
import random
import re
import time

import PyPDF2
import requests
import datetime as dt
import logging
from urllib.parse import urlparse
from lxml import etree
from requests.auth import HTTPBasicAuth
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, NoSuchElementException, TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from deep_translator import GoogleTranslator

from ingestaweb.modules.img_converter import build_pages
from ingestaweb.modules.utils import parse_date_fmt_to_current_date, convert_date_to_source_fmt, \
    make_pdf_file,  retry,  delete_files_in_folder, \
    check_if_file_downloaded_and_rename_file, create_pdf_from_images, remove_jpg_files

from ingestaweb.settings import TEMP_DIR_FILES, TEMP_DIR_IMAGES, Config, DownloadStatus

logger = logging.getLogger(__name__)


class Login:

    def __init__(self, scraper):
        self.scraper = scraper
        self.types = {
            'requests_basic': self.requests_basic,
            'selenium_basic': self.selenium_basic
        }

    def perform_login(self, login_type, navigate=True):
        if not login_type == 'nologin':
            self.scraper.logged_in = self.types.get(login_type)() if login_type == 'requests_basic' \
                else self.types.get(login_type)(navigate)

    @retry(max_retries=3, wait_time=10)
    def requests_basic(self):
        is_logged_in = False
        try:
            self.scraper.create_session()
            response = self.scraper.remote_session.post(
                self.scraper.login_info.get('login_url'),
                data=self.scraper.login_info.get('payload')
            )

            if response.ok:
                is_logged_in = True
            else:
                logger.error(f"\tError trying to login with request. Status code: {response.status_code}")
                raise Exception(f"Login failed with status code: {response.status_code}")
        except Exception as e:
            logger.error(f"\tLogin error: {e}")
        finally:
            return is_logged_in

    @retry(max_retries=1, wait_time=10)
    def selenium_basic(self, navigate=True):
        """
        1 -  navigate to login page
        2 - check iframe after login and switch to it
        3 - check if multistep
        4 - send keys user pass and login
        """
        validation = False
        try:
            if navigate:
                self.scraper.navigate_to_login(
                    url=self.scraper.login_info.get('login_url'),
                    validation=self.scraper.login_info.get('page_loaded_validator'),
                    iframe_before_cookies=self.scraper.login_info.get('xpath_iframe_landing'),
                    cookies=self.scraper.login_info.get('cookies')
                )
            if self.scraper.login_info.get('xpath_button_login'):
                self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_button_login')).click()
            elif self.scraper.login_info.get('xpath_button_login_1'):
                self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_button_login_1')).click()
                self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_button_login_2')).click()
            time.sleep(random.uniform(1, 2))
            iframe_after_login_xpath = self.scraper.login_info.get('xpath_iframe_login_form')
            self.scraper.move_to_iframe(iframe_after_login_xpath)
            multistep = self.scraper.login_info.get('xpath_button_submit_1')

            # User
            self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_box_user')) \
                .send_keys(self.scraper.login_info.get('user'))

            # Submit if multistep
            if multistep:
                self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_button_submit_1')).click()
            time.sleep(random.uniform(2, 4))
            # Password
            self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_box_pass')) \
                .send_keys(self.scraper.login_info.get('password'))
            time.sleep(random.uniform(2, 4))

            # Submit
            if navigate:
                if self.scraper.login_info.get('keys'):
                    self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_box_pass')) \
                        .send_keys(Keys.ENTER)
                else:
                    self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_button_submit')).click()
            else:
                self.scraper.search_element_by_path(self.scraper.login_info.get('xpath_button_submit_relogin')).click()
            time.sleep(random.uniform(2, 4))
            self.scraper.accept_notifications(self.scraper.login_info.get('cookies'), sleep=5)
            if self.scraper.login_info.get('login_validation'):
                self.scraper.chrome.switch_to.default_content()
                validation = self.scraper.login_validation(
                    self.scraper.login_info.get('login_validation')) if navigate else True
                if not validation:
                    raise Exception("Login failed...")
            else:
                validation = True
        except Exception as e:
            logger.error(f"\tUnexpected login error. \n Error: {e}")
        finally:
            return validation


class Download:
    def __init__(self, scraper):
        self.scraper = scraper
        self.d_types = {
            "standard_requests": self.download_pdf_content,
            "argia": self.argia,
            "3devuit": self.tresdevuit,
            "elcultural": self.elcultural,
            "lacomarca": self.lacomarca,
            "vidaeconomica": self.vidaeconomica,
            "eleconomista": self.eleconomista,
            "gacetamedica_elglobal": self.gacetamedica_elglobal,
            "lavozdegalicia": self.lavozdegalicia,
            "sudouest": self.sudouest,
            "barrons_wstreet": self.barrons_wallstreet,
            "laprensa": self.laprensa,
            "gentedigital": self.gentedigital,
            "diarisabadell": self.diarisabadell,
            "lavanguardia": self.lavanguardia,
            "eltemps": self.eltemps,
            "nuevaalcarria": self.nueva_alcarria,
            "elpuerto": self.elpuerto,
            'issuu': self.issuu,
            'lamanana': self.lamanana,
            'calameo': self.calameo,
            'kioskoymas': self.kioskoymas,
            'diariandorra': self.diariandorra
        }

    def perform_download(self, download_type, source, ftp):
        download = DownloadStatus.FAILED
        try:
            if not self.scraper.remote_session:
                self.scraper.create_session()
            download = self.d_types.get(download_type)(source, ftp)
        finally:
            return download

    @staticmethod
    def uppercase_between_words(url):
        pattern = re.compile(rf'({re.escape("DIARIOS")}.*$)', re.IGNORECASE)
        # Find the match in the sentence
        match = pattern.search(url)
        url = (url.replace(match.group(1), match.group(1).upper())).replace('PDF', 'pdf') if match else url
        return url

    def url_preprocess(self, source):
        post = True if self.scraper.download_conf.get('post') else False
        url = None
        try:
            if source.edition:
                url = parse_date_fmt_to_current_date(
                    url=self.scraper.download_conf.get('url').replace("{edition}", source.edition))
            else:
                url = parse_date_fmt_to_current_date(
                    url=self.scraper.download_conf.get('url')
                )
            if self.scraper.name == 'Viva':
                url = self.uppercase_between_words(url)
        except Exception as e:
            print(e)
        finally:
            return url, post

    @staticmethod
    def check_pdf_is_valid(file_path):
        with open(file_path, 'rb') as f:
            try:
                reader = PyPDF2.PdfReader(f)
                num_pages = len(reader.pages)
                if num_pages > 0:
                    return True
                else:
                    return False
            except Exception as e:
                return False

    def download_pdf_content(self, source, ftp, url=None, filename=None, is_post_request=False, payload=None,
                             edition=False):
        downloaded = DownloadStatus.FAILED
        try:
            if not filename:
                filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
            # output_file_path = os.path.join(DOWNLOAD_DIR, self.scraper.name, f'{source.name}', filename)
            output_file_path = os.path.join(TEMP_DIR_FILES, filename)
            if self.scraper.download_conf.get('type') == 'standard_requests':
                url, is_post_request = self.url_preprocess(source)
            pdf_content = self.scraper.make_request(url, is_post_request=is_post_request, payload=payload)

            if pdf_content:
                try:
                    created_file = make_pdf_file(pdf_content, output_file_path)
                    is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                    if created_file and is_pdf_valid:
                        downloaded = DownloadStatus.SUCCESS
                        ftp.upload_file(
                            local_file_path=os.path.dirname(output_file_path),
                            ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                            filename=filename
                        )
                    delete_files_in_folder(os.path.dirname(output_file_path))
                except Exception as e:
                    logger.error(f"\tError while trying to save pdf \n Error:{e}")
            elif edition:
                logger.warning("\tImpossible to retrieve pdf_content or not today's edition")
                downloaded = "failed"
            else:
                downloaded = "unavailable"
        finally:
            return downloaded

    def set_download_path(self, file_path):
        params = {'behavior': 'allow', 'downloadPath': os.path.dirname(file_path)}
        self.scraper.chrome.execute_cdp_cmd('Page.setDownloadBehavior', params)
        self.scraper.chrome.execute_cdp_cmd('Page.printToPDF', {
            'printOptions': {
                'displayHeaderFooter': False,
                'preferCSSPageSize': True,
            },
            'outputFile': file_path
        })

    def issuu(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        editions_list = []
        try:
            def get_endpoint_form_url(url):
                try:
                    url_obj = urlparse(url)
                    return url_obj.path
                except:
                    return None

            def issuu_get_library_html():
                self.scraper.navigate(self.scraper.login_info.get('url'), cookies=["//*[contains(text(),'Allow cookies')]"])
                html = self.scraper.chrome.page_source
                html_tree = etree.fromstring(html, parser=etree.HTMLParser())
                return html_tree

            library_html_tree = issuu_get_library_html()
            date_format = '%B %#d, %Y'
            formatted_date = convert_date_to_source_fmt(date_format)
            formatted_date_en = self.scraper.translate_date_to_source_lang(self.scraper, formatted_date)
            xpath_today_edition = self.scraper.download_conf.get('url_edition').replace('{format_date}', formatted_date_en)
            if source.edition:
                xpath_today_edition = xpath_today_edition.replace('{edition}', source.edition)
            editions_found = self.scraper.get_xpath_field_from_html(library_html_tree, xpath_today_edition)
            if isinstance(editions_found, str):
                editions_list.append(editions_found)
            elif isinstance(editions_found, list):
                editions_list.extend(editions_found)
            if editions_list:
                downloaded_files = []
                for edition in editions_list:
                    edition_url = f'https://issuu.com{edition}'
                    endpoint = get_endpoint_form_url(edition_url)
                    time.sleep(3)
                    self.scraper.navigate_to_page(edition_url)
                    time.sleep(3)
                    html_edition = self.scraper.chrome.page_source
                    html_tree = etree.fromstring(html_edition, parser=etree.HTMLParser())
                    secure_url = html_tree.xpath("//meta[@property='og:image:secure_url']/@content")
                    if secure_url:
                        limit = False
                        secure_url = secure_url[0]
                        c = 1
                        while not limit:
                            print(f"Retrieving page {c}")
                            page_resp = self.scraper.make_request(secure_url.replace("page_1.jpg", f"page_{c}.jpg"),
                                                                  return_content=False)
                            if page_resp:  # page_resp.ok
                                with open(os.path.join(TEMP_DIR_IMAGES, f"page_{c:03d}.jpg"), 'wb') as img:
                                    img.write(page_resp.content)
                            else:
                                limit = True
                            c += 1
                        # filename = f"issuu_{endpoint.replace('/', '_')}.pdf"
                        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
                        output_file_path = os.path.join(TEMP_DIR_FILES, filename)

                        create_pdf_from_images(
                            images_directory=TEMP_DIR_IMAGES,
                            pdf_path=output_file_path)

                        remove_jpg_files(TEMP_DIR_IMAGES)
                        # downloaded_files.append(check_if_file_exists(output_file_path))
                        # downloaded = all(downloaded_files)
                        is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path, rename_raw_file=False)
                        is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                        if is_downloaded and is_pdf_valid:
                            ftp.upload_file(os.path.dirname(output_file_path), f"{self.scraper.dirname}/{source.dirname}",
                                            filename)
                            downloaded = DownloadStatus.SUCCESS
                            delete_files_in_folder(os.path.dirname(output_file_path))
            else:
                downloaded = "unavailable"
        finally:
            return downloaded

    def kioskoymas(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        try:
            self.scraper.navigate(url=self.scraper.login_info.get('url'))
            filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
            output_file_path = os.path.join(TEMP_DIR_FILES, filename)

            current_date = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
            current_date = re.sub(r'[^0-9a-zA-Z]*([a-zA-Z])', lambda x: x.group(0).upper(), current_date, count=1)
            xpath_today_edition = '//span[contains(text(),"{date_fmt}")]'.replace('{date_fmt}', current_date)
            # xpath_today_edition = self.scraper.translate_date_to_source_lang(
            #     source,
            #     parse_date_fmt_to_current_date(self.scraper.download_conf.get('url_edition'))
            # )
            edition = self.scraper.find_elements_on_page(xpath_today_edition)
            if edition:
                redirect_to_pdf = self.scraper.find_elements_on_page(
                    self.scraper.download_conf.get('url_pdf')).get_attribute(
                    'href')
                self.scraper.navigate(redirect_to_pdf, validation='//span[contains(text(),"La Region")]')
                self.scraper.find_elements_on_page("//*[contains(text(),'1 PORTADA')]", sleep=1).click()
                pages = self.scraper.find_elements_on_page("//p[contains(@id,'thumb')]", single_element=False, sleep=1)
                if pages:
                    total_pages = len(pages)
                    self.scraper.find_elements_on_page("//*[contains(text(),'1 PORTADA')]", sleep=5).click()
                has_next = True
                page = 1
                main_page = self.scraper.chrome.current_url
                while has_next:
                    url_page = f'{main_page}/page/{page}'
                    self.scraper.navigate(url_page, validation='//span[contains(text(),"La Region")]')
                    page_is_rendered = True
                    while page_is_rendered:
                        elements = self.scraper.find_elements_on_page(
                            self.scraper.download_conf.get('xpath_path_pages')
                            .replace("{page}", f'{page}'),
                            single_element=False, sleep=1
                        )
                        if len(elements) >= 2:
                            urls = [x.get_attribute('src') for x in elements[:2]]
                            for url_page_elem in urls:
                                page_resp = self.scraper.make_request(url_page_elem, return_content=False)
                                if page_resp:  # page_resp.ok
                                    extension = '_fg.png' if 'fg' in url_page_elem else '_bg.jpeg'
                                    with open(os.path.join(TEMP_DIR_IMAGES, f'page{page:03d}{extension}'), 'wb') as img:
                                        img.write(page_resp.content)
                        else:
                            page_is_rendered = False
                        page = page + 1 if page_is_rendered else page
                        if page == total_pages + 1:
                            page_is_rendered = False
                    has_next = False if page >= total_pages else True
                status = build_pages(total_pages, os.path.join(TEMP_DIR_IMAGES, 'mounted_images'))
                if status:
                    create_pdf_from_images(os.path.join(TEMP_DIR_IMAGES, 'mounted_images'), output_file_path)
                    remove_jpg_files(os.path.join(TEMP_DIR_IMAGES))
                    remove_jpg_files(os.path.join(TEMP_DIR_IMAGES, 'mounted_images'))
                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path, rename_raw_file=False)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(os.path.dirname(output_file_path), f"{self.scraper.dirname}/{source.dirname}", filename)
                    delete_files_in_folder(os.path.dirname(output_file_path))
                    downloaded = DownloadStatus.SUCCESS
            else:
                downloaded = "unavailable"
        finally:
            return downloaded

    def calameo(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        try:
            filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
            output_file_path = os.path.join(TEMP_DIR_FILES, filename)

            html_tree = self.scraper.extract_html_from_page(
                url=self.scraper.login_info.get('hemeroteca_url'),
                scope='webdriver',
                cookies=self.scraper.login_info.get('cookies')
            )
            time.sleep(3)

            tmp_edition = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url_edition'))
            edition = html_tree.xpath(tmp_edition)
            if edition:
                id = edition[0].get('href').split('/')[-1]
                url = f'https://www.calameo.com/read/{id}'
                html_edition = self.scraper.extract_html_from_page(url=url)
                page = html_edition.xpath(self.scraper.download_conf.get('page_url_format'))[0]
                if page:
                    limit = False
                    c = 1
                    while not limit:
                        print(f"Retrieving page {c}")
                        page_resp = self.scraper.make_request(page.replace("p1.jpg", f"p{c}.jpg"),
                                                              return_content=False)
                        if page_resp:  # page_resp.ok
                            with open(os.path.join(TEMP_DIR_IMAGES, f'page_{c:03d}.jpg'), 'wb') as img:
                                img.write(page_resp.content)
                        else:  # elif page_resp.status_code == 403
                            limit = True
                        c += 1
                    create_pdf_from_images(
                        images_directory=TEMP_DIR_IMAGES,
                        pdf_path=output_file_path)
                    remove_jpg_files(TEMP_DIR_IMAGES)

                    # downloaded_files.append(check_if_file_exists(output_file_path))
                    # downloaded = all(downloaded_files)
                    is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path, rename_raw_file=False)
                    is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                    if is_downloaded and is_pdf_valid:
                        ftp.upload_file(os.path.dirname(output_file_path), f"{self.scraper.dirname}/{source.dirname}",
                                        filename)
                        delete_files_in_folder(os.path.dirname(output_file_path))
                        downloaded = "downloaded"
            else:
                downloaded = "unavailable"
        finally:
            return downloaded

    def lamanana(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
        output_file_path = os.path.join(TEMP_DIR_FILES, filename)

        self.set_download_path(output_file_path)
        try:
            url = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url_pdf'))
            edition = self.scraper.find_elements_on_page(url)
            if edition:
                edition.click()
                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(
                        local_file_path=os.path.dirname(output_file_path),
                        ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                        filename=filename
                    )
                    time.sleep(3)
                    delete_files_in_folder(os.path.dirname(output_file_path))
                    downloaded = DownloadStatus.SUCCESS
            else:
                downloaded = "unavailable"
        except Exception as e:
            logger.error("\tError trying to get pdf file,"
                         f" \n Error: {e}")
        finally:
            return downloaded

    def diariandorra(self, source, ftp):

        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
        output_file_path = os.path.join(TEMP_DIR_FILES, filename)
        downloaded = DownloadStatus.FAILED

        try:
            c = 1
            page = self.scraper.download_conf.get('url')
            edition = self.scraper.make_request(page.replace('{page}', f'{c}'), return_content=False)
            if edition:
                limit = False
                page = self.scraper.download_conf.get('url')
                while not limit:
                    print(f"Retrieving page {c}")
                    if c != 1:
                        page_resp = self.scraper.make_request(page.replace("{page}", f"{c}"),
                                                              return_content=False)
                    else:
                        page_resp = edition
                    if page_resp:  # page_resp.ok
                        with open(os.path.join(TEMP_DIR_IMAGES, f'page_{c:03d}.jpg'), 'wb') as img:
                            img.write(page_resp.content)
                    else:  # elif page_resp.status_code == 403
                        limit = True
                    c += 1
                    time.sleep(random.randint(0, 3))
                create_pdf_from_images(
                    images_directory=TEMP_DIR_IMAGES,
                    pdf_path=output_file_path)
                remove_jpg_files(TEMP_DIR_IMAGES)

                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path, rename_raw_file=False)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(os.path.dirname(output_file_path), f"{self.scraper.dirname}/{source.dirname}",
                                    filename)
                    delete_files_in_folder(os.path.dirname(output_file_path))
                    downloaded = "downloaded"
            else:
                downloaded = "unavailable"

        except Exception as e:
            logger.error("\tError trying to get pdf file,"
                         f" \n Error: {e}")
        finally:
            return downloaded
            # //p[contains(text(),'18-04-24')]/ancestor::article//a/@href
            # self.scraper.find_elements_on_page()
            # url = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url_pdf'))
            # edition = self.scraper.find_elements_on_page(url)
            # if edition:
            #     edition.click()
            #     is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
            #     is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
            #     if is_downloaded and is_pdf_valid:
            #         ftp.upload_file(
            #             local_file_path=os.path.dirname(output_file_path),
            #             ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
            #             filename=filename
            #         )
            #         time.sleep(3)
            #         delete_files_in_folder(os.path.dirname(output_file_path))
            #         downloaded = DownloadStatus.SUCCESS


    def elpuerto(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        try:
            filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
            output_file_path = os.path.join(TEMP_DIR_FILES, filename)
            self.set_download_path(output_file_path)

            url_edition = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url_edition'))
            self.scraper.navigate_to_page(url_edition)
            session_expired = self.scraper.search_element_by_path(
                xpath='//*[contains(text(),"¡Se ha superado el número de sesiones permitidas!")]')
            if session_expired is None:
                # xpath = "(//div[contains(@class,'icon-file-pdf') and @title='Descarga en pdf'])[1]"
                xpath = self.scraper.download_conf.get('download_pdf_button')

                xpath_error_html = self.scraper.download_conf.get('not_available_edition')
                xpath_complete_edition = self.scraper.download_conf.get('pdf_full_pages_edition')
                not_available = self.scraper.search_element_by_path(xpath_error_html)
                if not_available is not None:
                    downloaded = DownloadStatus.UNAVAILABLE
                else:
                    self.scraper.search_element_by_path(xpath, clicable=True)
                    self.scraper.search_element_by_path(xpath_complete_edition, clicable=True)
                    is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                    is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                    if is_downloaded and is_pdf_valid:
                        downloaded = DownloadStatus.SUCCESS
                        ftp.upload_file(
                            local_file_path=os.path.dirname(output_file_path),
                            ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                            filename=filename
                        )
                        delete_files_in_folder(os.path.dirname(output_file_path))

        except:
            downloaded = DownloadStatus.FAILED
        finally:
            return downloaded

    def nueva_alcarria(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        try:
            basic_auth = HTTPBasicAuth(self.scraper.login_info.get('user'), self.scraper.login_info.get('password'))
            month = '{MONTHNAME}'
            monthname = parse_date_fmt_to_current_date(month).upper()
            edition_page = parse_date_fmt_to_current_date(self.scraper.download_conf.get('today_edition'))
            edition_page_1 = edition_page.replace('{MNAME}', monthname)
            edition_page_2 = re.sub(r'/(\d{2}-\d{2}-\d{4})/',
                                    lambda match: '/' + match.group(1).replace('-', '_') + '/', edition_page_1)
            urls = [edition_page_1, edition_page_2]
            # self.scraper.navigate_to_page(edition_page)
            urls_library_found = [self.scraper.extract_html_from_page(edition_page, auth=basic_auth) for edition_page in
                                  urls]
            library_html = next((url for url in urls_library_found if url is not None), None)
            if library_html is not None:
                edition = self.scraper.get_xpath_field_from_html(library_html,
                                                                 self.scraper.download_conf.get('url_pdf'))
                if edition:
                    url_pdf = edition_page + edition
                    downloaded = self.download_pdf_content(
                        source=source,
                        url=url_pdf,
                        filename=f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf',
                        ftp=ftp,
                        edition=True
                    )
                else:
                    downloaded = "unavailable"
            else:
                logger.warning("\tNo today's root folder available")
                downloaded = "unavailable"
        except:
            downloaded = DownloadStatus.FAILED
        finally:
            return downloaded

    def eltemps(self, source, ftp):
        downloaded = DownloadStatus.FAILED
        try:
            editions_page = parse_date_fmt_to_current_date(self.scraper.download_conf.get('month_editions'))
            edition_code_xpath = parse_date_fmt_to_current_date(self.scraper.download_conf.get('edition'))
            library_html = self.scraper.extract_html_from_page(editions_page)
            if library_html is not None:
                edition = self.scraper.get_xpath_field_from_html(library_html, edition_code_xpath)
                if edition:
                    edition_clean = re.search('\d+', edition).group(0)
                    url_pdf = self.scraper.download_conf.get('url_pdf').replace('{edition}', edition_clean)
                    downloaded = self.download_pdf_content(
                        source=source,
                        url=url_pdf,
                        ftp=ftp,
                        edition=True
                    )
                else:
                    downloaded = "unavailable"
            else:
                logger.warning("\tImpossible to get html")
        except:
            downloaded = DownloadStatus.FAILED
        finally:
            return downloaded

    def lavanguardia(self, source, ftp):
        downloaded = self.multipage_request(source, ftp, leading_zero=2)
        return downloaded

    def multipage_request(self, source, ftp, leading_zero=0):
        count_downloaded = []
        page = 0
        next = DownloadStatus.SUCCESS
        try:
            while next == DownloadStatus.SUCCESS:
                page += 1
                url = parse_date_fmt_to_current_date(
                    (self.scraper.download_conf.get('url')
                     ).replace('{edition}', source.edition
                               ).replace('{P}', f'{str(page).zfill(2)}')
                )

                # output_file_name = os.path.join(DOWNLOAD_DIR, self.scraper.name, source.name,
                #                 f'{source.name}_{dt.datetime.now():%d%m%Y}_{str(page).zfill(leading_zero)}.pdf')
                filename = f'{source.name}_{dt.datetime.now():%d%m%Y}_{str(page).zfill(leading_zero)}.pdf'
                next = self.download_pdf_content(
                    source,
                    ftp=ftp,
                    url=url,
                    filename=filename
                )
                if next == DownloadStatus.SUCCESS:
                    count_downloaded.append(next)
                time.sleep(2)
            if len(count_downloaded) == 0:
                downloaded = DownloadStatus.UNAVAILABLE
            else:
                downloaded = DownloadStatus.SUCCESS
        except:
            downloaded = DownloadStatus.FAILED
        finally:
            return downloaded

    def gentedigital(self, source, ftp):
        """
        1 - extraemos html de la página de ediciones
        2 - buscamos código asociado al periódico del día de hoy
        3-  si existe el código, reemplazamos en la url y descargamos, si no devolvemos falso.
        """

        def is_date_between_daterange(str_date, fecha_actual):
            # Buscar las fechas en el formato DD/MM/YYYY
            cleaned_string = re.sub(r'\s+', ' ', re.sub(r'[^0-9/ ]', '', str_date)).strip()
            # match = re.search(r'(\d{2}/\d{2}/\d{4})\s*al\s*(\d{2}/\d{2}/\d{4})', cadena_rango)
            # Extraer las fechas encontradas
            fecha_inicio_str, fecha_fin_str = cleaned_string.split(' ')
            # Convertir las fechas a objetos datetime
            fecha_inicio = dt.datetime.strptime(fecha_inicio_str, '%d/%m/%Y')
            fecha_fin = dt.datetime.strptime(fecha_fin_str, '%d/%m/%Y')
            # Obtener la fecha actual
            # fecha_actual = dt.datetime.now()
            # Verificar si la fecha actual está dentro del rango
            check_date = fecha_inicio <= fecha_actual <= fecha_fin
            return check_date, fecha_inicio_str

        downloaded = DownloadStatus.FAILED
        try:
            editions_html_tree = self.scraper.extract_html_from_page(source.edition)
            if editions_html_tree is not None:
                xpath = "//ul[@class='lista_portadas clearfix']//li/p[substring-before(text(),'al')]/text()"
                all_html_dates = self.scraper.get_xpath_field_from_html(editions_html_tree, xpath, single_value=False)

                editions = [tuple for tuple in
                            (is_date_between_daterange(str_date=str_found_date, fecha_actual=dt.datetime.now()) \
                             for str_found_date in all_html_dates) if tuple and tuple[0]]
                if editions:
                    for edition in editions:
                        xpath_edition_code = self.scraper.download_conf.get('unformatted_xpath').replace('{fecha_inicio}',
                                                                                                         edition[1])
                        edition_code = self.scraper.get_xpath_field_from_html(editions_html_tree, xpath_edition_code)
                        if edition_code:
                            code_url = edition_code.split("/")[-2]
                            url_with_code_replaced = self.scraper.download_conf.get('url_pdf').replace("{code_url}",
                                                                                                       code_url)
                            url_pdf = parse_date_fmt_to_current_date(url_with_code_replaced)
                            downloaded = self.download_pdf_content(
                                source=source,
                                url=url_pdf,
                                ftp=ftp,
                                edition=True
                            )
                else:
                    downloaded = "unavailable"
            else:
                logger.warning("\tImpossible to get html")
        finally:
            return downloaded

    def diarisabadell(self, source, ftp):
        """
        1 - obtenermos el html de la hemeroteca
        2 - formateamos fecha con el formato requerido por el medio( traduccion, conversión...)
        3 - creamos el xpath con la fecha formateada
        4 - buscamos token mediante xpath y html
        5 - parseamos fecha en la url y generamos la url del pdf
        6 - descargamos url pdf

        """
        downloaded = DownloadStatus.FAILED
        try:
            filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
            output_file_path = os.path.join(TEMP_DIR_FILES, filename)
            self.scraper.create_webdriver_session()
            self.set_download_path(output_file_path)
            self.scraper.navigate(self.scraper.login_info.get('hemeroteca_url'))
            # delete_file_extension(os.path.dirname(output_file_path), '.crdownload')

            # library_html = self.scraper.extract_html_from_page(self.scraper.login_info.get('hemeroteca_url'))
            # if library_html is not None:
            formatted_date = self.scraper.translate_date_to_source_lang(
                self.scraper,
                convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt')).capitalize()
            )
            xpath_edition_current_date = self.scraper.download_conf.get('url').replace('{format_date}', formatted_date).replace("'", "’")
            # raw_edition_url = self.scraper.get_xpath_field_from_html(library_html, xpath_edition_current_date)
            raw_edition_url = self.scraper.find_elements_on_page(
                xpath_edition_current_date).get_attribute('href')

            if raw_edition_url:
                url_obj = urlparse(raw_edition_url)
                edition_url = f'{url_obj.scheme}://{url_obj.netloc}/file/download{url_obj.path}' + '/{DD}{MM}{YYYY}-pdf.pdf'
                url_pdf = parse_date_fmt_to_current_date(edition_url)
                self.scraper.navigate(url_pdf)

                edition = self.scraper.find_elements_on_page(self.scraper.download_conf.get('url_pdf'))

                if edition:
                    edition.click()
                    is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                    is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                    if is_downloaded and is_pdf_valid:
                        ftp.upload_file(
                            local_file_path=os.path.dirname(output_file_path),
                            ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                            filename=filename
                        )
                        time.sleep(2)
                        delete_files_in_folder(os.path.dirname(output_file_path))
                        downloaded = DownloadStatus.SUCCESS

                    else:
                        logger.warning("\tImpossible to get html")
            else:
                downloaded = "unavailable"

        finally:
            return downloaded

    def argia(self, source, ftp):
        """
        1. Extraer html --> HEMEROTECA (contiene diferentes ediciones)
        2. Buscar url edición de hoy
            - Si no existe --> No hay pdf de hoy
        3. Ir a la url encontrada
        4. Buscar url de descarga pdf + descargar


        downloaded = False
        # Extraer hemeroteca html
        library_html = make_request()
        # Buscar edicion
        edition_xpath = self.scraper.download_conf.get(..., self.scraper.convert__date_to_source_fmt())
        edtion_url = self.scraper.find_in_html_by_xpath(library_html, edition_xpath)
        if not edtion_url:
            return downloaded
        # Extraer url de descarga desde la pagina de la edicion
        edition_html = self.scraper.make_request(url=edtion_url)
        pdf_url = self.scraper.find_in_html_by_xpath(library_html, pdf_url_xpath)
        # Descargar
        if not pdf_url:
            return downloaded
        downloaded = self.scraper.download_pdf_file(pdf_url, source)
        return downloaded

        """
        downloaded = DownloadStatus.FAILED
        try:
            library_html_tree = self.scraper.extract_html_from_page(self.scraper.login_info.get('hemeroteca_url'))
            if library_html_tree:
                formatted_date_raw = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
                formatted_date = self.scraper.translate_date_to_source_lang(
                    self.scraper,
                    formatted_date_raw
                )
                edition_xpath = self.scraper.download_conf.get('unformatted_xpath').replace('{format_date}', formatted_date)
                edition_url = self.scraper.get_xpath_field_from_html(library_html_tree, edition_xpath)
                if edition_url:
                    edition_html_tree = self.scraper.extract_html_from_page(edition_url)
                    pdf_url_xpath = self.scraper.download_conf.get('url')
                    url_pdf = self.scraper.get_xpath_field_from_html(edition_html_tree, pdf_url_xpath)
                    downloaded = self.download_pdf_content(
                        source=source,
                        url=url_pdf,
                        ftp=ftp,
                        edition=True
                    )
                else:
                    downloaded = "unavailable"
            else:
                logger.warning("\tImpossible to get html")
        finally:
            return downloaded

    def tresdevuit(self, source, ftp):
        """
        1 - extraemos html de la página de la hemeroteca
        2 - formateamos fecha ( traducción idioma y formato de fecha)
        3 - sustituimos fecha en xpath_de la edición
        4 - buscamos xpath en el html de la hemeroteca para obtener la url del pdf
        5 - si hay url descargamos.
        """
        downloaded = DownloadStatus.FAILED
        try:
            filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
            output_file_path = os.path.join(TEMP_DIR_FILES, filename)
            self.set_download_path(output_file_path)
            # library_html_tree = self.scraper.extract_html_from_page(self.scraper.login_info.get('hemeroteca_url'))
            # if library_html_tree:
            formatted_date_raw = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
            formatted_date = self.scraper.translate_date_to_source_lang(self.scraper, formatted_date_raw)
            xpath_today_edition = self.scraper.download_conf.get('url').replace('{format_date}', formatted_date)
            # url_pdf = self.scraper.get_xpath_field_from_html(library_html_tree, xpath_today_edition)
            edition = self.scraper.find_elements_on_page(xpath_today_edition)
            if edition:
                edition.click()
                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(
                        local_file_path=os.path.dirname(output_file_path),
                        ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                        filename=filename
                    )
                    downloaded = DownloadStatus.SUCCESS
                    time.sleep(2)
                    delete_files_in_folder(os.path.dirname(output_file_path))
            else:
                downloaded = "unavailable"
        finally:
            return downloaded

    def elcultural(self, source, ftp):
        """
        1 - extraemos html de la hemeroteca
        2 - reemplazamos la fecha en el xpath de la edición
        3 - buscamos xpath en el html -> obtener url pdf de hoy
        4 - descargamos
        """
        downloaded = DownloadStatus.FAILED
        try:
            library_html_tree = self.scraper.extract_html_from_page(self.scraper.login_info.get('hemeroteca_url'))
            if library_html_tree is not None:
                xpath_today_edition = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url'))
                edition_url = self.scraper.get_xpath_field_from_html(library_html_tree, xpath_today_edition)
                if edition_url:
                    downloaded = self.download_pdf_content(
                        source=source,
                        url=edition_url,
                        ftp=ftp,
                        edition=True
                    )
                else:
                    downloaded = DownloadStatus.UNAVAILABLE
            else:
                logger.warning("\tImpossible to get html")
        finally:
            return downloaded

    def lacomarca(self, source, ftp):
        """
        1 - extraemos html de la hemeroteca
        2 - reemplazamos la fecha en el xpath de la edición
        3 - buscamos xpath en el html -> obtener url pdf de hoy
        4 - descargamos
        """
        downloaded = DownloadStatus.FAILED
        try:
            library_html_tree = self.scraper.extract_html_from_page(self.scraper.login_info.get('hemeroteca_url'),
                                                                    scope='webdriver')
            if library_html_tree:
                xpath_today_edition = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url'))
                edition_url = self.scraper.get_xpath_field_from_html(library_html_tree, xpath_today_edition)
                if edition_url:
                    downloaded = self.download_pdf_content(
                        source=source,
                        url=edition_url,
                        ftp=ftp,
                        edition=True
                    )
                else:
                    downloaded = DownloadStatus.UNAVAILABLE
            else:
                logger.warning("\tImpossible to get html")
        finally:
            return downloaded

    def vidaeconomica(self, source, ftp):
        """
        1 - Extraemos html de la hemeroteca
        2 - formateamos fecha y generamos xpath para la edición de hoy
        3 - buscamos el código de la edición para el dirario de hoy
        4 - Si existe edición descargamos

        """

        def change_to_new_tab():
            original_window = self.scraper.chrome.current_window_handle
            for window_handle in self.scraper.chrome.window_handles:
                if window_handle != original_window:
                    self.scraper.chrome.switch_to.window(window_handle)

        downloaded = DownloadStatus.FAILED
        try:
            self.scraper.navigate(self.scraper.login_info.get('hemeroteca_url'))
            formatted_date = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
            xpath_today_edition = self.scraper.download_conf.get('url').replace('{format_date}', formatted_date)
            edition = self.scraper.find_elements_on_page(xpath_today_edition)
            if edition:
                edition.click()
                change_to_new_tab()
                edition_url = self.scraper.chrome.current_url
                downloaded = self.download_pdf_content(
                    source=source,
                    url=edition_url,
                    ftp=ftp,
                    edition=True
                )
            else:
                downloaded = DownloadStatus.UNAVAILABLE
        finally:
            return downloaded

    def eleconomista(self, source, ftp):
        """
        1 - Generamos fecha actual customizada con el formato fecha del medio
        2 - Parseamos fecha a la url del medio
        3 - Sutituimos la fecha en el xpath
        4 - Recogemos html de la url principal del medio
        4 - Buscarmos en el html el xpath generado anteriormente
        5 - Si hay elementos
            - Generamos la url de la url donde se encuentra el link del archivo pdf
            - Recogemos html de la url anterior
            - Devolvemos xpath y respuesta
        """
        downloaded = DownloadStatus.FAILED
        try:
            edition_url_parsed_date = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url'))
            edition_url = edition_url_parsed_date.replace('{edition}', source.edition)
            formatted_date = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
            xpath_date_to_check = self.scraper.download_conf.get('unformatted_xpath').replace('{format_date}',
                                                                                              formatted_date)
            edition_html_tree = self.scraper.extract_html_from_page(edition_url)
            if edition_html_tree is not None:
                edition = self.scraper.get_xpath_field_from_html(edition_html_tree, xpath_date_to_check)
                if edition is not None:
                    url_download_button = self.scraper.get_xpath_field_from_html(edition_html_tree,
                                                                                 self.scraper.download_conf.get('url_pdf'))
                    download_button_tree_response = self.scraper.extract_html_from_page(url_download_button)
                    xpath_url_pdf_link = "//*[contains(@src,'pdf')]/@src"
                    url_pdf = self.scraper.get_xpath_field_from_html(download_button_tree_response, xpath_url_pdf_link)
                    if url_pdf is not None:
                        downloaded = self.download_pdf_content(
                            source=source,
                            url=url_pdf,
                            ftp=ftp,
                            edition=True
                        )
                else:
                    downloaded = DownloadStatus.UNAVAILABLE
            else:
                logger.warning("\tImpossible to get html")
        finally:
            return downloaded

    def gacetamedica_elglobal(self, source, ftp):
        """
        1 - extraemos html de la hemeroteca
        2 - generamos fecha actual customizada con el formato fecha del medio
        3 - formateamos xpath con la fecha que hemos generado anteriormente
        4 - buscarmos la url de la edición con el xpath y el html previo
        5 - buscamos el código de la edición en la url de la edición
        6 - generamos la url del pdf y descargamos
        """
        downloaded = DownloadStatus.FAILED
        try:
            library_html_tree = self.scraper.extract_html_from_page(self.scraper.login_info.get('hemeroteca_url'))
            if library_html_tree is not None:
                formatted_date = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
                xpath_current_date = self.scraper.download_conf.get('unformatted_xpath').replace('{format_date}',
                                                                                                 formatted_date)
                edition_url = self.scraper.get_xpath_field_from_html(library_html_tree, xpath_current_date)
                if edition_url:
                    edition_code = re.search('\d+', edition_url).group(0)
                    url_pdf = parse_date_fmt_to_current_date(self.scraper.download_conf.get('url_pdf')).replace(
                        '{edicion}', edition_code)
                    downloaded = self.download_pdf_content(
                        source=source,
                        url=url_pdf,
                        ftp=ftp,
                        edition=True
                    )
                else:
                    downloaded = DownloadStatus.UNAVAILABLE
            else:
                logger.warning("\tImpossible to get html")
        finally:
            return downloaded

    def lavozdegalicia(self, source, ftp):
        """
        1 - parseamos fecha actual a xpath de la edición en cuestión
        2 - navegamos a la hemeroteca
        3 - realizamos las acciones de click personalizasas para este medio, para descargar pdf

        """

        downloaded = DownloadStatus.FAILED
        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
        output_file_path = os.path.join(TEMP_DIR_FILES, filename)

        self.set_download_path(output_file_path)
        # delete_file_extension(os.path.dirname(output_file_path), '.crdownload')
        try:
            xpath_current_date_edition = (
                parse_date_fmt_to_current_date(self.scraper.download_conf.get('click_edicion'))
            ).replace('{edition}', source.edition)
            if self.scraper.chrome.current_url != self.scraper.login_info.get('hemeroteca_url'):
                time.sleep(1)
                self.scraper.navigate_to_page(
                    self.scraper.login_info.get('hemeroteca_url'),
                    validation=self.scraper.download_conf.get('selected_source')
                )
            time.sleep(1)
            self.scraper.search_element_by_path(self.scraper.download_conf.get('selected_source')).click()
            time.sleep(1)
            edition_element = self.scraper.search_element_by_path(xpath_current_date_edition)
            if edition_element:
                edition_element.click()
                time.sleep(2)
                self.scraper.search_element_by_path(self.scraper.download_conf.get('url_pdf')).click()
                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(
                        local_file_path=os.path.dirname(output_file_path),
                        ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                        filename=filename
                    )
                    downloaded = DownloadStatus.SUCCESS
                    time.sleep(2)
                    delete_files_in_folder(os.path.dirname(output_file_path))
            else:
                downloaded = DownloadStatus.UNAVAILABLE
        except Exception as e:
            logger.error("\tError trying to get pdf file"
                         f"\n Error: {e}")
        finally:
            return downloaded

    def sudouest(self, source, ftp):

        """
        1 - formateamos fecha y traducimos al idioma de la source
        2 - sustituimos la fecha en xpath de la url de la edición
        3 - navegamos a la source dependiendo del nombre: sudouest / sudouest version femenina
        4 - buscamos si existe el xpath previo con la fecha de hoy
        5 - si existe, clicamos en le edición
        6 - clicamos download
        """

        downloaded = DownloadStatus.FAILED

        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
        output_file_path = os.path.join(TEMP_DIR_FILES, filename)

        self.set_download_path(output_file_path)
        # delete_file_extension(os.path.dirname(output_file_path), '.crdownload')
        try:
            custom_date = convert_date_to_source_fmt(self.scraper.download_conf.get('date_fmt'))
            custom_date = self.scraper.translate_date_to_source_lang(custom_date)
            xpath_current_date = source.date.replace('{format_date}', custom_date)
            self.scraper.navigate_to_page(
                self.scraper.download_conf.get('url'),
                validation=self.scraper.download_conf.get('url_pdf')
            )
            # self.scraper.navigate(self.scraper.login_info.get('hemeroteca_magazine'),
            #                                  validation=self.scraper.download_conf.get('url_pdf'))
            edition = self.scraper.search_element_by_path(xpath_current_date)
            if edition:
                edition.click()
                self.scraper.search_element_by_path(self.scraper.download_conf.get('url_pdf')).click()
                # confirm_down = self.scraper.find_elements_on_page(self.scraper.download_conf.get('confirm_download'),
                #                                                   single_element=True)
                # if confirm_down:
                #     confirm_down.click()
                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(
                        local_file_path=os.path.dirname(output_file_path),
                        ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                        filename=filename
                    )
                    downloaded = DownloadStatus.SUCCESS
                    time.sleep(2)
                    delete_files_in_folder(os.path.dirname(output_file_path))
            else:
                downloaded = DownloadStatus.UNAVAILABLE
                return False

        except Exception as e:
            logger.error("\tError trying to get pdf file"
                         f"\n Error: {e}")
        finally:
            return downloaded

    def barrons_wallstreet(self, source, ftp):
        """
        1 - navegamos a al hemeroteca
        2 - cambiamos al main iframe para poder hacer click
        3 - clicamos boton ediciones y cambiamos al iframe ediciones
        4 - generamos xpath para buscar la edición parseando la fecha de hoy
        5 - buscamos el elemento xpath edicion con la fecha correcta en el html para hacer click.
        6 - si hay elemento edición ->
            cambiamos al main iframe de nuevo
            buscamos el botón de páginas mediante xpath en el html
            clicamos descargar la versión completa
        7 -  si no hay elemento edición
            no descargamos

        """
        downloaded = DownloadStatus.FAILED

        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
        output_file_path = os.path.join(TEMP_DIR_FILES, filename)

        self.set_download_path(output_file_path)
        # delete_file_extension(os.path.dirname(output_file_path), '.crdownload')
        try:
            if self.scraper.chrome.current_url != self.scraper.login_info.get('hemeroteca_url'):
                self.scraper.navigate_to_page(
                    self.scraper.login_info.get('hemeroteca_url'))
            time.sleep(5)
            if self.scraper.name == "Barron's":
                self.scraper.move_to_iframe(self.scraper.download_conf.get('main_iframes'))
                self.scraper.search_element_by_path(self.scraper.download_conf.get('edition_button')).click()
                self.scraper.move_to_iframe(self.scraper.download_conf.get('iframe_edition'))
            else:
                self.scraper.move_to_iframe(self.scraper.download_conf.get('first_iframe'))
                self.scraper.move_to_iframe(self.scraper.download_conf.get('main_iframes'))
                self.scraper.move_to_iframe(self.scraper.download_conf.get('main_iframes'))
                self.scraper.search_element_by_path(self.scraper.download_conf.get('edition_button')).click()
                self.scraper.move_to_iframe(self.scraper.download_conf.get('lightbox_iframe'))

            xpath_current_edition = parse_date_fmt_to_current_date(self.scraper.download_conf.get('edition'))
            edition = self.scraper.search_element_by_path(xpath_current_edition)
            if edition:
                edition.click()
                if self.scraper.name != 'The Wall Street Journal':
                    self.scraper.move_to_iframe(self.scraper.download_conf.get('main_iframes'),
                                                back_to_default_content=True)
                else:
                    self.scraper.move_to_iframe(self.scraper.download_conf.get('first_iframe'),
                                                back_to_default_content=True)
                    self.scraper.move_to_iframe(self.scraper.download_conf.get('main_iframes'))
                    self.scraper.move_to_iframe(self.scraper.download_conf.get('main_iframes'))

                self.scraper.search_element_by_path(self.scraper.download_conf.get('pages')).click()
                self.scraper.search_element_by_path(
                    self.scraper.download_conf.get('complete_edition_pdf')).click()

                is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                if is_downloaded and is_pdf_valid:
                    ftp.upload_file(
                        local_file_path=os.path.dirname(output_file_path),
                        ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                        filename=filename)
                    downloaded = DownloadStatus.SUCCESS
                    time.sleep(3)
                    delete_files_in_folder(os.path.dirname(output_file_path))
            else:
                downloaded = DownloadStatus.UNAVAILABLE
        except Exception as e:
            logger.error("\tError trying to get pdf file,"
                         f" \n Error: {e}")
        finally:
            return downloaded

    def scroll_get_pages_laprensa(self, side_menu, total_scroll_height):
        status = -1
        try:
            actions = ActionChains(self.scraper.chrome)
            actions \
                .move_to_element(side_menu) \
                .click_and_hold() \
                .move_by_offset(0, -total_scroll_height) \
                .release() \
                .perform()
            time.sleep(2)
            status = 0
        except:
            pass
        finally:
            return status

    def get_laprensa_pages(self):
        try:
            scrollbar_xpath = "//div[@id='scroller']/*[contains(@class,'iScrollVerticalScrollbar')]"
            side_menu = self.scraper.search_element_by_path(scrollbar_xpath)
            scroll_style = self.scraper.search_element_by_path(
                "//div[@id='scroller']//*[contains(@class,'iScrollIndicator')]"
            ).get_attribute('style')
            # scroll_style = self.scraper.chrome \
            #     .find_element(By.XPATH, "//div[@id='scroller']//*[contains(@class,'iScrollIndicator')]") \
            #     .get_attribute('style')
            total_scroll_height = int(re.search(r'height: (.*)px;', scroll_style).group(1)) - 1
            status = self.scroll_get_pages_laprensa(side_menu, total_scroll_height)
            if status == 0:
                elements = self.scraper.chrome.find_elements(By.XPATH, "//div[@data-page-container='true']")
                return len(elements)
            else:
                return status
        except:
            return -1

    def laprensa(self, source, ftp):
        """
        1 - navegamos a la hemeroteca
        2 - parseamos fecha en la url de la edición que queremos para el dia de hoy
        3 - navegamos a la url de la edicion para el dia de hoy
        4 - si nos da error --> no hay noticias para hoy
            si no da error:
                - check si estamos logueados
                - si no relogueamos
                - buscamos unicode con el xpath en el html
                - buscamos el total de páginas
                - montamos la url con el código uni, y dejamos la página como variable para iterar.
                - descargamos pagina a página
        """
        downloaded = DownloadStatus.FAILED
        status_downloads = []
        reached_edition_url = False
        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}.pdf'
        output_file_path = os.path.join(TEMP_DIR_FILES, filename)
        self.set_download_path(output_file_path)

        try:
            action = ActionChains(self.scraper.chrome)
            if self.scraper.chrome.current_url != self.scraper.login_info.get('hemeroteca_url'):
                self.scraper.navigate_to_page(
                    self.scraper.login_info.get('hemeroteca_url'),
                    validation=self.scraper.download_conf.get('validation_editions_page')
                )
            # self.scraper.navigate(self.scraper.login_info.get('hemeroteca_url'),
            #                       validation=self.scraper.download_conf.get('validation_editions_page'))
            url_today_edition = (parse_date_fmt_to_current_date(
                self.scraper.download_conf.get('url_pdf_edition')
            )
            ).replace('{edition}', source.edition)

            self.scraper.navigate_to_page(url_today_edition)
            if self.scraper.chrome.current_url == url_today_edition and self.scraper.chrome.title != '404 Not Found':
                reached_edition_url = self.scraper.page_validation(
                    validation_xpath=self.scraper.download_conf.get('pre_download_button'))
            elif self.scraper.chrome.title == '404 Not Found':
                downloaded = DownloadStatus.UNAVAILABLE
            else:
                time.sleep(3)
                web_login = self.scraper.find_elements_on_page(
                    self.scraper.download_conf.get('check_if_loged_in'),
                    single_element=False
                )
                if web_login:
                    self.scraper.login(login_type=self.scraper.login_info.get('type'), navigate=False)
                    if self.scraper.chrome.current_url == url_today_edition:
                        reached_edition_url = True
                    else:
                        session_up = self.scraper.page_validation(
                        validation_xpath=self.scraper.login_info.get('login_validation'))
                        if session_up:
                            self.scraper.navigate_to_page(url_today_edition, validation=self.scraper.download_conf.get('pre_download_button'))
                            if self.scraper.chrome.current_url == url_today_edition:
                                reached_edition_url = True

            if reached_edition_url:
                total_pages = self.scraper.search_element_by_path(
                    self.scraper.download_conf.get('total_pages')).text

                count_pages = 0
                still_pages = True
                while still_pages:
                    # click to open popup to download each page.
                    time.sleep(2)
                    download_button = self.scraper.search_element_by_path(
                        self.scraper.download_conf.get('download_button'))
                    # action = ActionChains(self.scraper.chrome)

                    # perform the operation
                    action.move_to_element(download_button).click().perform()
                    # download_button.click()
                    # retrieve pages elems to iterate
                    pages = self.scraper.find_elements_on_page(
                        self.scraper.download_conf.get('pages_download_elems'), single_element=False, sleep=2)

                    for page in pages:
                        page.click()
                        filename = f'{source.name}_{dt.datetime.now():%d%m%Y}_{str(count_pages + 1).zfill(3)}.pdf'
                        output_file_path = os.path.join(TEMP_DIR_FILES, filename)
                        is_downloaded = check_if_file_downloaded_and_rename_file(output_file_path)
                        time.sleep(1)
                        is_pdf_valid = self.check_pdf_is_valid(file_path=output_file_path)
                        if is_downloaded and is_pdf_valid:
                            ftp.upload_file(
                                local_file_path=os.path.dirname(output_file_path),
                                ftp_file_path=f"{self.scraper.dirname}/{source.dirname}",
                                filename=filename
                            )
                            downloaded = DownloadStatus.SUCCESS
                        count_pages += 1
                    download_button.send_keys(Keys.ESCAPE)
                    if self.scraper.chrome.current_url.split('/')[-2] != total_pages:
                        action.send_keys(Keys.ARROW_RIGHT).perform()
                    else:
                        still_pages = False
                    time.sleep(1)
                    status_downloads.append(downloaded)
                downloaded = DownloadStatus.SUCCESS if all([status for status in status_downloads]) else DownloadStatus.FAILED
                delete_files_in_folder(os.path.dirname(output_file_path))

            else:
                downloaded = DownloadStatus.UNAVAILABLE
        except Exception as e:
            logger.error("\tError trying to get pdf file"
                         f" source: {source.name} \n Error: {e}")
        finally:
            return downloaded


class Scraper:

    def __init__(self, webdriver=False, headless=False):
        self.headless = headless
        self.chrome = self.create_webdriver_session() if webdriver else None
        self.remote_session = None
        self.logged_in = False

    def login(self, login_type, navigate=True):
        login = Login(self)
        login.perform_login(login_type, navigate)

    def download(self, download_type, source, ftp):
        download = Download(self)
        return download.perform_download(download_type, source, ftp)

    def is_webdriver_up(self, root_source_name):
        if not self.chrome:
            self.create_webdriver_session(root_source_name=root_source_name)

    def set_cookies_from_remote_session(self, url):
        if self.remote_session:
            if self.remote_session.cookies:
                self.chrome.get(url)
                dict_resp_cookies = self.remote_session.cookies.get_dict()
                response_cookies_browser = [{'name': name, 'value': value} for name, value in dict_resp_cookies.items()]
                [self.chrome.add_cookie(cookie) for cookie in response_cookies_browser]

    def create_session(self):
        self.remote_session = self.create_remote_session()

    def create_remote_session(self):
        session = requests.Session()
        if self.chrome:
            self.set_cookies_from_browser(session)
        else:
            session.headers.update({'User-Agent': Config.USER_AGENT})
        return session

    def close_webdriver_session(self):
        try:
            self.chrome.quit()
            self.webdriver = False
        except:
            pass

    def create_webdriver_session(self, root_source_name=None):
        service = Service()
        chrome_options = webdriver.ChromeOptions()
        # chrome_options = Options()
        # download_directory = {
        #     "download.default_directory": f'{os.path.join(DOWNLOAD_DIR, root_source_name)}'}
        # chrome_options.add_experimental_option(
        #     "prefs", {
        #         "download.default_directory": f'{os.path.join(DOWNLOAD_DIR, root_source_name)}',
        #         "plugins.always_open_pdf_externally": True
        #     }
        # )
        user_agent = Config.USER_AGENT
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.page_load_strategy = 'eager'
        chrome_options.add_argument(f'user-agent={user_agent}')
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("disable-infobars")
        chrome_options.add_argument("start-maximized")
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--ignore-ssl-errors')
        chrome_options.add_argument('--disable-extensions')
        chrome_options.add_experimental_option(
            "excludeSwitches",
            ["enable-automation", "enable-logging", "disable-popup-blocking"]
        )
        if self.headless:
            chrome_options.add_argument("--headless")

        # driver = webdriver.Chrome(
        #     executable_path=ChromeDriverManager().install(),
        #     options=chrome_options
        # )
        driver = webdriver.Chrome(service=service, options=chrome_options)
        self.chrome = driver

        # from selenium import webdriver
        # from selenium.webdriver.chrome.service import Service
        #
        # service = Service()
        # options = webdriver.ChromeOptions()
        # driver = webdriver.Chrome(service=service, options=options)
        # # ...
        # driver.quit()

    def set_cookies_from_browser(self, session):
        for cookie in self.chrome.get_cookies():
            session.cookies.set(cookie['name'], cookie['value'])

    def accept_notifications(self, cookies, sleep=10):
        if cookies:
            for cookie_xpath in cookies:
                if cookie_xpath is not None:
                    try:
                        WebDriverWait(self.chrome, sleep).until(
                            EC.element_to_be_clickable((By.XPATH, cookie_xpath))
                        ).click()
                    except:
                        pass

    def login_validation(self, validation_xpath):
        try:
            element_validation = EC.presence_of_element_located(
                (By.XPATH, validation_xpath))
            WebDriverWait(self.chrome, 30).until(element_validation)
            if element_validation:
                return True
            else:
                return False
        except Exception as e:
            print(e)
            return False

    def page_validation(self, validation_xpath):
        is_page_reached = False
        try:
            element_present = WebDriverWait(self.chrome, 20) \
                .until(EC.presence_of_element_located((By.XPATH, validation_xpath)))
            if element_present:
                is_page_reached = True
        except Exception as e:
            print(e)
        finally:
            return is_page_reached

    def navigate(self, url, cookies=None, validation=None, iframe_before_cookies=None):
        try:
            self.is_webdriver_up('default_folder')
            self.chrome.get(url)
            if iframe_before_cookies:
                self.move_to_iframe(iframe_before_cookies)
            if cookies:
                self.accept_notifications(cookies)
            time.sleep(3)
            if validation:
                page_reached = self.page_validation(validation)
                assert page_reached is True
        except Exception as e:
            logger.error(f'Failed navigate to url {url}. \n Error: {e}')

    def search_element_by_path(self, xpath, clicable=False, single_element=True):
        """
        :param self.driver:      selenium driver
        :param xpath:       custom xpath
        :param search_type: 1 to search by xpath
                            2 to search by CSS SELECTOR
                            3 to search by TAG_NAME
        :return:
        """
        element = None
        try:
            if clicable:
                element = WebDriverWait(self.chrome, 10).until(
                    EC.element_to_be_clickable((By.XPATH, xpath)))
                if element is not None:
                    element.click()
            else:
                if single_element:
                    element = WebDriverWait(self.chrome, 10).until(
                        EC.presence_of_element_located((By.XPATH, xpath)))
                else:
                    element = WebDriverWait(self.chrome, 10).until(
                        EC.presence_of_all_elements_located((By.XPATH, xpath)))

            # elif search_type == 2:
            #     element = WebDriverWait(self.chrome, 10).until(
            #         EC.element_to_be_clickable((By.CSS_SELECTOR, custom_search)))
            # elif search_type == 3:
            #     element = WebDriverWait(self.chrome, 10).until(
            #         EC.element_to_be_clickable((By.TAG_NAME, custom_search)))
        except WebDriverException:
            logger.error(f"Failed WebdriverWait for xpath {xpath}")
        finally:
            return element

    def find_elements_on_page(self, xpath, single_element=True, sleep=None):
        element = None
        try:
            if sleep:
                time.sleep(sleep)
            if single_element:
                element = self.chrome.find_element(By.XPATH, xpath)
            else:
                element = self.chrome.find_elements(By.XPATH, xpath)
        except NoSuchElementException:
            logger.error(f'Xpath not found. \n XPATH: {xpath}')
        except TimeoutException:
            # Maneja la excepción si se agota el tiempo de espera
            logger.error('Timeout while find xpath on html')
        except WebDriverException as e:
            # Maneja otras excepciones de WebDriver
            logger.error(f"Unexpected Webdriver Exception: {e}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
        finally:
            return element

    def move_to_iframe(self, iframe_xpath, back_to_default_content=False):
        """
        search_type : 1:    CSS SELECTOR
                      !=1:  XPATH
        """
        try:
            if iframe_xpath:
                if back_to_default_content:
                    self.chrome.switch_to.default_content()
                time.sleep(3)
                iframe = self.chrome.find_element(By.XPATH, iframe_xpath)
                self.chrome.switch_to.frame(iframe)
                return self.chrome
        except Exception as e:
            print(e)

    def make_request(self, url, is_post_request=False, payload=None, auth=None, return_content=True):
        response = self.remote_session.post(url=url, data=payload, auth=auth) if is_post_request \
            else self.remote_session.get(url, auth=auth)
        if response.ok:
            if return_content:
                return response.content
            else:
                return response
        print(f'status_code: {response.status_code}')
        return None

    def extract_html_from_page(self, url, scope='etree', auth=None, cookies=None):
        html_tree = None
        try:
            if scope == "webdriver":
                self.navigate(url)
                if cookies:
                    self.accept_notifications(cookies)
                html = self.chrome.page_source
                # self.close_webdriver_session()
            else:
                html = self.make_request(url, auth=auth)
            html_tree = etree.fromstring(html, parser=etree.HTMLParser())
        finally:
            return html_tree

    @staticmethod
    def get_xpath_field_from_html(html_tree, url_xpath, single_value=True):
        result_xpath = html_tree.xpath(url_xpath)
        if result_xpath:
            if single_value:
                return result_xpath[0]
            return result_xpath
        return None

    @staticmethod
    def get_xpath_items_from_html(etree, url_xpath):
        result_xpath = etree.xpath(url_xpath)
        if result_xpath:
            return result_xpath
        return None

    @staticmethod
    def translate_date_to_source_lang(root_source, format_date):
        translated = GoogleTranslator(source='auto', target=root_source.download_conf.get('lang')).translate(
            format_date)
        return translated

    def move_to_new_browser_tab(self, window_number):
        p = self.chrome.current_window_handle
        parent = self.chrome.window_handles[0]
        chld = self.chrome.window_handles[window_number]
        self.chrome.switch_to.window(chld)
        time.sleep(3)
        return self.chrome

    @staticmethod
    def create_folder_if_not_exists_simple(path):
        if not os.path.exists(path):
            os.mkdir(path)
