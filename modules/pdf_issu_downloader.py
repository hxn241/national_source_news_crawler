import os
import time
import requests
from PIL import Image
from lxml import etree
from urllib.parse import urlparse
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from ingestaweb.settings import ROOT_DIR


def create_folder_if_not_exists_issu_scr(path):
    if not os.path.exists(path):
        os.mkdir(path)


class WebDriver:

    def __init__(self, headless, proxy=None):
        self.browser = self._get_session_profile(headless, proxy)
        self.session = requests.session()

    @staticmethod
    def _get_session_profile(headless, proxy):
        """
        Initialize browser with profile options.
        The profile is stored in user-data-dir the first time we open the browser if the
        argument is passed to webdriver options. From the second time on, the browser is
        opened with the same profile/options.
        """
        try:
            chrome_options = uc.ChromeOptions()
            # chrome_options.headless = headless
            if headless:
                chrome_options.add_argument('--headless')
            if proxy:
                chrome_options.add_argument(f'--proxy-server={proxy.get("https")}')
            # user_data_dir = os.path.join(ROOT_DIR, 'tmp', 'chrome')
            # return uc.Chrome(options=chrome_options, user_data_dir=user_data_dir)
            return uc.Chrome(options=chrome_options)
        except Exception as e:
            print(e)
            return None

    def set_cookies_from_browser(self):
        for cookie in self.browser.get_cookies():
            self.session.cookies.set(cookie['name'], cookie['value'])

    def create_session(self):
        user_agent = self.browser.execute_script("return navigator.userAgent;")
        self.session.headers.update({'User-Agent': user_agent})
        self.set_cookies_from_browser()


def remove_jpg_files(directory):
    for file in os.listdir(directory):
        if file.endswith(".jpg"):
            try:
                os.remove(os.path.join(directory, file))
            except:
                print(f"Impossible to remove {file}")


def create_pdf_from_images(images_directory, pdf_path):
    try:
        images = [
            Image.open(os.path.join(images_directory, f))
            for f in sorted(os.listdir(images_directory))
            if f.endswith('.jpg')
        ]

        images[0].save(
            pdf_path, "PDF", resolution=100.0, save_all=True, append_images=images[1:]
        )
        print(f"PDF guardado con éxito: {pdf_path}")

    except Exception as e:
        print(f"Imposible guardar PDF. {e}")


def scrape_issu(url_to_scrape, temp_dir, downloads_dir):
    endpoint = get_endpoint_form_url(url_to_scrape)
    if endpoint:
        temp_dir = os.path.join(ROOT_DIR, 'ingestaweb', "data", "images_temp")
        downloads_dir = os.path.join(ROOT_DIR, 'ingestaweb', "data", "issuu")
        create_folder_if_not_exists_issu_scr(temp_dir)
        create_folder_if_not_exists_issu_scr(downloads_dir)
    else:
        print("Url incorrecta. Prueba de nuevo.")
    time.sleep(10)

    base_url = "https://issuu.com"
    request_url = f"{base_url}{endpoint}"
    chrome = WebDriver(headless=True)
    chrome.browser.get(request_url)
    WebDriverWait(chrome.browser, 20).until(
        EC.element_to_be_clickable((By.XPATH, "//*[@id='CybotCookiebotDialogBodyButtonDecline']"))
    ).click()
    time.sleep(5)
    chrome.create_session()
    resp = chrome.session.get(request_url)
    if resp.ok:
        parser = etree.HTMLParser(recover=True)  # omit parsing errors
        html_tree = etree.fromstring(resp.text, parser=parser)
        secure_url = html_tree.xpath("//meta[@property='og:image:secure_url']/@content")
        if secure_url:
            limit = False
            secure_url = secure_url[0]
            c = 1
            while not limit:
                print(f"Retrieving page {c}")
                page_resp = chrome.session.get(secure_url.replace("page_1.jpg", f"page_{c}.jpg"))
                if page_resp.ok:
                    with open(os.path.join(temp_dir, f'page_{c:03d}.jpg'), 'wb') as img:
                        img.write(page_resp.content)
                elif page_resp.status_code == 403:
                    limit = True
                c += 1

    create_pdf_from_images(
        images_directory=temp_dir,
        pdf_path=os.path.join(downloads_dir, f"issuu_{endpoint.replace('/', '_')}.pdf")
    )

    remove_jpg_files(temp_dir)


def get_endpoint_form_url(url):
    try:
        url_obj = urlparse(url)
        return url_obj.path
    except:
        return None


def start_issu_download(url_to_scrape):
    endpoint = get_endpoint_form_url(url_to_scrape)
    if endpoint:
        print(f"Descargando PDF de {url_to_scrape}")
        temp_dir = os.path.join(ROOT_DIR, 'ingestaweb', "data", "images_temp")
        downloads_dir = os.path.join(ROOT_DIR, 'ingestaweb', "data", "issuu")
        create_folder_if_not_exists_issu_scr(temp_dir)
        create_folder_if_not_exists_issu_scr(downloads_dir)
        scrape_issu(endpoint, temp_dir, downloads_dir)
    else:
        print("Url incorrecta. Prueba de nuevo.")
    time.sleep(10)
