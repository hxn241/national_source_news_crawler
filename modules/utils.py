from functools import wraps
import time
import unicodedata
from lxml import etree
import re
import datetime as dt
import os.path
import glob
from PIL import Image
from datetime import datetime
import os
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from ingestaweb.settings import date_format_conf, ROOT_DIR, TEMP_DIR_FILES


def retry(max_retries, wait_time=2):
    """
    Decorator to retry any functions 'max_retries' times.
    """

    def retry_decorator(func):
        @wraps(func)
        def retried_function(*args, **kwargs):
            for i in range(max_retries):
                try:
                    values = func(*args, **kwargs)
                    return values
                except Exception as e:
                    print(f"Retrying...Attempt number {i + 1} - {e}")
                    time.sleep(wait_time)
            func(*args, **kwargs)

        return retried_function

    return retry_decorator


def remove_accents(input_str):
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])


def create_folder_name(raw_name):
    """ remove accents and spaces from source name """
    aux_name = remove_accents(raw_name)
    return aux_name.replace(" ", "_").replace("'", "").lower()


def parse_date_fmt_to_current_date(url, offset=0):
    try:
        for pattern in date_format_conf['dt_format']:
            url = re.sub("{"f"{pattern}""}",
                         (dt.datetime.now() - dt.timedelta(days=offset))
                         .strftime(f"{date_format_conf['dt_format'].get(pattern)}"), url)
        return url
    except:
        print('Error while parsing url')
        return None


def convert_date_to_source_fmt(date_fmt):
    return dt.datetime.now().strftime(date_fmt)


def get_etree(html):
    if html:
        return etree.fromstring(html.content, parser=etree.HTMLParser())


def find_xpath_in_html_tree(tree, xpath):
    result_xpath = tree.xpath(xpath)
    if result_xpath:
        return result_xpath[0]
    return None


def get_xpath_field_from_html(html, url_xpath):
    etree = get_etree(html)
    result_xpath = etree.xpath(url_xpath)
    if result_xpath:
        return result_xpath[0]
    return None


def check_if_file_exists(path):
    return os.path.isfile(path)


def wait_until_downloaded(download_path, extensions):
    # seconds = 0
    dl_wait = True
    while dl_wait:
        time.sleep(1)
        any_tmp_file = any([True if fname.endswith(extensions) else False for fname in os.listdir(download_path)])
        if any_tmp_file:
            dl_wait = False
            time.sleep(1)


def make_pdf_file(pdf_content, output_file_path):
    downloaded = False
    try:
        with open(output_file_path, 'wb') as f:
            f.write(pdf_content)
            if check_if_file_exists(output_file_path):
                downloaded = True
    except Exception as e:
        print(e)
    finally:
        return downloaded


def get_last_filename_and_rename(new_filename):
    try:
        save_folder = os.path.dirname(new_filename)
        files = [file for file in glob.glob(save_folder + '/*') for extension in ['.pdf', '.zip'] if file.endswith(extension)]
        if files:
            # ruta completa archivo original
            downloaded_file_path = max(files, key=os.path.getctime)
            # extensión archivo original: ex: ".pdf"
            extension = os.path.splitext(downloaded_file_path)[1]
            os.rename(downloaded_file_path, os.path.splitext(new_filename)[0] + extension)
            time.sleep(1)
    except Exception as e:
        print(e)


def check_if_file_downloaded_and_rename_file(filename, rename_raw_file=True):
    status = False
    try:
        wait_until_downloaded(os.path.dirname(filename), extensions=('.pdf', '.zip'))
        if rename_raw_file:
            get_last_filename_and_rename(filename)
        status = True
    except Exception as e:
        print(e)
    finally:
        return status


def get_laprensa_pages(driver):
    try:
        side_menu = driver.find_element(By.XPATH, "//div[@id='scroller']/*[contains(@class,'iScrollVerticalScrollbar')]")

        scroll_style = driver\
            .find_element(By.XPATH, "//div[@id='scroller']//*[contains(@class,'iScrollIndicator')]")\
            .get_attribute('style')
        total_scroll_height = int(re.search(r'height: (.*)px;', scroll_style).group(1))
        actions = ActionChains(driver)
        actions.move_to_element(side_menu).click_and_hold().move_by_offset(0, -total_scroll_height).release().perform()
        elements = driver.find_elements(By.XPATH, "//div[@data-page-container='true']")
        return len(elements)
    except Exception as e:
        print(e)
        return -1


def remove_jpg_files(directory):
    for file in os.listdir(directory):
        if file.endswith(".jpg") or file.endswith(".png") or file.endswith(".jpeg"):
            try:
                os.remove(os.path.join(directory, file))
            except:
                print(f"Impossible to remove {file}")


def create_pdf_from_images(images_directory, pdf_path):
    try:
        images = [
            Image.open(os.path.join(images_directory, f))
            for f in sorted(os.listdir(images_directory))
            if f.endswith('.jpg') or f.endswith(".png")
        ]

        images[0].save(
            pdf_path, "PDF", resolution=100.0, save_all=True, append_images=images[1:]
        )
        print(f"PDF guardado con éxito: {pdf_path}")

    except Exception as e:
        print(f"Imposible guardar PDF. {e}")


def create_daily_base_directories(root_source, ftp):
    if any(source.is_weekday_relevant for source in root_source.sources):
        fecha_actual = datetime.now().strftime("%d%m%Y")
        sources_relevantes = [source.name for source in root_source.sources if source.is_weekday_relevant]

        ruta_raiz = os.path.join(ROOT_DIR, 'ingestaweb', 'data', fecha_actual, root_source.name)
        for source_name in sources_relevantes:
            ruta = os.path.join(ruta_raiz, source_name)
            os.makedirs(ruta, exist_ok=True)


def delete_file_extension(folder, extension):
    try:
        # Cambiar al directorio especificado
        os.chdir(folder)

        # Obtener la lista de archivos en el directorio actual
        files = os.listdir()

        # Filtrar archivos por extensión y borrarlos
        for file in files:
            if file.endswith(extension):
                try:
                    os.remove(file)
                except OSError as e:
                    print(f"No se pudo borrar {file}: {e}")
    except FileNotFoundError:
        print(f"El directorio {folder} no fue encontrado.")


def delete_files_in_folder(folder_path):
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Unable to delete {file_name}: {e}")


def remove_file(file_path):
    try:
        os.remove(file_path)
    except Exception as e:
        print(f"Impossible to remove file {file_path}", e)
