import os
import time
import logging
import ftplib
import datetime as dt

from ingestaweb.settings import Config

logger = logging.getLogger(__name__)


class FTPClient:

    def __init__(self, default_path="/"):
        self.host = Config.FTP_HOST
        self.user = Config.FTP_USER
        self.pswd = Config.FTP_PASSWORD
        self.path = default_path
        self.disconnected = True
        self.create_connection()

    @staticmethod
    def create_date():
        return dt.datetime.now().strftime("%d-%m-%Y %H:%M")

    def __enter__(self):
        """ Allows connection with contextmanager (open) """
        return self

    def __exit__(self, exc_type, exc_value, tb):
        """ Allows connection with contextmanager (exit)"""
        self.session.quit()

    def create_connection(self, email_sent=False):
        """Connect to ftp"""
        start_time = time.time()
        while time.time() - start_time <= 300 and self.disconnected:
            try:
                self.session = ftplib.FTP(self.host)
                self.session.login(self.user, self.pswd)
                # self.conn.encoding = 'utf-8'
                self.session.cwd(self.path)
                self.disconnected = False
                print(f"{self.create_date()} - Connected to ftp")
            except Exception as e:
                time.sleep(10)
                logger.error(f"{self.create_date()} - Connection error. \n{e}")

        if self.disconnected:
            raise Exception("FTPConnectionError: Couldn't connect to ftp.")

    def check_connection(self, email_sent):
        """Check if ftp connection is already established. Otherwise, try to reconnect"""
        try:
            self.session.voidcmd("NOOP")
        except:
            logger.warning(f"{self.create_date()} - Trying to reconnect to ftp...")
            self.disconnected = True
            self.session = self.create_connection(email_sent)
        finally:
            return self

    def set_ftp_path(self, subpath_to_folder):
        """ Navegar al directorio del ftp que quedamos (dentro del definido en config) """
        path = '{}{}'.format(self.path, subpath_to_folder)
        if self.session.pwd() == "/" + self.path:
            self.session.cwd(subpath_to_folder)
        elif self.session.pwd() != "/" + path:
            self.session.cwd('..')  # return to parent directory
            self.session.cwd(subpath_to_folder)

    def get_file_from_dir(self, client, temp_dir):
        """ Descargar fichero del ftp en una carpeta temporal para su lectura """
        try:
            self.set_ftp_path(client.folder)
            filename = sorted(self.session.nlst(), key=lambda x: self.session.voidcmd(f"MDTM {x}"))[-1]
            file = os.path.join(temp_dir, "files", client.folder, filename)
            self.session.retrbinary("RETR " + filename, open(file, 'wb').write)
            return file
        except Exception as e:
            logger.error(
                "{0} - ERROR: FTP FileNotFound: It was not possible to get file {1} from ftp \n {2}".format(
                    self.create_date(), filename, e
                ))

    def retrieve_file(self, client, temp_dir):
        """ Descargar fichero del ftp en una carpeta temporal para su lectura """
        filename = None
        try:
            self.set_ftp_path(client.folder)
            filename = sorted(self.session.nlst(), key=lambda x: self.session.voidcmd(f"MDTM {x}"))[-1]
            files_to_save = []
            if client.channel == 'both':
                files_to_save.append(os.path.join(temp_dir, "files", client.folder, 'telegram', filename))
                files_to_save.append(os.path.join(temp_dir, "files", client.folder, 'whatsapp', filename))
            else:
                files_to_save.append(os.path.join(temp_dir, "files", client.folder, client.channel, filename))

            [self.session.retrbinary("RETR " + filename, open(file, 'wb').write) for file in files_to_save]
            return filename

        except Exception as e:
            logger.error(
                "{0} - ERROR: FTP FileNotFound: It was not possible to get file {1} from ftp \n {2}".format(
                    self.create_date(), filename, e
                ))
            return None

    def get_directory_docs_list(self, client):
        """ Get all files in a ftp directory for a certain client, returned as list """
        self.set_ftp_path(client.folder)
        return self.session.nlst()

    def upload_file(self, local_file_path, ftp_file_path, filename):
        """Upload a single file from local dir to ftp"""
        uploaded = False
        try:
            with open(os.path.join(local_file_path, filename), 'rb') as f:
                self.session.storbinary(fr'STOR {ftp_file_path}/{filename}', f)
            uploaded = True
        except ftplib.all_errors as e:
            uploaded = False
            logger.error(f"ERROR: Transfer not completed for {filename}. {e}")
        finally:
            return uploaded

    def create_dir_if_not_exists(self, dir_path):
        try:
            self.session.mkd(dir_path)
            return True
        except ftplib.error_perm:
            pass
        except:
            pass


# def create_daily_base_directories(ftp, all_sources):
#     """
#     FTP folders' structure:
#         -> date_today (by default, automatically created)
#             -> root_source
#                 -> source
#                     -> publication_name.pdf
#
#     example:
#         300323
#             20 minutos
#                 20 minutos (Ed Barcelona)
#                    20 Minutos_09012024.pdf
#                 20 minutos (Ed Sevilla)
#                     20 Minutos (Ed. Sevilla)_09012024.pdf
#
#     :param ftp: FTPClient session object
#     :param cedro_products: dict with the following structure
#         {group1: [product1, product2, ..., productN], ...}
#     """
#     today_folder = f"/{dt.datetime.now():%d%m%y}"
#     ftp.create_dir_if_not_exists(dir_path=today_folder)
#     for root_source, sources in all_sources.items():
#         base_path = f'/{today_folder}/{root_source}'
#         ftp.create_dir_if_not_exists(dir_path=base_path)
#         for source in sources:
#             ftp.create_dir_if_not_exists(dir_path=f'{base_path}/{source}')

def create_daily_base_directories_ftp(root_sources, ftp):
    """
    FTP folders' structure:
        -> date_today (by default, automatically created)
            -> root_source
                -> source
                    -> publication_name.pdf

    example:
        300323
            20 minutos
                20 minutos (Ed Barcelona)
                   20 Minutos_09012024.pdf
                20 minutos (Ed Sevilla)
                    20 Minutos (Ed. Sevilla)_09012024.pdf

    :param ftp: FTPClient session object
    :param cedro_products: dict with the following structure
        {group1: [product1, product2, ..., productN], ...}
    """
    try:
        ftp.session.cwd(Config.FTP_DEFAULT_PATH)
        today_folder = f"{dt.datetime.now():%Y%m%d}"
        ftp.create_dir_if_not_exists(dir_path=today_folder)
        ftp.session.cwd(today_folder)
        for root_source in root_sources:
            relevant_sources = [source for source in root_source.sources if source.is_weekday_relevant]
            if relevant_sources:
                base_path = root_source.dirname
                created_root_source = ftp.create_dir_if_not_exists(dir_path=base_path)
                if created_root_source:
                    ftp.session.cwd(base_path)
                    for source in relevant_sources:
                        ftp.create_dir_if_not_exists(dir_path=source.dirname)
                    ftp.session.cwd('..')
    except Exception as e:
        logger.error(f'Impossible to create daily base directories,\n {e}')
