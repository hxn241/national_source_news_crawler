import os
import sys
import pandas as pd
import datetime as dt

sys.path.append(os.path.dirname(os.path.realpath('__file__')))

from common.basic_utils import setup_logger
from ingestaweb.modules.db import Db
from ingestaweb.modules.gsheets import GSheetsWorkbook
from ingestaweb.modules.main_helpers import load_root_sources_from_config
from ingestaweb.settings import sources_config, ROOT_DIR, Config


logger = setup_logger(
    log_filename=f"{dt.datetime.now():%Y%m%d}report.log",
    src_path=os.path.join(ROOT_DIR, "data")
)


def update_data_to_gsheets(workbook, input_dataframe, sheet_name, upload_type):
    """
    upload_type: append or replace
    """
    start_row = workbook.next_available_row(workbook.get_worksheet(sheet_name)) if upload_type == 'append' else 2
    workbook.update_sheets_data(
        sheetname=sheet_name,
        start_cell=f"A{start_row}",
        body={'values': input_dataframe.values.tolist()}
    )


def data_cleansing_process(sources_to_report):
    try:
        df_ingesta_report = pd.DataFrame(
            sources_to_report,
            columns=[
                'SourceName',
                'Frequency',
                'Date',
                'Status',
                'DownloadedAt']
        )
        df_ingesta_report['Date'] = pd.to_datetime(df_ingesta_report['Date'])
        df_ingesta_report['Date'] = df_ingesta_report['Date'].dt.strftime('%d/%m/%Y')
        df_ingesta_report = df_ingesta_report[df_ingesta_report['Date'] == dt.datetime.now().strftime('%d/%m/%Y')]
        df_ingesta_report['DownloadedAt'] = df_ingesta_report['DownloadedAt'].astype('str')

        df_status_count = df_ingesta_report['Status'].value_counts().reset_index()
        df_status_count.insert(0, 'date', dt.datetime.now().strftime('%d/%m/%Y'))
        df_status_count = df_status_count.iloc[::-1].reset_index(drop=True)

        return df_ingesta_report, df_status_count
    except:
        logger.error("Error trying to make report dataframes")


if __name__ == "__main__":

    logger.info("Report process started\n")
    try:
        db = Db()

        root_sources = load_root_sources_from_config(sources_config, create_base_directories=False)
        sources_to_report = db.get_sources_filtered_report(root_sources)
        ingesta_report, status_count_report = data_cleansing_process(sources_to_report)

        workbook = GSheetsWorkbook(Config.SPREADSHEET_MASTER_INGESTA_ID, "key")
        update_data_to_gsheets(
            workbook,
            input_dataframe=ingesta_report,
            sheet_name='ingestaweb report',
            upload_type='append'
        )

        update_data_to_gsheets(
            workbook,
            input_dataframe=status_count_report,
            sheet_name='status count',
            upload_type='append'
        )

    except Exception as e:
        logger.error(f'Impossible to generate report. \n {e}')
    finally:
        logger.info(
            "Report process finished\n" + "=" * 60 + "\n\n"
        )


