import os
from dotenv import load_dotenv
import requests
from bs4 import BeautifulSoup
import json
from app.utils.logging_init import init_logger
from datetime import date as dt
load_dotenv()

logger = init_logger()
folder_filter = json.loads(os.getenv('FIELDS'))

parent_path = os.getenv('BASE_DATA_PATH')
answer = os.getenv('ALL_YEARS')


def pdffile_download(parent_path, folder_filter, answer):
    url = "https://www.cnlopb.ca/information/statistics/#rm"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    folders = list(folder_filter.keys())
    todays_date = dt.today()
    present_year = todays_date.year
    links_curr_year = soup.find_all('a', text=present_year)
    # Setting the current working directory
    os.chdir(parent_path)
    for folder in folders:
        curr_folder_path = parent_path + '//' + folder

        if not os.path.isdir(curr_folder_path):
            os.mkdir(folder)
        os.chdir(curr_folder_path)
        if answer == 'yes':
            folder_filter_curr = folder_filter[folder]
            # To display respective folder where related files going to download
            logger.info(f"In Folder: {folder}")
            i = 0
            try:
                for link in links:
                    if folder_filter_curr in link.get('href', []):
                        i += 1
                        # To display downloading file number
                        logger.info(f"Downloading file:  {i}")
                        response_page = requests.get(link.get('href'))
                        name = link.get('href').split('/')[-1]
                        pdf = open(name, 'wb')
                        pdf.write(response_page.content)
                        pdf.close()
                        # To display specific file is downloaded
                        logger.info(f"File {i} downloaded")
                os.chdir(parent_path)
            except Exception as e:
                logger.error(e)
        else:
            try:
                folder_filter_curr = folder_filter[folder]
                logger.info(f"In Folder: {folder}")
                i = 0
                for link in links_curr_year:
                    if folder_filter_curr in link.get('href', []):
                        i += 1
                        # To display downloading file number
                        logger.info(f"Downloading file: {i}")
                        response_page = requests.get(link.get('href'))
                        name = link.get('href').split('/')[-1]
                        pdf = open(name, 'wb')
                        pdf.write(response_page.content)
                        pdf.close()
                        # To display specific file is downloaded
                        logger.info(f"File {i} downloaded")
                os.chdir(parent_path)
            except Exception as e:
                logger.error(e)


if __name__ == '__main__':
    pdffile_download(parent_path, folder_filter, answer)
    # To show all files are downloaded.
    logger.info(f"All PDF files downloaded")
