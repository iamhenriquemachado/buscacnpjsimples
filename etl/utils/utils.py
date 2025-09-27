import os
import logging
from datetime import datetime

def createCnpjDataDirectory():
    directory_name = 'cnpj_data'
    try:
        os.mkdir(directory_name)
        return True

    except FileExistsError as e:
            logging.error(f"Directory '{directory_name}' already exists.", e)
            return False

    except PermissionError as p:
        logging.error(f"Permission denied: Unable to create '{directory_name}'.", p)
        return False

def getCurrentMonthAndYear():
    return datetime.now().strftime("%Y-%m")
