#!/usr/bin/env python3
# coding: utf-8

## Récupération des assurés déjà contactés
# Auteurs : Guillaume PLANTIVEAU
# Date : 19/01/2022
#
# Commande pour lancer le script:
# %run lm_1_retrieve_assures_contactes -d 20220430

import argparse
import logging
import pandas as pd
import os

from datetime import datetime
import calendar

from dateutil.relativedelta import relativedelta

import retention_mrh.utils as utils
import retention_mrh.lm_utils as lm_utils

# Get logger
logger = logging.getLogger(__name__) 
#logger.setLevel(logging.INFO)

def process(date_extraction: str):
    '''Fonction pour récupérer les assurés déjà contactés

    Args:
        date_extraction (str): Date d'extraction des contrats au format yyyymm
    Raises:
        ValueError : Si les paramètres d'entrée ne sont pas cohérents
    Returns:
        None - Sauvegarde le dataset dans le path paramétré
    '''
    
    print("Process - {}".format(__file__.split("/")[-1]))
    logger.info('------------------------------------------')
    logger.info("Lancement du script: Récupération des assurés déjà contactés par (SANTEIS/MRH)")
    logger.info('------------------------------------------')
    logger.info('Date d\'extraction utilisée: %s' % date_extraction)
    logger.info('')

    if not utils.is_dateextraction_ok(date_extraction):
        error_msg = "date_extraction au mauvais format: {}".format(date_extraction)
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Récupération de la date_extraction au format date
    d_extract = datetime.strptime(date_extraction, '%Y%m%d')
    
    # Récupération des 8 derniers fichiers de contact/temoin
    filepath_list = []
    for m in range(0,-9, -1):
        date_before_month = d_extract + relativedelta(months=m)
        year = date_before_month.year
        month = date_before_month.month
        (firstday, lastday) = calendar.monthrange(year, month)
        previous_date = str(year) + str(month).zfill(2) + str(lastday).zfill(2)
        
        # Fichier SANTEIS
        contact_f = lm_utils.get_list_contact_santeis_filepath(previous_date, temoin = False)
        filepath_list.append(contact_f)
        temoin_f = lm_utils.get_list_contact_santeis_filepath(previous_date, temoin = True)
        filepath_list.append(temoin_f) 
        # Fichier CONTACT MRH
        contact_f = lm_utils.get_list_contact_filepath(previous_date, temoin = False)
        filepath_list.append(contact_f)
        temoin_f = lm_utils.get_list_contact_filepath(previous_date, temoin = True)
        filepath_list.append(temoin_f)         
    
    # TODO: Changer cet apport manuel
    filepath_list.append("../output/liste_mensuelle/list_contact_20220430_Complement.csv")
    filepath_list.append("../output/liste_mensuelle/list_temoin_20220430_Complement.csv")
    
    contact_df_list = []
    
    for file_path in filepath_list:
        if os.path.isfile(file_path):
            dataframe = pd.read_csv(file_path, dtype = {"NOGPA": str})
            dataframe = dataframe.rename(columns={'NOGPA': 'identifiantrce'})
            
            logger.info('Fichier : {}'.format(file_path))
            logger.info('Nombre d\'assurés à ne pas contacter : {:,}'.format(len(dataframe.index)).replace(',', ' '))
            logger.info("--")
            contact_df_list.append(dataframe)
        
    # Concaténation des fichiers
    contact_df = pd.concat(contact_df_list)
    logger.info('Nombre d\'assurés à ne pas contacter : {:,}'.format(len(contact_df.index)).replace(',', ' '))
    
    # Sauvegarde
    file_path = lm_utils.get_id_rce_contact_filepath(date_extraction)
    contact_df.to_csv(path_or_buf = file_path, index = False)

    logger.info('Process terminé - Tout est OK')
    print("✅ Tout marche bien navette")

if __name__ == '__main__':
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    logging.basicConfig(filename = utils.get_log_path("RETENTION_MRH_LM_1"), format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--date_extraction', 
                        help='Date d\'extraction des contrats au format yyyyMMdd: Récupération des contrats en cours à cette date')
    args = parser.parse_args()
    process(date_extraction = args.date_extraction)