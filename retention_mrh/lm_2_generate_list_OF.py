#!/usr/bin/env python3
# coding: utf-8

## Génération de la liste des contrats à contacter pour cette date d'extraction
# Auteurs : Guillaume PLANTIVEAU
# Date : 24/01/2022
#
# Commande pour lancer le script:
# %run lm_2_generate_list_OF --repertoire /met_dsm/TA/work/Sante -p 202205 -d 20220531 -s 22

import argparse
import logging

import os.path 

import pandas as pd
import numpy as np
import pickle

from pyspark.sql.functions import col

from datetime import datetime
from dateutil.relativedelta import relativedelta
import random
import retention_mrh.utils as utils
import retention_mrh.spark_utils as spark_utils
import retention_mrh.lm_utils as lm_utils
import retention_mrh.message_utils as message_utils

import tarificateur.santeis as moteur_santeis
import tarificateur.serenivie as moteur_serenivie

# Get logger
logger = logging.getLogger(__name__)

def get_score_fragilite():
    # Code le plus propre de l'histoire du code
    return random.randint(7,10)

def _name_campagne(date_extraction, num_semaine):
    """Nommer la campagne sur le format suivant: TEST"""
    # Récupération de la date d'extraction au format timestamp
    dt_extract = datetime.strptime(date_extraction, '%Y%m%d')
    # Ajout d'1 mois (Si 20211231 => On est en train de traiter la liste de Janvier 2022)
    dt_extract = dt_extract + relativedelta(months=1)
    
    campagne_name = "OF_R2_MRH_Vague " + str(dt_extract.year) + " - S" + num_semaine.zfill(2) 

    return campagne_name

def process(dt_partition: str, repertoire: str, date_extraction: str, num_semaine: str):
    '''Fonction pour générer la liste des contrats à contacter pour cette date d'extraction

    Args:
        dt_partition (str): Date de partition au format yyyymm
        repertoire (str): Répertoire HDFS de stockage des fichiers de travail. Exemple: '/met_dsm/TA/work/Sante'
        date_extraction (str): Date d'extraction des contrats au format yyyymm
        num_semaine (str): Numéro de la semaine servant d'identifiant de vague
    Raises:
        ValueError : Si les paramètres d'entrée ne sont pas cohérents
        FileNotFoundError : Si le fichier correspondant à la date d'extraction n'existe pas
    Returns:
        None - Sauvegarde le dataset dans le répertoire data
    '''

    print("Process - {}".format(__file__.split("/")[-1]))
    logger.info('------------------------------------------')
    logger.info("Lancement du script: Génération de la liste à envoyer l'OF")
    logger.info('------------------------------------------')
    logger.info('Répertoire de stockage des données: %s' % repertoire)
    logger.info('Date d\'extraction utilisée: %s' % date_extraction)
    logger.info('Numéro de semaine de la vague: %s' % num_semaine)
    logger.info('')
    
    if not utils.is_dateextraction_ok(date_extraction):
        error_msg = "date_extraction au mauvais format: {}".format(date_extraction)
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    # Lecture de la liste
    output_df = pickle.load(open("../data/liste_OF_v2.pk", "rb"))
    
    # Ajout des données CC
    spark = spark_utils.get_spark_builder().getOrCreate()
    
    # Ajout de l'indicateur de présence CC
    hierarchie_df = spark.sql("select * from common_gmc.gmc_tables_retravaille_RS_Hierarchie where dt_partition = '"+dt_partition+"'")
    hierarchie_df = hierarchie_df.filter(col("niveau_ptf") == 5)
    hierarchie_df = hierarchie_df.select("Id_Intermediaire")
    hierarchie_df = hierarchie_df.toPandas()
    hierarchie_df["Presence_CC"] = "O"
    logger.info('Nombre de CC dans la hierarchie RS: {:,}'.format(len(hierarchie_df.index)).replace(',', ' '))
    output_df = output_df.merge(hierarchie_df, how = 'left', left_on = "identifiantintermediaire", right_on="Id_Intermediaire")
    output_df['Presence_CC'] = output_df.Presence_CC.fillna("N")
    logger.info('Nombre de contrats sans CC: {:,}'.format(len(output_df[output_df["Presence_CC"] == "N"].index)).replace(',', ' '))
    logger.info('Nombre de contrats avec CC: {:,}'.format(len(output_df[output_df["Presence_CC"] == "O"].index)).replace(',', ' '))
    
    # Ajout des infos CC
    rs_df = spark.read.format("csv").option("sep", ";").option("encoding", "UTF-8").option("header", "true").load("/met_dsm/TA/work/Sante/Apporteurs/RSG.csv")
    rs_df = rs_df.withColumnRenamed("Nom","cc_nom")
    rs_df = rs_df.withColumnRenamed("Prénom","cc_prenom")
    rs_df = rs_df.withColumnRenamed("Adresse mail", "email")
    rs_df = rs_df.select(["cc_nom","cc_prenom","email"]).toPandas()
    
    output_df["intmail"] =  output_df.apply(lambda x: lm_utils.upper_string(x["intmail"]), axis=1)
    output_df = output_df.merge(rs_df, how = 'left', left_on = "intmail", right_on="email")
    logger.info('Nombre de contrats sans le nom du CC: {:,}'.format(len(output_df[output_df["cc_nom"].isnull()].index)).replace(',', ' '))
    logger.info('Nombre de contrats avec le nom du CC: {:,}'.format(len(output_df[~output_df["cc_nom"].isnull()].index)).replace(',', ' '))
    
    # Ajout du score de fragilité
    output_df["score_fragilite"] = output_df.apply(lambda x: get_score_fragilite(), axis=1)

    # On positionne l'idRCE en int temporairement pour gérer mieux la jointure suivante
    output_df["identifiantrce"] = output_df.apply(lambda x: int(x["identifiantrce"]), axis=1)
    
    # Gestion des assurés déjà contactés
    file_path = lm_utils.get_id_rce_contact_filepath(date_extraction)
    client_contacte_df = pd.read_csv(file_path, dtype = {"identifiantrce": int})
    logger.info('Nombre d\'assurés à ne pas contacter : {:,}'.format(len(client_contacte_df.index)).replace(',', ' '))
    client_contacte_df["Contact"] = "DONE"

    output_df = output_df.merge(client_contacte_df, how = 'left', on = "identifiantrce")
    output_df['Contact'] = output_df.Contact.fillna("-")
    logger.info('Nombre d\'assurés qu\'on peut contacter dans la liste: {:,}'.format(len(output_df[output_df["Contact"] == "-"].index)).replace(',', ' '))
    
    output_df["identifiantrce"] = output_df.apply(lambda x: str(int(x["identifiantrce"])).zfill(8), axis=1)
    
    # Gestion du téléphone
    output_df["telephone"] = output_df.apply(lambda x: lm_utils.get_telephone(x["telmobile"],x["telfixe"]), axis=1)
    
    # Gestion de l'indicatif téléphone
    output_df["indicatif"] = output_df.apply(lambda x: lm_utils.get_code_indic_tel(x["codedepartement"]), axis=1)
    logger.info('Nombre de contrats dans le dataset: {:,}'.format(len(output_df.index)).replace(',', ' '))
    
    # On retire de la liste les contrats sans téléphone
    output_df = output_df[~(output_df["telephone"] == "")]
    logger.info('Nombre de contrats dans le dataset avec un téléphone {:,}'.format(len(output_df.index)).replace(',', ' '))
  
    # Filtre sur l'age
    output_df = output_df[~output_df["agepersphysique"].isnull()]
    output_df = output_df[output_df["agepersphysique"] >= 18]
    
    # Récupération de l'age en entier
    output_df["age_assure"] = output_df.apply(lambda x: int(x["agepersphysique"]), axis=1)
    
    # Récupération du nom du fichier cible pour voir si il n'a pas déjà été généré
    new_file_path = lm_utils.get_list_contrat_filepath(date_extraction, 2)
    
    if os.path.isfile(new_file_path): # La liste existe déjà. On ne recherche pas les ids, on prend les existants
        logger.info("L'extraction pour cette date existe déjà. On récupère les identifiants ...")
        former_df = pd.read_csv(new_file_path, dtype = {"id_contrat": str})
        id_output_set = set(list(former_df["id_contrat"]))
        id_temoin_set = set(list(former_df[former_df["temoin"] == True]["id_contrat"]))

        output_df = output_df[output_df["identifiant"].isin(id_output_set)]
        output_df["temoin"] = output_df.apply(lambda x: x["identifiant"] in id_temoin_set, axis=1)    
        logger.info('Nombre de contrats dans cette liste: {:,}'.format(len(output_df.index)).replace(',', ' '))
    else: # La liste n'existe pas. On va faire le tirage dans ce cas
        logger.info("L'extraction pour cette date n'existe pas. On applique les filtres")
        output_df = lm_utils.filter_vague(output_df, date_extraction)
        id_output_set = set(list(output_df["identifiant"]))
        logger.info('Nombre de contrats dans cette liste: {:,}'.format(len(id_output_set)).replace(',', ' '))

    # Si doublon pour un individu. On prend la première ligne
    output_df.drop_duplicates(subset = ['identifiantrce'], keep = 'first', inplace = True)
    
    # Qualité donnée - Primes
    output_df["prime_annuelle"] = output_df["montantannuelttcvalestime"].fillna(0)
    output_df["prime_mensuelle"] = output_df.apply(lambda x: int(x["prime_annuelle"]/12), axis=1)

    # Application du tarif SANTEIS
    moteur_tarif = moteur_santeis.Moteur_Santeis()
    output_df["proposition_formule_santeis"] = output_df.apply(lambda x: moteur_tarif.propose_formule_tarif(x["codedepartement"],x["age_assure"])[0], axis=1)
    output_df["proposition_tarif_santeis"] = output_df.apply(lambda x: moteur_tarif.propose_formule_tarif(x["codedepartement"],x["age_assure"])[1], axis=1)

    # Application du tarif SERENIVIE
    moteur_tarif = moteur_serenivie.Moteur_Serenivie()
    output_df["proposition_formule_serenivie"] = output_df.apply(lambda x: moteur_tarif.propose_formule_tarif(x["age_assure"])[0], axis=1)
    output_df["proposition_tarif_serenivie"] = output_df.apply(lambda x: moteur_tarif.propose_formule_tarif(x["age_assure"])[1], axis=1)

    # Message de fragilité (message1)
    output_df["message1"] = output_df.apply(lambda x: message_utils.write_message1(x["autres_produits_detenus"], x["raisons_fragilite"]), axis=1)
    # Message de vente (message2)
    output_df["message2"] = output_df.apply(lambda x: message_utils.write_message2(x["proposition"],
                                                                                   x["libsituationfamiliale"],
                                                                                   x["SITFAM_2"],
                                                                                   x["Presence_CC"],
                                                                                   x["proposition_formule_santeis"],
                                                                                   x["proposition_tarif_santeis"],
                                                                                   x["proposition_formule_serenivie"],
                                                                                   x["proposition_tarif_serenivie"]), axis=1)
        
    # Nommage de la campagne
    output_df["campagne"] = _name_campagne(date_extraction, num_semaine)
    
    # Gestion de formule
    output_df["formule"] = output_df["lib_produit"]
    
    # Age du client
    output_df["age_assure"] = output_df.agepersphysique.fillna(0).astype('int')

    # On prépare la liste contact / temoin
    contact_df = output_df[["identifiantrce","score_fragilite","temoin"]]

    list_temoin_df = contact_df[contact_df["temoin"] == True]
    list_contact_df = contact_df[contact_df["temoin"] == False]

    logger.info('Nombre de contrats à contacter: {:,}'.format(len(list_contact_df.index)).replace(',', ' '))
    logger.info('Nombre de contrats témoins en plus: {:,}'.format(len(list_temoin_df.index)).replace(',', ' '))
    
    contact_filepath_csv = lm_utils.get_list_contact_filepath(date_extraction, False)
    contact_filepath_xls = lm_utils.get_list_contact_filepath(date_extraction, False, '.xlsx')
    temoin_filepath_csv = lm_utils.get_list_contact_filepath(date_extraction, True)
    temoin_filepath_xls = lm_utils.get_list_contact_filepath(date_extraction, True, '.xlsx')
    
    # Stockage de la liste à contacter
    list_contact_df = list_contact_df[["identifiantrce","score_fragilite"]].rename(columns={'identifiantrce': 'NOGPA',
                                                                                            'score_fragilite': 'note_fragilite'})
    list_contact_df.to_csv(path_or_buf = contact_filepath_csv, index = False)
    list_contact_df.to_excel(excel_writer = contact_filepath_xls, index = False)  
    logger.info("Fichier de contact stocké sous " + contact_filepath_xls)
    # Stockage de la liste témoin
    list_temoin_df = list_temoin_df[["identifiantrce","score_fragilite"]].rename(columns={'identifiantrce': 'NOGPA',
                                                                                          'score_fragilite': 'note_fragilite'})
    list_temoin_df.to_csv(path_or_buf = temoin_filepath_csv, index = False)
    list_temoin_df.to_excel(excel_writer = temoin_filepath_xls, index = False)  
    logger.info("Fichier témoin stocké sous " + temoin_filepath_xls)
    
    # Renaming
    output_df = output_df.rename(columns={'nompersonne': 'nom','identifiant': 'id_contrat'})
    
    # Stockage de tout le fichier
    output_df.to_csv(path_or_buf = new_file_path, index = False)
    logger.info("Fichier de travail stocké sous " + new_file_path)
    
    # Génération de la liste pour l'app
    output_df = output_df[output_df["temoin"] == False]
    output_df = output_df.rename(columns={'prime_mensuelle': 'prime'})
    output_df['rac'] = 0
    output_df['type_contrat'] = " - "
    
    col_keeper = ["identifiantrce","nom","prenom","campagne","age_assure","libcsp","telephone",
              "nomrue","localite","formule","prime","rac","message1","message2","id_contrat",
                  "type_contrat","cc_nom","cc_prenom","indicatif"]
    output_df = output_df[col_keeper]

    file_path = lm_utils.get_outputpath(date_extraction)
    output_df.to_csv(path_or_buf = file_path, index = False)
    logger.info("Fichier output stocké sous " + file_path)
    
    logger.info('Process terminé - Tout est OK')    
    print("✅ Tout marche bien navette")

    
if __name__ == '__main__':
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    logging.basicConfig(filename = utils.get_log_path("RETENTION_MRH_LM_2"), format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--partition', 
                        help='Date de partition au format yyyymm')
    parser.add_argument('-r', '--repertoire', 
                        help='Répertoire HDFS de stockage des fichiers de travail. Exemple: \'/met_dsm/TA/work/Sante\'')
    parser.add_argument('-s', '--semaine', 
                        help='Numéro de la semaine servant d\'identifiant de vague')
    parser.add_argument('-d', '--date_extraction', 
                        help='Date d\'extraction des contrats au format yyyyMMdd: Récupération des contrats en cours à cette date')
    args = parser.parse_args()
    process(dt_partition = args.partition, repertoire = args.repertoire, date_extraction = args.date_extraction, num_semaine = args.semaine)