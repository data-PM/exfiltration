#!/usr/bin/env python3
# coding: utf-8

## Utils - Fonction utils
# Auteurs : Guillaume PLANTIVEAU
# Date : 16/03/2021

import datetime
import pandas as pd
from dataclasses import dataclass
import logging
import math
import numpy as np

# Get logger
logger = logging.getLogger(__name__)

indicatif_dict = {1: "04",2: "03",3: "04",4: "04",5: "04",6: "04",7: "04",8: "03",9: "04", 10: "03",11: "04",12: "04",13: "04",
                  14: "02",15: "04",16: "05",17: "05",18: "02",19: "05",20: "04",21: "03",22: "02",23:"05",24: "05",25: "03",
                  26: "04",27: "02",28: "02",29: "02",30: "04",31: "04",32: "04",33: "05",34: "04",35: "02",36: "02",37: "02",
                  38: "04",39: "03",40: "05",41: "02",42: "04",43: "04",44: "02",45: "02",46: "04",47: "05",48:"04",49: "02",
                  50: "02",51: "03",52: "03",53: "02",54: "03",55: "03",56: "02",57: "03",58: "03",59: "03",60: "03",61: "02",
                  62: "03",63: "04",64: "05",65: "04",66: "04",67: "03",68: "03",69: "04",70: "03",71: "03",72: "02",73: "04",
                  74: "04",75: "01",76: "02",77: "01",78: "01",79: "05",80: "03",81: "04",82: "04",83: "04",84: "04",85: "02",
                  86: "05",87: "05",88: "03",89: "03",90: "03",91: "01",92: "01",93: "01",94: "01",95: "01",97: "-"}

@dataclass
class Produit:
    cdproduit: str
    cdproduit_montee_gamme: str
    lib_famille: str
    lib_produit: str
    
produit_dict = {}
produit_dict["SATEI-FM01"] = Produit("SATEI-FM01","SATEI-FM02", "CLASSIQUE", "SANTEIS CLASSIQUE F1")
produit_dict["SATEI-FM02"] = Produit("SATEI-FM02","SATEI-FM03", "CLASSIQUE", "SANTEIS CLASSIQUE F2")
produit_dict["SATEI-FM03"] = Produit("SATEI-FM03","SATEI-FM04", "CLASSIQUE", "SANTEIS CLASSIQUE F3")
produit_dict["SATEI-FM04"] = Produit("SATEI-FM04","SATEI-FM05", "CLASSIQUE", "SANTEIS CLASSIQUE F4")
produit_dict["SATEI-FM05"] = Produit("SATEI-FM05","SATEI-FM06", "CLASSIQUE", "SANTEIS CLASSIQUE F5")
produit_dict["SATEI-FM06"] = Produit("SATEI-FM06","SATEI-FM06", "CLASSIQUE", "SANTEIS CLASSIQUE F6")
produit_dict["SATEI-FS03"] = Produit("SATEI-FS03","SATEI-FS3P", "SENIOR", "SANTEIS SENIOR F3")
produit_dict["SATEI-FS3P"] = Produit("SATEI-FS3P","SATEI-FS05", "SENIOR", "SANTEIS SENIOR F3+")
produit_dict["SATEI-FS05"] = Produit("SATEI-FS05","SATEI-FS05", "SENIOR", "SANTEIS SENIOR F5")

def get_produit_dict():
    return produit_dict
    
def get_code_indic_tel(code_departement):
    if code_departement:
        return indicatif_dict.get(int(code_departement),"-")
    else:
        return "-"

def get_cdproduit_propose(action_id, cdproduit):
    """Proposition de nouveau cdproduit en se basant sur l'action_id"""
    cdproduit_propose = ""
    if(action_id[0] == '1'): # Classique vers Senior
        cdproduit_propose = "SATEI-FS03"
    if(action_id[1] == '1'): # Classique++ / FM03 si ancienne formule
        ancien_produit = Produit("AUTRE","SATEI-FM03", "AUTRE", "AUTRE")
        cdproduit_propose = produit_dict.get(cdproduit, ancien_produit).cdproduit_montee_gamme
    if(action_id[2] == '1'): # Senior++ / FS03 si ancienne formule
        ancien_produit = Produit("AUTRE","SATEI-FS03", "AUTRE", "AUTRE")
        cdproduit_propose = produit_dict.get(cdproduit, ancien_produit).cdproduit_montee_gamme
    if cdproduit_propose == "": # Si pas de montée de gamme
        cdproduit_propose = cdproduit    

    return cdproduit_propose

def flag_sim_atypique(formule, mont_rac, mont_rac_simulated):
    """Flag si une simulation est trop éloigné de la réalité"""
    flag = 0
    
    if (formule in produit_dict.keys()):
        if mont_rac > 0:
            mont_rac = float(mont_rac)
            mont_rac_simulated = float(mont_rac_simulated)
            diff = abs(mont_rac - mont_rac_simulated)
            ratio = 100*(diff / mont_rac)
            if ratio > 10:
                if diff > 5:
                    flag = 1
        else:
            if mont_rac_simulated >= 1:
                flag = 1
    
    return flag

def upper_string(string_to_process: str):
    output = ""
    if string_to_process:
        output = string_to_process.upper()
    return output

def correct_mont_rac_sim(sim_atypique, mont_rac, mont_rac_simulated):
    "Si la simulation actuel est 'atypique' alors on prend le RAC actuel plutôt que le simulé"
    output_rac = mont_rac_simulated
    if sim_atypique == 1:
        output_rac = mont_rac
    return output_rac

def get_montant_rac_sim_ko(sim_atypique, mont_rac, statut_simulated):
    "Renvoit le montant du RAC si celui ci est atypique ou KO"
    output_rac = 0
    if sim_atypique == 1:
        output_rac = mont_rac
    if statut_simulated == "KO":
        output_rac = mont_rac
    return output_rac

def get_telephone(telmobile, telfixe):
    "Formattage du téléphone"
    if not math.isnan(telmobile):
        telephone = "0" + str(telmobile).split('.')[0]
    else:
        if not math.isnan(telfixe):
            telephone = "0" + str(telfixe).split('.')[0]
        else:
            telephone = ""
    
    return telephone

def get_formule(cdproduit: str, rce_lib_produit: str):
    "Récupérer le libellé du produit"
    formule = ""
    if cdproduit in produit_dict:
        formule = produit_dict[cdproduit].lib_produit
    else:
        formule = rce_lib_produit + " (" + cdproduit + ")"
    
    return formule

def get_famille_contrat(cdproduit):
    "Récupérer la famille du produit"
    formule = ""
    if cdproduit in produit_dict:
        formule = produit_dict[cdproduit].lib_famille
    else:
        formule = "AUTRE"
    
    return formule

def get_id_rce_contact_filepath(date_extraction : str):
    '''Fonction pour récupérer le nom de fichier de la liste de clients déjà contactés'''
    filepath = "../data/id_rce_contact_"
    filepath += date_extraction
    filepath += '.csv'
    
    return filepath

def get_list_contact_santeis_filepath(date_extraction : str, temoin: bool = False, extension: str = ".csv"):
    '''Fonction pour récupérer le filepath du fichier de contact / temoin'''
    if temoin:
        filepath = "../../SANTE_INDIV/output/liste_mensuelle/list_temoin_"
    else:
        filepath = "../../SANTE_INDIV/output/liste_mensuelle/list_contact_"
    filepath += date_extraction
    filepath += extension
    return filepath

def get_list_contact_filepath(date_extraction : str, temoin: bool = False, extension: str = ".csv"):
    '''Fonction pour récupérer le filepath du fichier de contact / temoin'''
    if temoin:
        filepath = "../output/liste_mensuelle/list_temoin_"
    else:
        filepath = "../output/liste_mensuelle/list_contact_"
    filepath += date_extraction
    filepath += extension
    return filepath

def get_list_contrat_filepath(date_extraction : str, index : int):
    '''Fonction pour récupérer le filepath de la liste de contrats'''
    filepath = "../data/work_list_contrats_"
    filepath += date_extraction
    filepath += "_"
    filepath += str(index)
    filepath += '.csv'
    
    return filepath

def get_outputpath(date_extraction : str):
    '''Fonction pour récupérer le filepath de l'output'''
    filepath = "../output/liste_mensuelle/output_"
    filepath += date_extraction
    filepath += '.csv'
    
    return filepath

def last_day_of_month(any_day):
    ''' Fournit le dernier jour du mois appartenant au jourj en input'''
    # On prend le 28ème jour du mois, et on ajoute 4 jours. On est donc dans le mois suivant automatiquement
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # On retire le nombre de jour du mois suivant, pour arriver automatiquement au dernier jour du mois d'anyday
    return next_month - datetime.timedelta(days=next_month.day)

def is_action1(formule, age_assure):
    ''' Montée de gamme Classique vers Senior'''
    if formule != "SANTEIS_SENIOR":
        if age_assure > 60:
            return 1
    return 0

def is_action2(formule, montant_rac_12, age_assure):
    ''' Montée de gamme Classique '''
    if formule != "SANTEIS_SENIOR":
        if montant_rac_12 > 100:
            if age_assure <= 60:
                return 1
    return 0
    

def is_action3(formule, montant_rac_12):
    ''' Montée de gamme Senior'''
    if formule == "SANTEIS_SENIOR":
        if montant_rac_12 > 250:
            return 1
    return 0

def is_action4(top_multiequipe):
    ''' Multi-équipement'''
    if top_multiequipe == 0:
        return 1
    return 0

def is_action5(nbrctract_fam_mrh, nbrctract_fam_gav, nbrctract_fam_protectionjuridique):
    ''' Multi-équipement - Faible'''
    if (nbrctract_fam_mrh > 0) & (nbrctract_fam_gav == 0) & (nbrctract_fam_protectionjuridique == 0):
        return 1
    if (nbrctract_fam_mrh == 0) & (nbrctract_fam_gav > 0) & (nbrctract_fam_protectionjuridique == 0):
        return 1
    if (nbrctract_fam_mrh == 0) & (nbrctract_fam_gav == 0) & (nbrctract_fam_protectionjuridique > 0):
        return 1
    return 0

def is_action6(formule, anciennete_contrat_mois):
    ''' Bonus fidélité 1 an '''
    if (formule == "SANTEIS_SENIOR") | (formule == "SANTEIS_CLASSIQUE"):
        if anciennete_contrat_mois >= 6:
            if anciennete_contrat_mois < 30:
                return 1
    return 0

def is_action7(formule, anciennete_contrat_mois):
    ''' Bonus fidélité 3 ans '''
    if (formule == "SANTEIS_SENIOR") | (formule == "SANTEIS_CLASSIQUE"):
        if anciennete_contrat_mois >= 30:
            return 1
    return 0

def get_action_id(seg_action1: int, seg_action2: int, seg_action3: int, seg_action67: int):
    "Affecter un id action unique pour cette vague"
    action_id = 0
    if seg_action1 == 1:
        action_id += 1000
    if seg_action2 == 1:
        action_id += 100
    if seg_action3 == 1:
        action_id += 10
    if seg_action67 == 1:
        action_id += 1
    
    return str(action_id).zfill(4)

def lib_action_prio(action_id: str):
    "Récupérer le libellé d'action priorisé"
    lib_action = ""
    if action_id[3] == '1':
        lib_action = "Bonus fidélité"
    if action_id[2] == '1':
        lib_action = "Montée de produit"
    if action_id[1] == '1':
        lib_action = "Montée de produit"
    if action_id[0] == '1':
        lib_action = "Montée de formule"
    return lib_action
    
def _tirage_contrats(dataframe: pd.DataFrame, nbcontrats: int):
    """ Tirage des nbcontrats avec le score de fragilité le plus élevés"""
    score_sorted = sorted(list(dataframe["score_model"]), reverse = True)
    seuil_score = score_sorted[nbcontrats-1]
    
    dataframe["is_list"] = dataframe.apply(lambda x: x["score_model"] >= seuil_score, axis=1)
    dataframe = dataframe[dataframe["is_list"] == True]
    
    return dataframe

def _tirage_aleatoire_temoin(dataframe: pd.DataFrame, nbcontrats_temoin: int):
    """ Tirage aléatoire des contrats témoin dans un dataframe de contrats à contacter"""
    # Générer un flag pour la population témoin
    echantillon_df = dataframe.sample(n = nbcontrats_temoin, random_state = 1)
    id_set = set(echantillon_df["identifiant"])
    dataframe["temoin"] = dataframe.apply(lambda x: x["identifiant"] in id_set, axis=1)
    
    return dataframe

def filter_vague(dataframe: pd.DataFrame, date_extraction: str):
    """ Filtre pour récupérer les contrats à contacter sur une vague"""
    
    # Initialisation du nombre de contrats
    nbcontrats_contact = 17000
    nbcontrats_temoin = 800
    nbcontrats = nbcontrats_contact + nbcontrats_temoin
    nbcontrats_restants = nbcontrats
    logger.info('Nombre de contrats total à produire: {:,}'.format(nbcontrats).replace(',', ' ')) 
    logger.info('-- Nombre de contrats dispo au démarrage du filtrage: {:,}'.format(len(dataframe.index)).replace(',', ' '))
    
    # Prendre des contrats qui n'ont pas déjà été contacté
    filtered_df = dataframe[dataframe["Contact"] == "-"]
    logger.info('Nombre de contrats dispo suite au filtre CONTACT: {:,}'.format(len(filtered_df.index)).replace(',', ' '))
    
    # Prendre des contrats qui n'ont pas déjà été contacté: A voir en fonction de l'action en face
    filtered_df = filtered_df[filtered_df["Presence_CC"] == "O"]
    logger.info('Nombre de contrats dispo suite au filtre Secteur blanc: {:,}'.format(len(filtered_df.index)).replace(',', ' '))
    
    # Sélection des contrats à envoyer: Ceux pour qui on veut pousser de la GAV ou du SANTEIS
    #filtered_df = filtered_df[filtered_df.proposition.str.contains('GAV') | filtered_df.proposition.str.contains('SANTEIS')]
    cible_proposition_set = {'GAV','GAV, SANTEIS','SANTEIS', 'SERENIVIE','SERENIVIE, GAV','SERENIVIE, GAV, SANTEIS', 'SERENIVIE, SANTEIS'}
    
    filtered_df = filtered_df[filtered_df.proposition.isin(cible_proposition_set)]
    logger.info('Nombre de contrats dispo suite au filtre GAV/SANTEIS/SERENIVIE: {:,}'.format(len(filtered_df.index)).replace(',', ' '))

    # Filtre de base:
    filtered_df = filtered_df[filtered_df["age_assure"] <= 80] # Filtre sur l'age
    logger.info('Nombre de contrats dispo suite au filtre AGE: {:,}'.format(len(filtered_df.index)).replace(',', ' '))
    
    filtered_df = filtered_df.head(nbcontrats)
    logger.info('Nombre de contrats piochés: {:,}'.format(len(filtered_df.index)).replace(',', ' '))
    
    """filtered_df = dataframe[dataframe["score_fragilite"] >= 7] # Contrat fragile
    
    filtered_df = filtered_df[filtered_df["anciennete_contrat_mois"] > 6] # Filtre sur l'ancienneté du contrat minimale

    # Filtre pour avoir des contrats qui n'ont pas prévu de résilier avant 1 mois 
    filtered_df= filtered_df[(np.isnan(filtered_df["nb_mois_depuis_resiliation"])) 
                             | (filtered_df["nb_mois_depuis_resiliation"] < -1)]
    

    # Filtrer par action avec 2 options:
    # - Des contrats avec action 1,2 ou 3 et un CC présent
    # - Des contrats avec une action 6 ou 7
    filtered_df = filtered_df[((filtered_df["actions_123"] > 0) & (filtered_df["Presence_CC"] == "O")) 
                              | (filtered_df["actions_67"] > 0)]
    
    # Récupération du mois d'extraction (Exemple: Si 20211231: On récupère le '12')
    month_extraction = int(date_extraction[-4:-2])
    
    # Récupération des contrats avec un mois d'anniversaire à M + 4:
    # Si nous sommes en Janvier: On lance le script avec 20211231
    # On fait donc l'opération +5 pour récupérer le M + 4
    month_contrat = (month_extraction + 5)%12
    
    # Filtre sur les contrats à M + 4
    filtered1_df = filtered_df[filtered_df["mois_anniversaire"] == month_contrat]
    nbline = len(filtered1_df.index)
    logger.info('Nombre de contrats pour le mois {}: {:,}'.format(month_contrat, nbline).replace(',', ' '))    

    if nbline <= nbcontrats_restants:
        output_filtered_df = _tirage_contrats(filtered1_df, nbline)
        nbcontrats_restants = nbcontrats_restants - nbline
    else:
        output_filtered_df = _tirage_contrats(filtered1_df, nbcontrats_restants)
        nbcontrats_restants = 0
    logger.info('Nombre de contrats restants à produire: {:,}'.format(nbcontrats_restants).replace(',', ' '))   
            
    if nbcontrats_restants > 0: # Gestion des contrats de Janvier
        # Filtre sur les contrats de Janvier avec au moins 6 mois d'ancienneté
        filtered2_df = filtered_df[filtered_df["mois_anniversaire"] == 1]
        nbline = len(filtered2_df.index)
        logger.info('Nombre de contrats pour le mois de Janvier: {:,}'.format(nbline).replace(',', ' '))

        if nbline <=  nbcontrats_restants:
            output_filtered_df = pd.concat([output_filtered_df,_tirage_contrats(filtered2_df, nbline)])
            nbcontrats_restants = nbcontrats_restants - nbline
        else:
            output_filtered_df = pd.concat([output_filtered_df,_tirage_contrats(filtered2_df, nbcontrats_restants)])
            nbcontrats_restants = 0
        logger.info('Nombre de contrats restants à produire: {:,}'.format(nbcontrats_restants).replace(',', ' ')) 
"""
    output_filtered_df = _tirage_aleatoire_temoin(filtered_df, nbcontrats_temoin)
    logger.info('Nombre de contrats total au final: {:,}'.format(len(output_filtered_df.index)).replace(',', ' '))  
    
    return output_filtered_df