#!/usr/bin/env python3
# coding: utf-8

## Utils - Fonction utils
# Auteurs : Guillaume PLANTIVEAU
# Date : 16/03/2021

import logging

import numpy as np
import pandas as pd

from pyspark.sql.functions import col
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

from pyspark.sql.window import Window

import os
from datetime import datetime

import math

# Get logger
logger = logging.getLogger(__name__)

def write_to_monitor_file(monitoring_path : str, params : dict, list_var: list, training_metrics : list, dev_metrics_list: list, test_metrics_list: list):
    '''Fonction pour écrire dans le fichier de monitoring

    Args:
        monitoring_path: Le path du fichier
        params: Le/les paramètres du modèle
        training_metrics: La liste des métriques de Train sous forme ("Nom de la métrique", Valeur de la métrique)
        dev_metrics_list: La liste des métriques de Dev sous forme ("Nom de la métrique", Valeur de la métrique)
        test_metrics_list: La liste des métriques de Test sous forme ("Nom de la métrique", Valeur de la métrique)
    Raises:
        TODO
    Returns:
        True if exists. Else False
    '''
    # Stockage des résultats    
    # Check si on a besoin d'écrire un header
    needHeader = False
    if (not is_file(monitoring_path)):
        needHeader = True
        
    with open(monitoring_path, 'a+') as f:
        if needHeader:
            f.write("Date;Variables;Params")
            for (m,v) in training_metrics:
                f.write(";"+"Training_"+m)
            for (m,v) in dev_metrics_list:
                f.write(";"+"Dev_"+m)
            for (m,v) in test_metrics_list:
                f.write(";"+"Test_"+m)
            f.write("\r\n")
                
        f.write(datetime.now().strftime("%Y_%m_%d-%H_%M_%S") + ";")
        f.write('/'.join(list_var) + ";")
        f.write(str(params))
        for (m,v) in training_metrics:
            f.write(";"+v)
        for (m,v) in dev_metrics_list:
            f.write(";"+v)
        for (m,v) in test_metrics_list:
            f.write(";"+v)
        f.write("\r\n")
                
def is_file(filename : str = None):
    '''Fonction pour savoir si un fichier existe

    Args:
        filename: Le path du fichier
    Raises:
        TODO
    Returns:
        True if exists. Else False
    '''
    return os.path.isfile(filename)
    
def get_monitoring_path(model_name = None):
    '''Fonction pour récupérer le path du monitoring du modèle indiqué en paramètre

    Args:
        model_name: Le nom du modèle
    Raises:
        TODO
    Returns:
        monitoring_path: Le path du fihier de log
    '''
    # Dir Path
    dir_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'monitoring')
    
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path) 
        
    # Nom du fichier
    file_name = model_name
    # Construction du path
    monitoring_path = os.path.join(dir_path, file_name)
    
    return monitoring_path

def get_log_path(name = None):
    '''Fonction pour récupérer le path du fihier de log

    Args:
        name: Le nom du Main appelant
    Raises:
        TODO
    Returns:
        log_path: Le path du fihier de log
    '''
    # Dir Path
    dir_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'logs')
    if not os.path.isdir(dir_path):
        os.mkdir(dir_path) 
        
    # Nom du fichier
    file_name = datetime.now().strftime("%Y_%m_%d-%H_%M_%S_"+name)
    
    # Construction du path
    log_path = os.path.join(dir_path, file_name)
    
    return log_path
    
def generate_dataframe(spark, object_name, database, table_name, var_list, filters = None, c_filters = None, columns_to_rename = None, windowing_param = None):
    '''Fonction pour générer un dataframe relatif à une table

    Args:
        spark: La session Spark
        object_name: Le nom simple de ce qu'on veut générer ('contrat', 'prime' etc)
        database: Le nom de la database ) requêter
        table_name: Le nom complet de la table qu'on veut récupérer
        var_list: La liste des variables à garder dans la table
        filters: Les filtres à appliquer à la requête
        c_filters: Les filtres "Contains" à appliquer à la requête
        columns_to_rename: Liste des colonnes à renommer avec le suffixe du champ object_name
        windowing_param: Si un filtre est à faire sur un champ incrémental
    Raises:
        TODO
    Returns:
        pyspark.sql.dataframe.DataFrame
    '''

    # Construction de la query
    query = "SELECT * FROM " + database + "." + table_name
    # Mise en place des filtres
    if filters:
        query += " WHERE "
        for (c,v) in filters.items(): query += c + " = '" + v + "' AND "
        query = query[:-5]
    
    # Création du dataframe
    dataframe = spark.sql(query)
    
    if c_filters:
        for(c,v) in c_filters.items(): dataframe = dataframe.filter(col(c).contains(v))
            
    # Renommage des colonnes
    for c in columns_to_rename: dataframe = dataframe.withColumnRenamed(c,c+"_"+object_name)
        
    # Filtrage sur une valeur incrémental de colonnes
    if windowing_param:
        w = Window.partitionBy(windowing_param['partitionBy'])
        dataframe = dataframe.withColumn(windowing_param['column']+"_max",f.max(windowing_param['column']).over(w))
        dataframe = dataframe.filter(col(windowing_param['column']+"_max")==col(windowing_param['column']))
        
    # On filtre sur les variables d'interêt
    dataframe = dataframe.select(var_list)
        
    # On ajoute un préfixe à chaque colonne ??
    #df_new = df.select([F.col(c).alias("`"+c+"`") for c in df.columns])
    
    # Little print
    print("✅ %i lignes pour la table %s" % (dataframe.count(),object_name))
    
    return dataframe

def retrieve_table_name_cv(spark, agi, table_type):
    '''
    Fonction pour récupérer le nom de la table du collecteur VIE
    Args:
        spark: La session Spark
        agi (str): Nom de l'AGI
        table_name: Nom ou portion du nom de la table
    Raises:
        IOError: Si on ne trouve pas exactement 1 table correspondant aux critères
    Returns:
        str: Le nom complet de la table
    '''

    table_name_list = (spark.sql("show tables in PRD_CV")
                       .filter(col("tableName").contains(table_type))
                       .filter(col("tableName").contains(agi))
                       .toPandas()["tableName"].tolist())

    if len(table_name_list) >= 1:
        if len(table_name_list) > 1:
            #raise FileNotFoundError("Le nom est trouvé en plusieurs exemplaires")
            raise IOError("Le nom %s est trouvé en plusieurs exemplaires" % table_type)
        return table_name_list[0]
    else:
        raise IOError("Le nom %s est inexistant" % table_type)
        
def get_monthid_from_dateref(date_ref: str):
    "Récupérer un month_id à partir d'une date_ref au format YYYYMMDD"
    first_date, date_ref_list = generate_date_ref_list()
    month_ref = ""
    for (month_id, date) in date_ref_list:
        if date == date_ref:
            month_ref = month_id
    return month_ref

def generate_date_ref_list():
    
    start_year = 2016 # On commence au 01/01/2016
    first_date = "20160101"
    month_ref_range = range(12,7*12,3) # 12 = Mois de décembre 2017, 15 = Mois de Mars 2018 etc. 7*12 => On va jusqu'à 09/2022
    date_ref_list = []
    year = start_year # Année de démarrage: Exemple: 2016
    i = 3 # Trimestre de démarrage de 1 à 4 (Exemple pour i = 3: C'est comme si on venait de finir le 3ème trimestre)
    # Création de la liste (month_id, date_ref) pour avoir la date de référence correspondante au month_id
    for month_id in month_ref_range:
        i += 1
        if i > 4:
            i = 1
            year += 1

        month = month_id%12
        if month == 0:
            month = 12

        month = str(month).zfill(2)
        day = '31'
        if (month == '06') | (month == '09'):
            day = '30'
        date_ref = str(year) + month + day
        date_ref_list.append((month_id, date_ref))
        
    return first_date, date_ref_list

def generate_date_extraction_list(firstdate: str, nbmonth: int):
    '''
    Générer une liste de nbmonth dates consécutives.
    Exemple: Si firstdate = 20190301 et nbmonth = 3, renvoit:
    20190301 / 20190401 / 20190501
    Args:
        firstdate (str): La première date au format YYYMM01
        nbmonth (int): Le nombre de mois à renvoyer
    Raises:
        TODO
    Returns:
        date_extraction_list (list): Liste des dates
    '''

    date_extraction_list = [firstdate]
    
    start_year = int(firstdate[:4])
    start_month = int(firstdate[4:6])
    
    for month_id in range(1,nbmonth):

        inc_year = (start_month + month_id - 1)//12
        year = str(start_year + inc_year)
    
        month = (start_month + month_id -1 )%12 + 1
        month = str(month).zfill(2)
        
        date_extraction = str(year) + month + "01"
        
        date_extraction_list.append(date_extraction)

    return date_extraction_list

def convert_dataframe_x_y(spark_df, list_var_cat, list_var_num, list_label):
    '''
    Fonction pour récupérer le X,y utiles à l'input d'un algorithme sklearn
    Args:
        spark_df: Dataframe Spark
        list_var_cat: Liste des variables catégorielles
        list_var_num: Liste des variables numériques
        list_label: Liste des labels
    Raises:
        TODO
    Returns:
        X: Les données en input
        y: Le label à prédire
    '''
    list_var_input = list_var_cat.copy()
    list_var_input.extend(list_var_num)

    list_var_total = list_var_input.copy()
    list_var_total.extend(list_label)
    
    pd_df = spark_df.select(list_var_total).toPandas()

    pd_df[list_var_num] = pd_df[list_var_num].fillna(0)
    pd_df[list_var_cat] = pd_df[list_var_cat].fillna("NULL")
    
    y = np.ravel(pd_df[list_label])
    X = pd_df[list_var_input]

    return X,y

def convert_code_regime(code_regime: str):
    '''Fonction pour convertir un code régime avec un référentiel maison (STEA/AM)
    AM = Alsace-Moselle
    STEA: Salarié / TNS / Exploitant Agricole

    Args:
        code_regime (str): Code régime
    Raises:
        None
    Returns:
        coderegime_output (str): Code régime modifié
    '''
    coderegime_output = "STEA"
    if code_regime in ["Alsace Moselle",3]:
        coderegime_output = "AM"
    return coderegime_output

def convert_cadre_legal(cadre_legal: str):
    '''Fonction pour convertir un cadre légal avec un référentiel maison (NF / Fonctionnaire)
    NF = Non fonctionnaire
    Fonctionnaire: Fonctionnaire

    Args:
        cadre_legal (str): Cadre légal
    Raises:
        None
    Returns:
        cadre_legal_output (str): Cadre légal modifié
    '''
    cadre_legal_output = "NF"
    if cadre_legal in ["Fonctionnaire", 3]:
        cadre_legal_output = "F"
    return cadre_legal_output

def arrondir_montant(montant: float, tranche: int):
    '''Fonction pour arrondir un montant

    Args:
        montant (float): Montant à arrondir
        tranche (int): Facteur d'arrondi (Si arr = 50, on arrondi par tranche de 50)
    Raises:
        None
    Returns:
        montant_arrondi (float): Montant arrondi
    '''
    if (not montant) | math.isnan(montant):
        montant = 0
    div = montant//tranche
    montant_arrondi = div*tranche
    return montant_arrondi

def get_listcontrat_filepath(date_extraction : str, index : int, vague: str = None):
    '''Fonction pour récupérer le nom de fichier de la liste de contrats

    Args:
        date_extraction (str): Date d'extraction des contrats
        index (str): Index du fichier
        vague (str): Identifiant de la vague
    Raises:
        None
    Returns:
        filepath (str): Le file path
    '''
    filepath = "../data/work_list_contrats_"
    filepath += date_extraction
    if vague:
        filepath += "_"
        filepath += vague
    filepath += "_"
    filepath += str(index)
    filepath += '.csv'
    
    return filepath

def get_flag_id_vague(vague : str):
    '''Fonction pour récupérer le nom de la feature pour la liste

    Args:
        vague (str): Identifiant de la vague
    Raises:
        None
    Returns:
        field_name (str): Le nom de la colonne
    '''
    field_name = "is_list_"
    field_name += vague
    
    return field_name

def is_dtpartition_ok(dt_partition: str = None):
    ''' Fonction pour valider le format d\'une date de partition: yyyymm'''
    
    statut = True
    try:
        datetime.strptime(dt_partition, '%Y%m')
    except ValueError:
        statut = False
    except TypeError:
        statut = False
    return statut

def is_dateextraction_ok(date_extraction: str = None):
    ''' Fonction pour valider le format d\'une date d'extraction': yyyymmdd'''
    
    statut = True
    if date_extraction is not None:
        if len(date_extraction) != 8:
            statut = False
    try:
        datetime.strptime(date_extraction, '%Y%m%d')
    except ValueError:
        statut = False
    except TypeError:
        statut = False
    return statut      
    
def convert_dataframe_for_output(spark_df, list_var_cat, list_var_num, list_label, list_sup):
    '''Fonction pour convertir un dataframe Spark en 2 dataframe Pandas:
    - Un avec les paramètres correspondant au fit du modèle
    - Un avec l'ensemble des données que l'on souhaite afficher

    Args:
        spark_df: Dataframe Spark
        list_var_cat: Liste des variables catégorielles
        list_var_num: Liste des variables numériques
        list_label: Liste des labels
        list_sup: Liste des variables supplémentaires que l'on souhaite afficher
    Raises:
        None
    Returns:
        X - Dataframe pandas pour le predict
        pd_df - Dataframe pandas pour la sortie en liste
    '''
    list_var_input = list_var_cat.copy()
    list_var_input.extend(list_var_num)

    list_var_total = list_var_input.copy()
    list_var_total.extend(list_sup)
    
    pd_df = spark_df.select(list_var_total).toPandas()

    pd_df[list_var_num] = pd_df[list_var_num].fillna(0)
    pd_df[list_var_cat] = pd_df[list_var_cat].fillna("NULL")

    X = pd_df[list_var_input]

    return X, pd_df

def score_prediction_on_10(y_predict_proba: list, seuil_list: list):
    '''Fonction pour scorer chaque élément d'une liste sur 10 en se basant sur:
    - Un vecteur de probabilité
    - Le seuil à appliquer

    Args:
        spary_predict_proba (list): La liste des probabilités en sortie de modèle
        seuil_list: La liste des seuils, triée par ordre décroissant
    Raises:
        None
    Returns:
        score_list (list) - La liste des scores
    '''
    
    score_list = []
    for y_proba in y_predict_proba:
        for idx, seuil in enumerate(seuil_list):
            if y_proba >= seuil:
                score_list.append(10-idx)
                break
    return score_list