#!/usr/bin/env python3
# coding: utf-8

## Utils - Fonction utils pour Spark
# Auteurs : Guillaume PLANTIVEAU
# Date : 19/04/2021

from pyspark.sql import SparkSession

from pyspark.mllib.evaluation import RegressionMetrics
import logging

# Get logger
logger = logging.getLogger(__name__)

def get_spark_builder():
    '''
    Fonction pour récupérer le Spark Builder
    Args:
    Raises:
    Returns:
        builder: Le builder
    '''
    builder = SparkSession.builder\
                .appName ("Sante_Indiv")\
                .config ( "spark.python.worker.reuse", "true" )\
                .config ( "spark.task.maxFailures", "8" )\
                .config ( "spark.scheduler.mode", "FAIR" )\
                .config ( "spark.dynamicAllocation.enabled", "true" )\
                .config ( "spark.ui.showConsoleProgress", "false" )\
                .config ( "spark.default.parallelism", "320" )\
                .config ( "spark.sql.shuffle.partions", "320" )\
                .config ( "spark.broadcast.compress", "true" )\
                .config ( "spark.kryoserializer.buffer.max", "512" )\
                .config ( "spark.dynamicAllocation.maxExecutors", "16" )\
                .config ( 'spark.sql.session.timeZone', 'Etc/UTC' )\
                .config("spark.archives", "../venv_sante_indiv.tar.gz#environment" )\
                .config('spark.network.timeout','3h')
#                .config("spark.archives", "../venv_sante_indiv.tar.gz#environment" )\

    logger.info("Récupération du Spark Builder OK")
    return builder

def retrieve_metrics(dataframe, model, actual_target):
    '''Fonction pour récupérer les métriques d'un modèle en prenant en input un dataframe et un modèle

    Args:
        dataframe (Spark dataframe): Le dataframe en input
        model: Le modèle
        actual_target: La véritable valeur de l'output
    Raises:
        TODO
    Returns:
        metrics_list (list): Liste des métriques au format (Nom de la métrique, Valeur de la métrique)
    '''
    # Création d'un RDD pour avoir les valeurs prédites et réelles
    pred_actual_rdd = dataframe.rdd.map(lambda p: (float(p.prediction), float(p[actual_target])))
    
    # Récupération de l'objet métrique
    metrics = RegressionMetrics(pred_actual_rdd)
    
    # Récupération des étriques
    mse = "{:.2f}".format(metrics.meanSquaredError)
    rmse = "{:.2f}".format(metrics.rootMeanSquaredError)
    r2 = "{:.2f}".format(metrics.r2)
    mae = "{:.2f}".format(metrics.meanAbsoluteError)
    ev = "{:.2f}".format(metrics.explainedVariance)
    
    metrics_list = [("MSE",mse), ("RMSE", rmse), ("R-squared", r2), ("MAE", mae), ("Explained variance", ev)]
    
    return metrics_list

def retrieve_rmse(dataframe, model, actual_target):
    '''Fonction pour renvoyer le RMSE du modèle sur un dataframe

    Args:
        dataframe (Spark dataframe): Le dataframe en input
        model: Le modèle
        actual_target: La véritable valeur de l'output
    Raises:
        TODO
    Returns:
        metrics_list (list): Liste des métriques au format (Nom de la métrique, Valeur de la métrique)
    '''
    # Création d'un RDD pour avoir les valeurs prédites et réelles
    pred_actual_rdd = dataframe.rdd.map(lambda p: (float(p.prediction), float(p[actual_target])))
    
    # Récupération de l'objet métrique
    metrics = RegressionMetrics(pred_actual_rdd)

    return metrics.rootMeanSquaredError