#!/usr/bin/env python3
# coding: utf-8

## Extraction des données Santeis
# Auteurs : Guillaume PLANTIVEAU
# Date : 16/03/2021
#
# Commande pour lancer le script:
# %run generer_stats_resiliation_v2 -p 202201 -s 201901 -e 202201

import argparse
import logging

from pyspark.sql import functions as f
from pyspark.sql.functions import to_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.types import StringType
from pyspark.sql.types import ArrayType
from pyspark.sql.types import StructType
from datetime import datetime
from pyspark.sql.functions import explode
from pyspark.sql.functions import col
from pyspark.sql.functions import when
from pyspark.sql.functions import lpad
from pyspark.sql.functions import concat
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import lit
from pyspark.sql.functions import countDistinct
from pyspark.sql import Window
import pandas as pd
import pickle as pkl
pd.set_option('display.max_columns', 50)
pd.set_option('display.max_rows', 100)

# import santeindiv.spark_utils as spark_utils
# import santeindiv.utils as utils

import spark_utils
import utils

# Get logger
logger = logging.getLogger(__name__) 

def load_data(spark,dt_partition):
    """
    Elle permet de charger les différentes tables (RCE, Collecteur VIE), de selectionner les colonnes et de réaliser les jointures.
    
    Parameters:
        dt_partition: la partition à extraire
    
    Returns:
        spark Dataframe
    """
    ##### liste des colonnes à sélectionner 
    col_to_select = ['polcodc',
                     'uidpersgen',
                     'identifiantrce',
                     'contratactif',
                     'famille_cible',
                     'identifiant',
                     'date_effet',
                     'date_resil',
                     'societedugroupe',
                     'systemeorigine',
                     'libcsp',
                     'top_mar_part', 
                     'top_mar_prope',
                     'top_mar_ent',
                     'mois_entree',
                     'mois_sortie',
                     'annee_sortie',
                     'libreseaucalcule',
                     'precisionsystemeorigine',
                     'year',
                     'libformejuridique',
                     'formejuridique',
                     'cdprodcdgauer',
                     'rce_lib_produit',
                     'lib_produit',
                     'dt_partition',
                     col("agepersphysique").alias('age')
                    ]
    ##### Chargement des données de la table socle
    ctr=(spark.table("pub_ta_global.met_r_rce_t_socle_client_contrat")
         # filtre sur les dates de partition renseignées
         .filter(col('dt_partition').isin(dt_partition) &
                 # filtre sur les réseaux
                 col('libreseaucalcule').isin('AGENT', 'COURTIER', 'SALARIE GPROX'))
         # filtre sur la famille de produit
         .filter(col('famille_cible')=='SANTE INDIVIDUELLE')
         # on récupère que les clients sinon doublon des contrats
         .filter((col('top_client')==1) &
                 # on supprime les affiliés et les GPMA sinon doublon des contrats
                 ((col('indicateurgpma')!=2)|col('indicateurgpma').isNull()))
         # définition de la police
         .withColumn('polcodc', when(col('societedugroupe').isin(['07', '59', '01', '81', '76', '57']), lpad(col('identifiant'), 11, '0')).otherwise(col('identifiant')))
        )
    
    ##### Chargement des dates d'éffet des contrats
    anc = spark.sql("""SELECT 
                    cd_cie as societedugroupe, 
                    num_ctrt as polcodc,
                    cd_appli as systemeorigine,
                    dt_deb_valdt_ctrt 
                    
                    FROM prd_brute.b_t_bmg_t_inf_ctrt
                    
                    WHERE (dt_partition={dt_partition})
                    """.format(dt_partition = dt_partition)
                   )
    
    ##### jointure des deux tables
    ctr = (ctr.join(anc, ['societedugroupe','polcodc', 'systemeorigine'], 'left')
           # si periodevaliditectrstartdate est null remplacé par dt_deb_valdt_ctrt
           .withColumn('periodevaliditectrstartdate', when(col('periodevaliditectrstartdate').isNull(), col('dt_deb_valdt_ctrt'))
                       .otherwise(col('periodevaliditectrstartdate')))
           # recupérer l'année de la date d'effet du contrat
           .withColumn('year', year('periodevaliditectrstartdate'))
           # recupérer le mois de la date d'effet du contrat
           .withColumn('month', month('periodevaliditectrstartdate'))
           # coding de la mensuelle d'effet au format de AAAAMM
           .withColumn('mois_entree', concat(col('year'), lpad(col('month'), 2, '0')).cast(StringType()))
           # recupérer l'année de la date de fin du contrat
           .withColumn('annee_sortie', year('periodevaliditectrenddate'))
           # recupérer le mois de la date de fin du contrat
           .withColumn('month', month('periodevaliditectrenddate'))
           # coding de la mensuelle de fin de contrat au format de AAAAMM
           .withColumn('mois_sortie', concat(col('annee_sortie'), lpad(col('month'), 2, '0')).cast(StringType()))
           # renommer la date de début
           .withColumnRenamed('periodevaliditectrstartdate', "date_effet")
           # renommer la date de fin
           .withColumnRenamed('periodevaliditectrenddate', "date_resil")
           .select(*col_to_select)
           # defenition de l'id contrat unique
           .withColumn('idCtr', concat(col('identifiant'), lit('_'), col('societedugroupe'), lit('_'), col('systemeorigine')))
          )
    
    ##### chargement referentiel produit FUE
    ref_fue = spark.sql("""SELECT
                        ciecodc as societedugroupe,
                        catcodc as cdprodcdgauer,
                        rlpcodc,
                        rlplibl
                        
                        FROM prd_brute.b_i_tcga_i_dmtaxeprod 
                        
                        WHERE (dt_partition={dt_partition})
                        """.format(dt_partition = dt_partition)
                       )
    # jointure avec la table contrats
    ctr = ctr.join(ref_fue, ['societedugroupe', 'cdprodcdgauer'], 'left')
    
    list_var_contrat = ["cdcompagnie",
                        "cdappli",
                        "cdproduit",
                        "nucontra",
                        "cdciegen",
                        "idintermagi",
                        "idinterm",
                        "dtsaisie",
                        "idsouscragi",
                        "dtsouscr",
                        "dteffet",
                        "dtterme",
                        "cdtypterme",
                        "cdsituat",
                        "dtsituat",
                        "lbsouscr"
                       ]
    #colonnes table produit
    list_var_produit = ['cdcompagnie',
                        'cdappli',
                        'cdproduit',
                        'lbproduit',
                        'lbcommercial',
                        'cdcollindiv'
                       ]
    
    ##### chargement de la table des contrats du collecteur vie
    w = Window.partitionBy('nucontra')
    contrat_df = (spark.sql("select * from prd_cv.b_v_cv_t_gpas_ttcontrat where (dt_partition={dt_partition})".format(dt_partition = dt_partition))
                  # pour chaque contrat lui associé sa valeur max de cdlotimport
                  .withColumn("codelotimport_max", f.max('cdlotimport').over(w))
                  # filtrer sur les lignes correspondant à la valeur maximale de cdlotimport
                  .filter((col("codelotimport_max")==col('cdlotimport')))
                  # selectionner les variables à récupérer
                  .select(list_var_contrat)
                 )
    
    ##### chargement de la table des produits du collecteur vie
    produit_df = (spark.sql("select * from prd_cv.b_v_cv_t_gpas_ttproduit where (dt_partition={dt_partition})".format(dt_partition = dt_partition))
                  # filtre sur les produits individuelle
                  .filter((col("cdcollindiv")=="I"))
                  # selectionner les variables à récupérer
                  .select(list_var_produit)
                 )
    ##### jointure des deux tables 
    contrat_df = contrat_df.join(produit_df,on=['cdcompagnie', 'cdappli',"cdproduit"], how="inner").distinct()
    
    # selectionner les variables d'interet et les renommer
    contrat_df = contrat_df.select(col("nucontra").alias('identifiant'),
                                   col("cdciegen").alias('societedugroupe'), 
                                   col('cdproduit').alias('cdproduit_cv'), 
                                   col('lbproduit').alias('lbproduit_cv'),
                                   col('lbcommercial').alias('lbcommercial_cv'))
    
    # jointure avec la table des contrats
    ctr = ctr.join(contrat_df, ['identifiant', 'societedugroupe'], 'left')

    return ctr

def add_top_sante(df):
    """
    Elle permet de flagger les différents périmétres produit
    
    Parameters:
        df : data frame contenant les contrats et leurs informations
    Returns:
        spark Dataframe
    """
    return (df
            .withColumn('top_santeis_senior', when((col('lbproduit_cv').rlike('SANTéIS')|col('lib_produit').rlike('SANTEIS'))
                                                   & (col('lbproduit_cv').rlike('FORMULE 3S|FORMULE 3S+|FORMULE 5S')) , 1).otherwise(0))
            .withColumn('top_santeis_clas', when((col('lbproduit_cv').rlike('SANTéIS')|col('lib_produit').rlike('SANTEIS'))
                                                 & (col('top_santeis_senior')==0), 1).otherwise(0))
            .withColumn('top_tns', when(col('rlpcodc')=='A246', 1).otherwise(0))
            .withColumn('top_part', when(col('rlpcodc')=='A241', 1).otherwise(0))
            .withColumn('top_ideo', when(col('cdprodcdgauer')=='460', 1).otherwise(0))
            .withColumn('top_autre', when((col('top_santeis_senior')==0)&(col('top_santeis_clas')==0)
                                          & (col('top_tns')==0)&(col('top_part')==0)
                                          & (col('top_ideo')==0), 1).otherwise(0))
           )



def generate_month_list(start, end):
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)):
        y, m = divmod(tot_m, 12)
        mlist.append(datetime(y, m+1, 1).strftime("%Y%m"))
    return mlist

def get_liste_mois_actif(date_debut_extraction, date_effet, date_resil, date_fin_extraction):
    start = date_debut_extraction
    end = date_fin_extraction
    
    if (date_resil and (date_resil < date_fin_extraction)):
        end = date_resil
    if (date_effet and (date_debut_extraction < date_effet)):
        start = date_effet
    return generate_month_list(start, end)

get_liste_mois_actif = f.udf(get_liste_mois_actif)

def explode_dataframe(df, date_debut_extraction, date_fin_extraction):
    """
    Elle permet de pivoter le dataframe pour avoir une vision à chaque mois passé 
    
    Parameters:
        df : dataframe contenant les contrats et leurs informations
        date_debut_extraction : Date de début d'extraction (Format yyyyMMdd)
        date_fin_extraction : Date de fin d'extraction  (Format yyyyMMdd)
    Returns:
        spark Dataframe
    """
    
    new_df = df.withColumn("list_mois_actif", get_liste_mois_actif(to_timestamp(f.lit(date_debut_extraction),'yyyyMM'),
                                                             col("date_effet"),
                                                             col("date_resil"),
                                                             to_timestamp(f.lit(date_fin_extraction),'yyyyMM')))
    
    schema = ArrayType(StringType())

    new_df = new_df.withColumn("list_mois_actif", from_json(new_df.list_mois_actif, schema))
     
    col_to_keep = ['uidpersgen',
                   'identifiantrce',
                   'idCtr',
                   'date_effet',
                   'date_resil',
                   'mois_sortie',
                   'annee_sortie',
                   'libreseaucalcule',
                   'top_santeis_senior',
                   'top_santeis_clas',
                   'top_tns',
                   'top_part',
                   'top_ideo']
    
    new_df = new_df.select(*col_to_keep,explode(new_df.list_mois_actif).alias("mois"))

    new_df = new_df.withColumn('annee', f.substring(col('mois'),1,4))
    
    # Supression des contrats "ouverts/fermés"
    new_df = new_df.filter((new_df.date_effet != new_df.date_resil) | (new_df.date_resil.isNull()))
    
    #  Correction des dates d'effet et de résil pour fusionner les contrats qui sont remplacés
    window = Window.partitionBy('idCtr')

    new_df = new_df.withColumn('nan', f.to_date(f.lit("9999-12-01"), 'yyyy-MM-dd'))
    new_df = new_df.withColumn('date_resil', when(new_df.date_resil.isNull(), new_df.nan).otherwise(new_df.date_resil))
    new_df = new_df.withColumn("date_resil", f.max(new_df.date_resil).over(window))
    new_df = new_df.withColumn('annee_sortie', year('date_resil'))
    new_df = new_df.withColumn('mois_sortie', concat(col('annee_sortie'), lpad(month('date_resil'), 2, '0')).cast(StringType()))
    
    return new_df

def compute_stock(df, agg_cols=[]):
    """
    Elle permet de calculer le stock de client et de contrat actif. 
    
    Parameters:
        df : dataframe contenant les contrats et leurs informations
        agg_cols : liste des colonnes sur lesquelles l'agrégation sera faite. si liste vide, retourne le stock par partition
    Returns:
        Pandas Dataframe
    """ 
    
    if (len(agg_cols)==0):
        
        # quand la liste est vide, aggregation sur le mois seulement
        col_to_agg = ['mois']
        # filtre sur les contrats actifs pour calculer le stock
        agg_data = df.groupBy(*col_to_agg).agg(countDistinct('uidpersgen').alias('nbCli'), countDistinct('idCtr').alias('nbCtr'))
        
    else :
        # quand la liste est non vide, aggregation sur le mois et le reseau
        col_to_agg = ['mois', 'libreseaucalcule']
        agg_data = df.select(*col_to_agg).distinct()
        # on parcourt la liste et on calcule le stock par perimètre santé
        for c in agg_cols:
            # filtre sur les contrats actifs et sur la colonne courante pour calculer le stock
            col_data = (df.filter(col(c)==1)
                        .groupBy(*col_to_agg)
                        .agg(countDistinct('uidpersgen').alias('nbCli_'+c), countDistinct('idCtr').alias('nbCtr_'+c))
                       )
            agg_data = agg_data.join(col_data, col_to_agg, 'left')
            
    return agg_data.sort(col_to_agg).toPandas()

def compute_resiliation(df, periode ='mois', agg_cols=[]):
    """
    Elle permet de calculer les résiliations. 
    
    Parameters:
        df: data frame contenant les contrats et leurs informations
        periode: est la periode sur laquelle la résiliation sera calculée. 2 valeurs possible: 'annee' ou 'mois'
        agg_cols: liste des colonnes sur lesquelles l'agrégation sera faite. si liste vide, retourne la résiliation par année total sante ind
    Returns:
        Pandas Dataframe
    """
    
    if periode == 'mois':
        df_resil = df.withColumn('is_mois_resil', when(col('mois') == col("mois_sortie"), 1).otherwise(0))
        df_resil = df_resil.withColumn('uidpersgen_resil', col("uidpersgen") * col("is_mois_resil"))
        df_resil = df_resil.withColumn('idCtr_resil', when(col("is_mois_resil") == 1, col("idCtr")).otherwise(0))
    else: # année
        df_resil = df.withColumn('is_annee_resil', when(col('annee') == col("annee_sortie"), 1).otherwise(0))
        df_resil = df_resil.withColumn('uidpersgen_resil', col("uidpersgen") * col("is_annee_resil"))
        df_resil = df_resil.withColumn('idCtr_resil', when(col("is_annee_resil") == 1, col("idCtr")).otherwise(0))
    
    if (len(agg_cols)==0):
        
        # quand la liste est vide, agregation par annee ou par mois
        col_to_agg = [periode]
        # filtre sur les contrats inactifs pour calculer les résiliations
        agg_data = (df_resil.groupBy(*col_to_agg)
                    # calcul du nombre de clients et de contrats inactifs
                    .agg(countDistinct('uidpersgen_resil').alias('nbCli'), countDistinct('idCtr_resil').alias('nbCtr'))
                   )
        
    else :
        
        # quand la liste n'est pas vide, agregation par annee et reseau ou par mois et reseau
        col_to_agg = [periode, 'libreseaucalcule']
        agg_data = df_resil.select(*col_to_agg).distinct()
        # on parcourt la liste et on calcule les résiliations par perimètre santé
        for c in agg_cols:
            # filtre sur les contrats inactifs et sur la colonne courante pour calculer les résiliations
            col_data = (df_resil.filter(col(c)==1)
                        .groupBy(*col_to_agg)
                        .agg(countDistinct('uidpersgen_resil').alias('nbCli_'+c), countDistinct('idCtr_resil').alias('nbCtr_'+c))
                       )
            agg_data = agg_data.join(col_data, col_to_agg, 'left')
            
    return agg_data.sort(col_to_agg).toPandas()

def main(dt_partition, startdate, enddate):
    '''Fonction pour extraire les statistiques de stock / résiliation de la santé individuelle

    Args:
        dt_partition: La partition à récupérer pour les tables le nécéssitant (au format YYYYMM)
        startdate: Date de début d'extraction au format YYYYMM
        enddate: Date de fin d'extraction au format YYYYMM
    Raises:
        ValueError: Quand le dataframde  sortie est vide, certainement du à une mauvaise dt_partition
    Returns:
        None - Sauvegarde du fichier excel dans le répertoire spécifié
    '''
    spark = spark_utils.get_spark_builder().getOrCreate()
    
    logger.info('------------------------------------------')
    logger.info("Lancement du script: Extraction des statistiques de stock / résiliation de la santé individuelle")
    logger.info('------------------------------------------')
    logger.info('Partition utilisée: %s' % dt_partition)
    logger.info('Date de début d\'extraction: %s' % startdate)
    logger.info('Date de fin d\'extraction: %s' % enddate)
    logger.info('repertoire de stockage du fichier Excel de sortie: output')
    logger.info('')
    
    logger.info('Loading data ...')
    ctr = load_data(spark, int(dt_partition))
    ctr = add_top_sante(ctr)
    exploded_ctr = explode_dataframe(ctr, startdate, enddate)

    logger.info('Computing Stock ...')
    stock_df = compute_stock(exploded_ctr, ['top_santeis_senior', 'top_santeis_clas', 'top_ideo', 'top_tns', 'top_part','top_autre'])
    
    if stock_df.empty:
        raise ValueError("La partition renseigné ne permet pas de récupérer des données")
        
    logger.info('Computing Resil ...')
    resil_annee_df = compute_resiliation(exploded_ctr, periode = 'annee', agg_cols=['top_santeis_senior', 'top_santeis_clas', 'top_ideo', 'top_tns', 'top_part'])
    resil_mois_df = compute_resiliation(exploded_ctr, periode = 'mois', agg_cols=['top_santeis_senior', 'top_santeis_clas', 'top_ideo', 'top_tns', 'top_part'])

    print("Stockage des statistiques sous output")
    file_path = '../output/' + datetime.now().strftime("%Y_%m_%d-%H_%M_%S_"+ "stats_stock_resiliation.xlsx")
    with pd.ExcelWriter(path = file_path) as writer:  
        stock_df.to_excel(writer, sheet_name='STOCK')
        resil_annee_df.to_excel(writer, sheet_name='RESIL ANNEE')
        resil_mois_df.to_excel(writer, sheet_name='RESIL MOIS')
    
    MMYY = '../output/' + datetime.now().strftime("%Y_%m_%d-%H_%M_%S_")
    stock_df.to_pickle(MMYY+'stock_df')
    resil_mois_df.to_pickle(MMYY+'resil_mois_df')
    
    print("✅ Tout marche bien navette")
    logger.info('✅ Tout marche bien navette')

if __name__ == '__main__':
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    logging.basicConfig(filename = utils.get_log_path("GENERER_STATS_RESILIATION"), format='%(asctime)s %(levelname)-8s %(message)s', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--partition', help='Partition à extraire pour les tables concernés (au format YYYYMM)')
    parser.add_argument('-s', '--startdate', help='Date de début d\'extraction au format YYYYMM')
    parser.add_argument('-e', '--enddate', help='Date de fin d\'extraction au format YYYYMM')
    args = parser.parse_args()
    main(dt_partition = args.partition, startdate = args.startdate, enddate = args.enddate)