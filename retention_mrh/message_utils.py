#!/usr/bin/env python3
# coding: utf-8

## Utils - Fonction utils
# Auteurs : Guillaume PLANTIVEAU
# Date : 27/01/2022

import pandas as pd

def write_message1(autres_produits_detenus, raisons_fragilite):
    """ Génération du message 1 de constat: Les produits détenus et les raisons de fragilité """
    
    if autres_produits_detenus == '':
        message = "Le client ne détient que de la MRH"
    else:
        autres_produits_detenus = autres_produits_detenus.split(',')
        message = "En plus de la MRH, le client détient également le(s) produit(s) suivant(s) : "
        message += autres_produits_detenus[0]
        for i in autres_produits_detenus[1:]:
            message += " / " + i
    
    message += ". "
    
    # Raison fragilité
    if raisons_fragilite == '[]':
        message += "Le risque de résiliation du client est dans la moyenne"
    else:
        raisons_fragilite = raisons_fragilite.replace('[', '').replace(']', '').replace('"', '').replace("'", '').split(',')
        message += "Le client est jugé fragile car : "
        message += raisons_fragilite[0].replace("le client", "il")
        for i in raisons_fragilite[1:]:
            message += " / " + i.replace("le client", "il")
    message += "."        
    return message


def write_message2(proposition: str, libsituationfamiliale, SITFAM_2, presence_CC,
                   proposition_formule_santeis, proposition_tarif_santeis: float,
                   proposition_formule_serenivie, proposition_tarif_serenivie: float):
    """ Génération du message 2: La proposition"""
    
    if proposition == '':
        message = "Le client n'a a priori pas d'appétence particulière à un produit"
    else:
        proposition_list = proposition.split(',')
        message = "Le client a le profil type des clients appétents à "
        message += proposition_list[0]
        for i in proposition_list[1:]:
            message += " / " + i
            
    message += ". "
    
    # à ce niveau, on sait qu'il n'y aura que des clients appétents à la GAV UNIQUEMENT (car on n'a pas les autres argumentaires)
    # donc la suite du message ne marche que pour la première liste, on l'enrichira après avec les autres argumentaires & tarifs
    
    if presence_CC == "N":
        message += "Néanmoins, il n'est rattaché a priori à aucun CC"
    else:
        if 'GAV' in proposition:
            # Proposition de GAV
            message += "Il est conseillé de proposer au client une GAV " + SITFAM_2 + " car sa situation familiale est " + libsituationfamiliale + ", "

            tarif_GAV = {"Solo 5%": "146.97",
                         "Solo 30%": "120.27",
                         "Famille 5%": "267.03",
                         "Famille 30%": "213.61"
                        }
            message += "le tarif proposé est: "
            message += tarif_GAV[SITFAM_2+" 5%"] + " EUR pour une IP de 5%"
            message += " / "
            message += tarif_GAV[SITFAM_2+" 30%"] + " EUR pour une IP de 30%. "
            
        if "SANTEIS" in proposition:
            # Proposition de SANTEIS
            if proposition_formule_santeis == "SATEI-FM03":
                formule = "Formule 3 CLASSIQUE"
            if proposition_formule_santeis == "SATEI-FS03":
                formule = "Formule 3 SENIOR"
            
            message += "Il est conseillé de proposer au client un contrat SANTEIS (" + formule + "), "
            message += "le tarif proposé est: "
            message += str(proposition_tarif_santeis)
            message += " EUR. "
            
        if "SERENIVIE" in proposition:
            # Proposition de SERENIVIE
            if proposition_formule_serenivie != "":
                if proposition_formule_serenivie == "PRESTIGE":
                    formule = "Paiement VIAGER - Formule PRESTIGE"
                if proposition_formule_serenivie == "REFERENCE":
                    formule = "Paiement VIAGER - Formule REFERENCE"

                message += "Il est conseillé de proposer au client un contrat SERENIVIE (" + formule + "), "
                message += "le tarif proposé est environ: "
                message += str(proposition_tarif_serenivie)
                message += " EUR. "
            
    return message