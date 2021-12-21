from dbManager import DBManager
from celeryWorker import iot_inner
import sys

# from dbManager import DBManager
from back_DB_bridge import Back_DB_bridge
from back_worker_bridge import Back_worker_bridge

# import psycopg2,multiprocessing
from joblib import Parallel, delayed

# db = DBManager(database="data")
r=""
task_cluster = {
    "ml": [
        {"m_code": "0_1", "m_name": "KWh^J"},
        {"m_code": "1_1", "m_name": "KWh^S"},
        {"m_code": "2_1", "m_name": "KWh^M"},
        {"m_code": "3_1", "m_name": "KWh^A"},
        {"m_code": "4_1", "m_name": "VARh^J"},
        {"m_code": "5_1", "m_name": "VARh^S"},
        {"m_code": "6_1", "m_name": "VARh^M"},
        {"m_code": "7_1", "m_name": "VARh^A"},
        {"m_code": "8_1", "m_name": "KWhT^J"},
        {"m_code": "9_1", "m_name": "KWhT^S"},
        {"m_code": "10_1", "m_name": "KWhT^M"},
        {"m_code": "11_1", "m_name": "KWhT^A"},
        {"m_code": "12_1", "m_name": "V1"},
        {"m_code": "13_1", "m_name": "V2"},
        {"m_code": "14_1", "m_name": "V3"},
        {"m_code": "15_1", "m_name": "U1"},
        {"m_code": "16_1", "m_name": "U2"},
        {"m_code": "17_1", "m_name": "U3"},
        {"m_code": "18_1", "m_name": "A1"},
        {"m_code": "19_1", "m_name": "A2"},
        {"m_code": "20_1", "m_name": "A3"},
        {"m_code": "21_1", "m_name": "P1"},
        {"m_code": "22_1", "m_name": "P2"},
        {"m_code": "23_1", "m_name": "P3"},
        {"m_code": "24_1", "m_name": "KWh"},
        {"m_code": "25_1", "m_name": "VARh"},
        {"m_code": "26_1", "m_name": "A MOY"},
        {"m_code": "27_1", "m_name": "COS PHI Total"},
        {"m_code": "28_1", "m_name": "INC"},
        {"m_code": "29_1", "m_name": "INC^J"},
        {"m_code": "30_1", "m_name": "INC^S"},
        {"m_code": "31_1", "m_name": "INC^M"},
        {"m_code": "32_1", "m_name": "INC^A"},
        {"m_code": "33_1", "m_name": "KWh^J-1"},
        {"m_code": "34_1", "m_name": "KWh^S-1"},
        {"m_code": "35_1", "m_name": "KWh^M-1"},
        {"m_code": "36_1", "m_name": "KWh^A-1"},
        {"m_code": "37_1", "m_name": "VARh^J-1"},
        {"m_code": "38_1", "m_name": "VARh^S-1"},
        {"m_code": "39_1", "m_name": "VARh^M-1"},
        {"m_code": "40_1", "m_name": "VARh^A-1"},
        {"m_code": "41_1", "m_name": "Inc^J-1"},
        {"m_code": "42_1", "m_name": "Inc^S-1"},
        {"m_code": "43_1", "m_name": "Inc^M-1"},
        {"m_code": "44_1", "m_name": "Inc^A-1"},
        {"m_code": "45_1", "m_name": "KWhT^J-1"},
        {"m_code": "46_1", "m_name": "KWhT^S-1"},
        {"m_code": "47_1", "m_name": "KWhT^M-1"},
        {"m_code": "48_1", "m_name": "KWhT^A-1"},
    ],
    "cl": [{"Code_Compteur": "OE117", "Le_Compteur": "Abattage Elec"}],
    "retour": "json",
    "cross_tab": "cross_tab_ml"
    # cross_tab_ml
    # normalised
}

task_cluster_2_FAIL = {
    "ml": [
        {"m_code": "11_2", "m_name": "Kg/h_J"},
        #  {"m_code": "13_2", "m_name": "Ratio_J"},
        #   {"m_code": "12_2", "m_name": "INC_J"}
    ],
    "cl": [
        {"Code_Compteur": "MZV00A", "Le_Compteur": "ElMazraa_Cons_Vapeur"},
        {"Code_Compteur": "MZV002", "Le_Compteur": "Collecteur_COP_Vapeur"},
        {"Code_Compteur": "MZVB12", "Le_Compteur": "BâcheAEau_Vapeur"},
        {"Code_Compteur": "MZVA10", "Le_Compteur": "Abattoir_Vapeur"},
        {"Code_Compteur": "MZVA12", "Le_Compteur": "LaveCaisse_Vapeur"},
        {"Code_Compteur": "MZVA11", "Le_Compteur": "Abattage_vapeur"},
        {"Code_Compteur": "MZVA2B", "Le_Compteur": "Conserve_Vapeur"},
        {"Code_Compteur": "MZVA2A", "Le_Compteur": "Surgule_Vapeur"},
        {"Code_Compteur": "MZVA21", "Le_Compteur": "RestTransforme_Vapeur"},
        {"Code_Compteur": "MZVA2C", "Le_Compteur": "Autoclave_Vapeur"},
        {"Code_Compteur": "MZVA2D", "Le_Compteur": "UCPC_Vapeur"},
        {"Code_Compteur": "MZVA2E", "Le_Compteur": "Charcuterie_Vapeur"},
        {"Code_Compteur": "MZVA20", "Le_Compteur": "Transforme_Vapeur"},
        {"Code_Compteur": "MZVA22", "Le_Compteur": "Petfood_Vapeur"},
    ],
    "retour": "json",
    "cross_tab": "normalized",
}

task_cluster_3 = {
    "ml": [{"m_code": "11_2", "m_name": "Kg/h_J"}],
    "cl": [
        {"Code_Compteur": "MZV002", "Le_Compteur": "Collecteur_COP_Vapeur"},
        {"Code_Compteur": "MZV110", "Le_Compteur": "LPlume_Vapeur"},
        {"Code_Compteur": "MZV130", "Le_Compteur": "LViande_Vapeur"},
        {"Code_Compteur": "MZV133", "Le_Compteur": "LV_Cuiseur_3_Vapeur"},
        {"Code_Compteur": "MZV132", "Le_Compteur": "LV_Cuiseur_2_Vapeur"},
        {"Code_Compteur": "MZV131", "Le_Compteur": "LV_Cuiseur_1_Vapeur"},
        {"Code_Compteur": "MZV140", "Le_Compteur": "Bass_Pression_Cop_Vapeur"},
        {"Code_Compteur": "MZV120", "Le_Compteur": "LSang_Vapeur"},
    ],
    "retour": "json",
    "cross_tab": "cross_tab_ml",
}

task_iot_inner_error = {
    "ml": [
        {"m_code": "0_1", "m_name": "KWh^J"},
        {"m_code": "1_1", "m_name": "KWh^S"},
        {"m_code": "2_1", "m_name": "KWh^M"},
        {"m_code": "3_1", "m_name": "KWh^A"},
        {"m_code": "4_1", "m_name": "VARh^J"},
        {"m_code": "5_1", "m_name": "VARh^S"},
        {"m_code": "6_1", "m_name": "VARh^M"},
        {"m_code": "7_1", "m_name": "VARh^A"},
        {"m_code": "8_1", "m_name": "KWhT^J"},
        {"m_code": "9_1", "m_name": "KWhT^S"},
        {"m_code": "10_1", "m_name": "KWhT^M"},
        {"m_code": "11_1", "m_name": "KWhT^A"},
        {"m_code": "12_1", "m_name": "V1"},
        {"m_code": "13_1", "m_name": "V2"},
        {"m_code": "14_1", "m_name": "V3"},
        {"m_code": "15_1", "m_name": "U1"},
        {"m_code": "16_1", "m_name": "U2"},
        {"m_code": "17_1", "m_name": "U3"},
        {"m_code": "18_1", "m_name": "A1"},
        {"m_code": "19_1", "m_name": "A2"},
        {"m_code": "20_1", "m_name": "A3"},
        {"m_code": "21_1", "m_name": "P1"},
        {"m_code": "22_1", "m_name": "P2"},
        {"m_code": "23_1", "m_name": "P3"},
        {"m_code": "24_1", "m_name": "KWh"},
        {"m_code": "25_1", "m_name": "VARh"},
        {"m_code": "26_1", "m_name": "A MOY"},
        {"m_code": "27_1", "m_name": "COS PHI Total"},
        {"m_code": "28_1", "m_name": "INC"},
        {"m_code": "29_1", "m_name": "INC^J"},
        {"m_code": "30_1", "m_name": "INC^S"},
        {"m_code": "31_1", "m_name": "INC^M"},
        {"m_code": "32_1", "m_name": "INC^A"},
        {"m_code": "33_1", "m_name": "KWh^J-1"},
        {"m_code": "34_1", "m_name": "KWh^S-1"},
        {"m_code": "35_1", "m_name": "KWh^M-1"},
        {"m_code": "36_1", "m_name": "KWh^A-1"},
        {"m_code": "37_1", "m_name": "VARh^J-1"},
        {"m_code": "38_1", "m_name": "VARh^S-1"},
        {"m_code": "39_1", "m_name": "VARh^M-1"},
        {"m_code": "40_1", "m_name": "VARh^A-1"},
        {"m_code": "41_1", "m_name": "Inc^J-1"},
        {"m_code": "42_1", "m_name": "Inc^S-1"},
        {"m_code": "43_1", "m_name": "Inc^M-1"},
        {"m_code": "44_1", "m_name": "Inc^A-1"},
        {"m_code": "45_1", "m_name": "KWhT^J-1"},
        {"m_code": "46_1", "m_name": "KWhT^S-1"},
        {"m_code": "47_1", "m_name": "KWhT^M-1"},
        {"m_code": "48_1", "m_name": "KWhT^A-1"},
    ],
    "cl": [{"Code_Compteur": "OE117", "Le_Compteur": "Abattage Elec"}],
    "tl": [
        {
            "SQL": "between '2021-06-02 14:15:00' AND  '2021-06-20 13:00:00' ",
            "SQLc": "where",
        },
        {
            "SQL": "time_bucket_gapfill('30 minutes', iot.date) AS time,avg(iot.value)as valeur",
            "SQLc": "select",
        },
        {"SQL": "LIMIT 5", "SQLc": "limit"},
    ],
    "retour": "json",
    "cross_tab": "normalised",
}

task_iot_inner_2 = {
    "ml": [
        {"m_code": "0_1", "m_name": "KWh^J"},
        {"m_code": "1_1", "m_name": "KWh^S"},
        {"m_code": "2_1", "m_name": "KWh^M"},
        {"m_code": "3_1", "m_name": "KWh^A"},
        {"m_code": "4_1", "m_name": "VARh^J"},
        {"m_code": "5_1", "m_name": "VARh^S"},
        {"m_code": "6_1", "m_name": "VARh^M"},
        {"m_code": "7_1", "m_name": "VARh^A"},
        {"m_code": "8_1", "m_name": "KWhT^J"},
        {"m_code": "9_1", "m_name": "KWhT^S"},
        {"m_code": "10_1", "m_name": "KWhT^M"},
        {"m_code": "11_1", "m_name": "KWhT^A"},
        {"m_code": "12_1", "m_name": "V1"},
        {"m_code": "13_1", "m_name": "V2"},
        {"m_code": "14_1", "m_name": "V3"},
        {"m_code": "15_1", "m_name": "U1"},
        {"m_code": "16_1", "m_name": "U2"},
        {"m_code": "17_1", "m_name": "U3"},
        {"m_code": "18_1", "m_name": "A1"},
        {"m_code": "19_1", "m_name": "A2"},
        {"m_code": "20_1", "m_name": "A3"},
        {"m_code": "21_1", "m_name": "P1"},
        {"m_code": "22_1", "m_name": "P2"},
        {"m_code": "23_1", "m_name": "P3"},
        {"m_code": "24_1", "m_name": "KWh"},
        {"m_code": "25_1", "m_name": "VARh"},
        {"m_code": "26_1", "m_name": "A MOY"},
        {"m_code": "27_1", "m_name": "COS PHI Total"},
        {"m_code": "28_1", "m_name": "INC"},
        {"m_code": "29_1", "m_name": "INC^J"},
        {"m_code": "30_1", "m_name": "INC^S"},
        {"m_code": "31_1", "m_name": "INC^M"},
        {"m_code": "32_1", "m_name": "INC^A"},
        {"m_code": "33_1", "m_name": "KWh^J-1"},
        {"m_code": "34_1", "m_name": "KWh^S-1"},
        {"m_code": "35_1", "m_name": "KWh^M-1"},
        {"m_code": "36_1", "m_name": "KWh^A-1"},
        {"m_code": "37_1", "m_name": "VARh^J-1"},
        {"m_code": "38_1", "m_name": "VARh^S-1"},
        {"m_code": "39_1", "m_name": "VARh^M-1"},
        {"m_code": "40_1", "m_name": "VARh^A-1"},
        {"m_code": "41_1", "m_name": "Inc^J-1"},
        {"m_code": "42_1", "m_name": "Inc^S-1"},
        {"m_code": "43_1", "m_name": "Inc^M-1"},
        {"m_code": "44_1", "m_name": "Inc^A-1"},
        {"m_code": "45_1", "m_name": "KWhT^J-1"},
        {"m_code": "46_1", "m_name": "KWhT^S-1"},
        {"m_code": "47_1", "m_name": "KWhT^M-1"},
        {"m_code": "48_1", "m_name": "KWhT^A-1"},
    ],
    "cl": [{"Code_Compteur": "OE117", "Le_Compteur": "Abattage Elec"}],
    "tl": [
        {
            "SQL": "iot.date between '2021-06-02 14:15:00' AND  '2021-06-20 13:00:00' ",
            "SQLc": "where",
        },
        {
            "SQL": "time_bucket_gapfill('30 minutes', iot.date) AS time,avg(iot.value)as valeur",
            "SQLc": "select",
        },
        {"SQL": "LIMIT 5", "SQLc": "limit"},
    ],
    "retour": "json",
    "cross_tab": "normalised",
}

task_iot_inner_test = {
    "ml": [{"m_code": "26_1", "m_name": "Index E.Active (KWh)"}],
    "cl": [{"Code_Compteur": "MZCB4B", "Le_Compteur": "Process_Elec_COP"}],
    "tl": [
        {
            "SQL": "iot.date between '2021-06-14'::timestamp -INTERVAL '24 hours' and '2021-06-15'::timestamp",
            "SQLc": "where",
        }
    ],
    "retour": "json",
    "cross_tab": "cross_tab_ml",
}


update_task_1_mod = {
    "Table_name": "Email_V3",
    "data": [
        {
            "Email_Code": "M000",
            "Email_Nom": "Email2-1111111111111",
            "Email_To": {"1": "1"},
            "Email_CC": {"1": "1"},
            "Email_Subject": "1",
            "Email_Body": "1",
            "Email_Attachement": {"1": "1"},
            "Email_Description": "1",
            "To_Internal": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
            "CC_Internl": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
            "Report_FactBook": "",
            "DBAction": "1",
        }
    ],
}

update_task_1_add = {
    "Table_name": "Email_V3",
    "data": [
        {
            "Email_Code": "M000",
            "Email_Nom": "Email2-2",
            "Email_To": {"2": "2"},
            "Email_CC": {"2": "2"},
            "Email_Subject": "2",
            "Email_Body": "2",
            "Email_Attachement": {"2": "2"},
            "Email_Description": "2",
            "To_Internal": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
            "CC_Internl": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
            "Report_FactBook": "",
            "DBAction": "2",
        }
    ],
}

update_task_1_del = {
    "Table_name": "Email_V3",
    "data": [{"Email_Code": "M000", "DBAction": "3"}],
}

update_task_2_add = {
    "Table_name": "Alarme_F_Reporting_V3",
    "data": [
        {
            "Alarme_Code": "A26",
            "Compteur_Incident": "MZC001$28",
            "Formule": "MZC001$0~+~MZC00B$0~",
            "Parsed_Formule": "IN_MZC001$28=MZC001$0~+~MZC00B$0~",
            "Operateur": "<",
            "Objectif": {
                "U_inputobjective": "objective : 78",
                "Sys_inputobjective": [
                    {
                        "keyword": None,
                        "operateur": None,
                        "att": None,
                        "valeur": [{"type": "r", "content": "78"}],
                        "user_value": None,
                    }
                ],
            },
            "Frequence": {
                "Frequence": {
                    "NbUnite": "4",
                    "Periode": "Temps_Reel",
                    "UniteTemp": "Min",
                    "FrequenceUser": "4_Min",
                },
                "OperateurValue": [
                    {
                        "keyword": "Intervalle",
                        "operateur": "Inclure",
                        "att": "Entre",
                        "valeur": "''16:47'' and ''21:45''",
                        "valeur_format": "time",
                    }
                ],
                "UserInterface": ["Inclure Periode  16:47  , 21:45  "],
            },
            "Next_Check": "24/06/2021 00:00:00",
            "U_Alarme_Name": "alarme",
            "Description": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V1~+~Usine_ElMazraa_Cons_Elec$V1~",
            "U_Compteur": "ElMazraa_Cons_Elec$INC_0",
            "U_Formule": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V1~+~Usine_ElMazraa_Cons_Elec$V1~",
            "Nbr_Error": "0",
            "TAG_Formule": "TAG : ",
            "evaluation": None,
        }
    ],
}

# DBAction=1 ==> mod
# DBAction=2 ==> add
# DBAction=3 ==> del
getMaxCode_task = {"Table_name": "tl", "nbr_code": "5"}

display_task_1 = {
    "Table_name": "AllCompteur",
    "Header_list": "*",
    "Header_value": "*",
    "Column_select_liste": "Code_Compteur;Le_Compteur",
    "Column_condition_select_list": "",
    "Column_orderby_list": "",
}

display_task_2 = {
    "Table_name": "AllCompteur",
    "Header_list": "Energie",
    "Header_value": "Air Comprime",
    "Column_select_liste": "Le_Compteur",
    "Column_condition_select_list": "DISTINCT",
    "Column_orderby_list": "desc",
}

fitler_task_1 = {
    "Header_value": "*",
    "Column_select_liste": "Code_Compteur;Le_Compteur",
    "Column_condition_select_list": "*",
    "Header_list": "*",
    "identifier": "undefined3e6cc-f21-0d71-d75a-41a40c61ea26",
    "Column_orderby_list": "*",
    "Table_name": "AllCompteur",
}

display_task_3 = {
    "Header_value": "Electrique",
    "Column_select_liste": "Code_Compteur;Le_Compteur",
    "Column_condition_select_list": "*",
    "Header_list": "Energie",
    "Column_orderby_list": "*",
    "Table_name": "AllCompteur",
}

getObjective_task = {
    "identifier": "15-06-2021-09-42-53-452000-825a7f4-4a84-05e-703-2e785ce438",
    "ml": [
        {"m_code": "0_1", "m_name": "V1"},
        {"m_code": "1_1", "m_name": "V2"},
        {"m_code": "2_1", "m_name": "V3"},
        {"m_code": "3_1", "m_name": "U12"},
        {"m_code": "4_1", "m_name": "U23"},
        {"m_code": "5_1", "m_name": "U31"},
        {"m_code": "6_1", "m_name": "A1"},
        {"m_code": "7_1", "m_name": "A2"},
        {"m_code": "8_1", "m_name": "A3"},
        {"m_code": "9_1", "m_name": "A_Total"},
        {"m_code": "10_1", "m_name": "P1"},
        {"m_code": "11_1", "m_name": "P2"},
        {"m_code": "12_1", "m_name": "P3"},
        {"m_code": "13_1", "m_name": "P"},
        {"m_code": "14_1", "m_name": "Q1"},
        {"m_code": "15_1", "m_name": "Q2"},
        {"m_code": "16_1", "m_name": "Q3"},
        {"m_code": "17_1", "m_name": "Q"},
        {"m_code": "18_1", "m_name": "S1"},
        {"m_code": "19_1", "m_name": "S2"},
        {"m_code": "20_1", "m_name": "S3"},
        {"m_code": "21_1", "m_name": "S"},
        {"m_code": "22_1", "m_name": "PF1"},
        {"m_code": "23_1", "m_name": "PF2"},
        {"m_code": "24_1", "m_name": "PF3"},
        {"m_code": "25_1", "m_name": "PF"},
        {"m_code": "26_1", "m_name": "KWh"},
        {"m_code": "27_1", "m_name": "VARh"},
        {"m_code": "28_1", "m_name": "INC"},
        {"m_code": "29_1", "m_name": "KWh_B"},
        {"m_code": "30_1", "m_name": "VARh_B"},
        {"m_code": "31_1", "m_name": "INC_B"},
        {"m_code": "32_1", "m_name": "Ratio_B"},
        {"m_code": "33_1", "m_name": "KWh_B-1"},
        {"m_code": "34_1", "m_name": "VARh_B-1"},
        {"m_code": "35_1", "m_name": "INC_B-1"},
        {"m_code": "36_1", "m_name": "Ratio_B-1"},
        {"m_code": "37_1", "m_name": "KWh_J"},
        {"m_code": "38_1", "m_name": "VARh_J"},
        {"m_code": "39_1", "m_name": "INC_J"},
        {"m_code": "40_1", "m_name": "Ratio_J"},
        {"m_code": "41_1", "m_name": "KWh_J-1"},
        {"m_code": "42_1", "m_name": "VARh_J-1"},
        {"m_code": "43_1", "m_name": "INC_J-1"},
        {"m_code": "44_1", "m_name": "Ratio_J-1"},
        {"m_code": "45_1", "m_name": "KWh_S"},
        {"m_code": "46_1", "m_name": "VARh_S"},
        {"m_code": "47_1", "m_name": "INC_S"},
        {"m_code": "48_1", "m_name": "Ratio_S"},
        {"m_code": "49_1", "m_name": "KWh_S-1"},
        {"m_code": "50_1", "m_name": "VARh_S-1"},
        {"m_code": "51_1", "m_name": "INC_S-1"},
        {"m_code": "52_1", "m_name": "Ratio_S-1"},
        {"m_code": "53_1", "m_name": "KWh_M"},
        {"m_code": "54_1", "m_name": "VARh_M"},
        {"m_code": "55_1", "m_name": "INC_M"},
        {"m_code": "56_1", "m_name": "Ratio_M"},
        {"m_code": "57_1", "m_name": "KWh_M-1"},
        {"m_code": "58_1", "m_name": "VARh_M-1"},
        {"m_code": "59_1", "m_name": "INC_M-1"},
        {"m_code": "60_1", "m_name": "Ratio_M-1"},
        {"m_code": "61_1", "m_name": "KWh_A"},
        {"m_code": "62_1", "m_name": "VARh_A"},
        {"m_code": "63_1", "m_name": "INC_A"},
        {"m_code": "64_1", "m_name": "Ratio_A"},
        {"m_code": "65_1", "m_name": "KWh_A-1"},
        {"m_code": "66_1", "m_name": "VARh_A-1"},
        {"m_code": "67_1", "m_name": "INC_A-1"},
        {"m_code": "68_1", "m_name": "Ratio_A-1"},
        {"m_code": "69_1", "m_name": "A_Moy_B"},
        {"m_code": "70_1", "m_name": "A_Moy_B-1"},
        {"m_code": "71_1", "m_name": "A_Moy_J"},
        {"m_code": "72_1", "m_name": "A_Moy_J-1"},
        {"m_code": "73_1", "m_name": "A_Moy_S"},
        {"m_code": "74_1", "m_name": "A_Moy_S-1"},
        {"m_code": "75_1", "m_name": "A_Moy_M"},
        {"m_code": "76_1", "m_name": "A_Moy_M-1"},
        {"m_code": "77_1", "m_name": "A_Moy_A"},
        {"m_code": "78_1", "m_name": "A_Moy_A-1"},
        {"m_code": "79_1", "m_name": "A_Min_B"},
        {"m_code": "80_1", "m_name": "A_Min_B-1"},
        {"m_code": "81_1", "m_name": "A_Min_J"},
        {"m_code": "82_1", "m_name": "A_Min_J-1"},
        {"m_code": "83_1", "m_name": "A_Min_S"},
        {"m_code": "84_1", "m_name": "A_Min_S-1"},
        {"m_code": "85_1", "m_name": "A_Min_M"},
        {"m_code": "86_1", "m_name": "A_Min_M-1"},
        {"m_code": "87_1", "m_name": "A_Min_A"},
        {"m_code": "88_1", "m_name": "A_Min_A-1"},
        {"m_code": "89_1", "m_name": "A_Max_B"},
        {"m_code": "90_1", "m_name": "A_Max_B-1"},
        {"m_code": "91_1", "m_name": "A_Max_J"},
        {"m_code": "92_1", "m_name": "A_Max_J-1"},
        {"m_code": "93_1", "m_name": "A_Max_S"},
        {"m_code": "94_1", "m_name": "A_Max_S-1"},
        {"m_code": "95_1", "m_name": "A_Max_M"},
        {"m_code": "96_1", "m_name": "A_Max_M-1"},
        {"m_code": "97_1", "m_name": "A_Max_A"},
        {"m_code": "98_1", "m_name": "A_Max_A-1"},
        {"m_code": "99_1", "m_name": "Total_INC_Check"},
        {"m_code": "100_1", "m_name": "C%_B"},
        {"m_code": "101_1", "m_name": "C%_B-1"},
        {"m_code": "102_1", "m_name": "C%_J"},
        {"m_code": "103_1", "m_name": "C%_J-1"},
        {"m_code": "104_1", "m_name": "C%_S"},
        {"m_code": "105_1", "m_name": "C%_S-1"},
        {"m_code": "106_1", "m_name": "C%_M"},
        {"m_code": "107_1", "m_name": "C%_M-1"},
        {"m_code": "108_1", "m_name": "C%_A"},
        {"m_code": "109_1", "m_name": "C%_A-1"},
        {"m_code": "1000_1", "m_name": "V1 / OBJECTIF"},
        {"m_code": "1001_1", "m_name": "V2 / OBJECTIF"},
        {"m_code": "1002_1", "m_name": "V3 / OBJECTIF"},
        {"m_code": "1003_1", "m_name": "U12 / OBJECTIF"},
        {"m_code": "1004_1", "m_name": "U23 / OBJECTIF"},
        {"m_code": "1005_1", "m_name": "U31 / OBJECTIF"},
        {"m_code": "1006_1", "m_name": "A1 / OBJECTIF"},
        {"m_code": "1007_1", "m_name": "A2 / OBJECTIF"},
        {"m_code": "1008_1", "m_name": "A3 / OBJECTIF"},
        {"m_code": "1009_1", "m_name": "A_Total / OBJECTIF"},
        {"m_code": "1010_1", "m_name": "P1 / OBJECTIF"},
        {"m_code": "1011_1", "m_name": "P2 / OBJECTIF"},
        {"m_code": "1012_1", "m_name": "P3 / OBJECTIF"},
        {"m_code": "1013_1", "m_name": "P / OBJECTIF"},
        {"m_code": "1014_1", "m_name": "Q1 / OBJECTIF"},
        {"m_code": "1015_1", "m_name": "Q2 / OBJECTIF"},
        {"m_code": "1016_1", "m_name": "Q3 / OBJECTIF"},
        {"m_code": "1017_1", "m_name": "Q / OBJECTIF"},
        {"m_code": "1018_1", "m_name": "S1 / OBJECTIF"},
        {"m_code": "1019_1", "m_name": "S2 / OBJECTIF"},
        {"m_code": "1020_1", "m_name": "S3 / OBJECTIF"},
        {"m_code": "1021_1", "m_name": "S / OBJECTIF"},
        {"m_code": "1022_1", "m_name": "PF1 / OBJECTIF"},
        {"m_code": "1023_1", "m_name": "PF2 / OBJECTIF"},
        {"m_code": "1024_1", "m_name": "PF3 / OBJECTIF"},
        {"m_code": "1025_1", "m_name": "PF / OBJECTIF"},
        {"m_code": "1029_1", "m_name": "KWh_B / OBJECTIF"},
        {"m_code": "1030_1", "m_name": "VARh_B / OBJECTIF"},
        {"m_code": "1031_1", "m_name": "INC_B / OBJECTIF"},
        {"m_code": "1032_1", "m_name": "Ratio_B / OBJECTIF"},
        {"m_code": "1037_1", "m_name": "KWh_J / OBJECTIF"},
        {"m_code": "1038_1", "m_name": "VARh_J / OBJECTIF"},
        {"m_code": "1039_1", "m_name": "INC_J / OBJECTIF"},
        {"m_code": "1040_1", "m_name": "Ratio_J / OBJECTIF"},
        {"m_code": "1045_1", "m_name": "KWh_S / OBJECTIF"},
        {"m_code": "1046_1", "m_name": "VARh_S / OBJECTIF"},
        {"m_code": "1047_1", "m_name": "INC_S / OBJECTIF"},
        {"m_code": "1048_1", "m_name": "Ratio_S / OBJECTIF"},
        {"m_code": "1053_1", "m_name": "KWh_M / OBJECTIF"},
        {"m_code": "1054_1", "m_name": "VARh_M / OBJECTIF"},
        {"m_code": "1055_1", "m_name": "INC_M / OBJECTIF"},
        {"m_code": "1056_1", "m_name": "Ratio_M / OBJECTIF"},
        {"m_code": "1061_1", "m_name": "KWh_A / OBJECTIF"},
        {"m_code": "1062_1", "m_name": "VARh_A / OBJECTIF"},
        {"m_code": "1063_1", "m_name": "INC_A / OBJECTIF"},
        {"m_code": "1064_1", "m_name": "Ratio_A / OBJECTIF"},
        {"m_code": "1069_1", "m_name": "A_Moy_B / OBJECTIF"},
        {"m_code": "1071_1", "m_name": "A_Moy_J / OBJECTIF"},
        {"m_code": "1073_1", "m_name": "A_Moy_S / OBJECTIF"},
        {"m_code": "1075_1", "m_name": "A_Moy_M / OBJECTIF"},
        {"m_code": "1077_1", "m_name": "A_Moy_A / OBJECTIF"},
        {"m_code": "1079_1", "m_name": "A_Min_B / OBJECTIF"},
        {"m_code": "1081_1", "m_name": "A_Min_J / OBJECTIF"},
        {"m_code": "1083_1", "m_name": "A_Min_S / OBJECTIF"},
        {"m_code": "1085_1", "m_name": "A_Min_M / OBJECTIF"},
        {"m_code": "1087_1", "m_name": "A_Min_A / OBJECTIF"},
        {"m_code": "1089_1", "m_name": "A_Max_B / OBJECTIF"},
        {"m_code": "1091_1", "m_name": "A_Max_J / OBJECTIF"},
        {"m_code": "1093_1", "m_name": "A_Max_S / OBJECTIF"},
        {"m_code": "1095_1", "m_name": "A_Max_M / OBJECTIF"},
        {"m_code": "1097_1", "m_name": "A_Max_A / OBJECTIF"},
        {"m_code": "2000_1", "m_name": "V1 / PARENT"},
        {"m_code": "2001_1", "m_name": "V2 / PARENT"},
        {"m_code": "2002_1", "m_name": "V3 / PARENT"},
        {"m_code": "2003_1", "m_name": "U12 / PARENT"},
        {"m_code": "2004_1", "m_name": "U23 / PARENT"},
        {"m_code": "2005_1", "m_name": "U31 / PARENT"},
        {"m_code": "2006_1", "m_name": "A1 / PARENT"},
        {"m_code": "2007_1", "m_name": "A2 / PARENT"},
        {"m_code": "2008_1", "m_name": "A3 / PARENT"},
        {"m_code": "2009_1", "m_name": "A_Total / PARENT"},
        {"m_code": "2010_1", "m_name": "P1 / PARENT"},
        {"m_code": "2011_1", "m_name": "P2 / PARENT"},
        {"m_code": "2012_1", "m_name": "P3 / PARENT"},
        {"m_code": "2013_1", "m_name": "P / PARENT"},
        {"m_code": "2014_1", "m_name": "Q1 / PARENT"},
        {"m_code": "2015_1", "m_name": "Q2 / PARENT"},
        {"m_code": "2016_1", "m_name": "Q3 / PARENT"},
        {"m_code": "2017_1", "m_name": "Q / PARENT"},
        {"m_code": "2018_1", "m_name": "S1 / PARENT"},
        {"m_code": "2019_1", "m_name": "S2 / PARENT"},
        {"m_code": "2020_1", "m_name": "S3 / PARENT"},
        {"m_code": "2021_1", "m_name": "S / PARENT"},
        {"m_code": "2022_1", "m_name": "PF1 / PARENT"},
        {"m_code": "2023_1", "m_name": "PF2 / PARENT"},
        {"m_code": "2024_1", "m_name": "PF3 / PARENT"},
        {"m_code": "2025_1", "m_name": "PF / PARENT"},
        {"m_code": "2029_1", "m_name": "KWh_B / PARENT"},
        {"m_code": "2030_1", "m_name": "VARh_B / PARENT"},
        {"m_code": "2031_1", "m_name": "INC_B / PARENT"},
        {"m_code": "2032_1", "m_name": "Ratio_B / PARENT"},
        {"m_code": "2037_1", "m_name": "KWh_J / PARENT"},
        {"m_code": "2038_1", "m_name": "VARh_J / PARENT"},
        {"m_code": "2039_1", "m_name": "INC_J / PARENT"},
        {"m_code": "2040_1", "m_name": "Ratio_J / PARENT"},
        {"m_code": "2045_1", "m_name": "KWh_S / PARENT"},
        {"m_code": "2046_1", "m_name": "VARh_S / PARENT"},
        {"m_code": "2047_1", "m_name": "INC_S / PARENT"},
        {"m_code": "2048_1", "m_name": "Ratio_S / PARENT"},
        {"m_code": "2053_1", "m_name": "KWh_M / PARENT"},
        {"m_code": "2054_1", "m_name": "VARh_M / PARENT"},
        {"m_code": "2055_1", "m_name": "INC_M / PARENT"},
        {"m_code": "2056_1", "m_name": "Ratio_M / PARENT"},
        {"m_code": "2061_1", "m_name": "KWh_A / PARENT"},
        {"m_code": "2062_1", "m_name": "VARh_A / PARENT"},
        {"m_code": "2063_1", "m_name": "INC_A / PARENT"},
        {"m_code": "2064_1", "m_name": "Ratio_A / PARENT"},
        {"m_code": "2069_1", "m_name": "A_Moy_B / PARENT"},
        {"m_code": "2071_1", "m_name": "A_Moy_J / PARENT"},
        {"m_code": "2073_1", "m_name": "A_Moy_S / PARENT"},
        {"m_code": "2075_1", "m_name": "A_Moy_M / PARENT"},
        {"m_code": "2077_1", "m_name": "A_Moy_A / PARENT"},
        {"m_code": "2079_1", "m_name": "A_Min_B / PARENT"},
        {"m_code": "2081_1", "m_name": "A_Min_J / PARENT"},
        {"m_code": "2083_1", "m_name": "A_Min_S / PARENT"},
        {"m_code": "2085_1", "m_name": "A_Min_M / PARENT"},
        {"m_code": "2087_1", "m_name": "A_Min_A / PARENT"},
        {"m_code": "2089_1", "m_name": "A_Max_B / PARENT"},
        {"m_code": "2091_1", "m_name": "A_Max_J / PARENT"},
        {"m_code": "2093_1", "m_name": "A_Max_S / PARENT"},
        {"m_code": "2095_1", "m_name": "A_Max_M / PARENT"},
        {"m_code": "2097_1", "m_name": "A_Max_A / PARENT"},
    ],
    "cl": [{"Code_Compteur": "OIN_MZC001", "Le_Compteur": "ElMazraa_Cons_Elec"}],
    "cross_tab": "normalised",
    "retour": "json",
}

complexQuery_task = {}
update_task_3_del = {
    "Table_name": "Alarme_F_Reporting_V3",
    "data": [{"Alarme_Code": "A48888", "DBAction": "3"}],
}
update_task_3_add = {
    "Table_name": "Alarme_F_Reporting_V3",
    "data": [
        {
            "Alarme_Code": "A48888",
            "Compteur_Incident": "MZC001$28",
            "Formule": "MZC001$2~+~#25~+~MZC110$2~",
            "Parsed_Formule": "IN_MZC001$28=MZC001$2~+~#25~+~MZC110$2~",
            "Operateur": "<",
            "Objectif": {
                "U_inputobjective": "objective : 78",
                "Sys_inputobjective": [
                    {
                        "keyword": None,
                        "operateur": None,
                        "att": None,
                        "valeur": [{"type": "r", "content": "78"}],
                        "user_value": None,
                    }
                ],
            },
            "Frequence": {
                "Frequence": {
                    "NbUnite": "3",
                    "Periode": "Temps_Reel",
                    "UniteTemp": "Min",
                    "FrequenceUser": "3_Min",
                },
                "OperateurValue": [
                    {
                        "keyword": "Intervalle",
                        "operateur": "Inclure",
                        "att": "Entre",
                        "valeur": "''10:34'' and ''04:33''",
                        "valeur_format": "time",
                    }
                ],
                "UserInterface": ["Inclure Periode 10:34,04:33 "],
            },
            "Next_Check": "25/06/2021 00:00:00",
            "U_Alarme_Name": "alarme1",
            "Description": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
            "U_Compteur": "ElMazraa_Cons_Elec$INC_0",
            "U_Formule": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
            "Nbr_Error": "0",
            "TAG_Formule": "TAG : ",
            "evaluation": None,
            "DBAction": "2",
        }
    ],
}

update_task_3_mod = {
    "Table_name": "Alarme_F_Reporting_V3",
    "data": [
        {
            "Alarme_Code": "A48888",
            "Compteur_Incident": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa$28",
            "Formule": "MZC001$2~+~#25~+~MZC110$2~",
            "Parsed_Formule": "IN_MZC001$28=MZC001$2~+~#25~+~MZC110$2~",
            "Operateur": "<",
            "Objectif": {
                "U_inputobjective": "objective : 78",
                "Sys_inputobjective": [
                    {
                        "keyword": None,
                        "operateur": None,
                        "att": None,
                        "valeur": [{"type": "r", "content": "78"}],
                        "user_value": None,
                    }
                ],
            },
            "Frequence": {
                "Frequence": {
                    "NbUnite": "3",
                    "Periode": "Temps_Reel",
                    "UniteTemp": "Min",
                    "FrequenceUser": "3_Min",
                },
                "OperateurValue": [
                    {
                        "keyword": "Intervalle",
                        "operateur": "Inclure",
                        "att": "Entre",
                        "valeur": "''10:34'' and ''04:33''",
                        "valeur_format": "time",
                    }
                ],
                "UserInterface": ["Inclure Periode 10:34,04:33 "],
            },
            "Next_Check": "25/06/2021 00:00:00",
            "U_Alarme_Name": "alarme1",
            "Description": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
            "U_Compteur": "ElMazraa_Cons_Elec$INC_0",
            "U_Formule": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
            "Nbr_Error": "0",
            "TAG_Formule": "TAG : ",
            "evaluation": None,
            "DBAction": "1",
        }
    ],
}


update_task_alarme = {
    "Table_name": "Alarme_F_Reporting_V3",
    "data": [
        {
            "Alarme_Code": "A0000001",
            "Compteur_Incident": "MZC001$28",
            "Formule": "MZC001$2~+~#25~+~MZC110$2~",
            "Parsed_Formule": "IN_MZC001$28=MZC001$2~+~#25~+~MZC110$2~",
            "Operateur": "<",
            "Objectif": {
                "U_inputobjective": "objective : 78",
                "Sys_inputobjective": [
                    {
                        "keyword": None,
                        "operateur": None,
                        "att": None,
                        "valeur": [{"type": "r", "content": "78"}],
                        "user_value": None,
                    }
                ],
            },
            "Frequence": {
                "Frequence": {
                    "NbUnite": "3",
                    "Periode": "Temps_Reel",
                    "UniteTemp": "Min",
                    "FrequenceUser": "3_Min",
                },
                "OperateurValue": [
                    {
                        "keyword": "Intervalle",
                        "operateur": "Inclure",
                        "att": "Entre",
                        "valeur": "''10:34'' and ''04:33''",
                        "valeur_format": "time",
                    }
                ],
                "UserInterface": ["Inclure Periode  10:34  , 04:33  "],
            },
            "Next_Check": "25/06/2021 00:00:00",
            "U_Alarme_Name": "alarme1",
            "Description": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
            "U_Compteur": "ElMazraa_Cons_Elec$INC_0",
            "U_Formule": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
            "Nbr_Error": "0",
            "TAG_Formule": "TAG : ",
            "evaluation": None,
            "DBAction": "2",
        }
    ],
}

# test=Back_worker_bridge( rabbitUsername="test",rabbitPassword="test",rabbitAddress="192.168.3.100",oneTimeUse=True)
# r=test.iot_inner(task_iot_inner)
# print(r)

# db = Back_DB_bridge(database="vmzdb3", server="192.168.3.91",logging=True,printLog=True)
# r=db.cluster(task_cluster_3)
# print(r)

# db = Back_DB_bridge(database="backup_17_06_2021_v2_14_41", server="192.168.3.100",logging=True,printLog=False)
# r=db.cluster(task_cluster)
# print(r)
# print(type(r))

task_iot_inner_3 = {
    "ml": [{"m_code": "26_1", "m_name": "Index E.Active (KWh)"}],
    "cl": [{"Code_Compteur": "MZCB4B4", "Le_Compteur": "USang_1"}],
    "tl": [
        {
            "SQL": "( iot.date between LOCALTIMESTAMP(0) -INTERVAL '24 hour' and LOCALTIMESTAMP(0) )",
            "SQLc": "where",
        }
    ],
    "retour": "json",
    "cross_tab": "cross_tab_ml",
}

task_iot_inner_4 = {
    "ml": [{"m_code": "26_1", "m_name": "Index E.Active (KWh)"}],
    "cl": [{"Code_Compteur": "MZCB4B", "Le_Compteur": "Process_Elec_COP"}],
    "tl": [
        {
            "SQL": "( iot.date between '2021-06-15 00:00:00' -INTERVAL '24 hour' and '2021-06-15 00:00:00')",
            "SQLc": "where",
        }
    ],
    "retour": "json",
    "cross_tab": "cross_tab_ml",
}

update_task_4_add = {
    "Table_name": "Tableaux_V3",
    "data": [
        {
            "Autheur": "",
            "Code": "T34444444444",
            "Description": "asma",
            "Nom": "asma",
            "Prefix": "T",
            "Security_Group": "16383",
            "identifier": "30-06-2021-09-09-21-799000-43e6a83-a655-5564-672b-00e476eb3a",
            "DBAction": "2",
        }
    ],
}


bwb = Back_worker_bridge(
    rabbitUsername="test", rabbitPassword="test", rabbitAddress="192.168.3.100"
)
bdb=Back_DB_bridge(database="vmzdb3",transaction=True,server="192.168.3.91")

task_iot_inner_44 = {
    "ml": [{ "m_code": "26_1", "m_name": "Index E.Active (KWh)" }],
    "cl": [ {"Code_Compteur": "MZCB4B","Le_Compteur":"Process_Elec_COP"}],
    "tl": [{
                "SQL": "( iot.date between '2021-06-15 00:00:00'::timestamp -INTERVAL '24 hour' and '2021-06-15 00:00:00'::timestamp)",
                "SQLc": "where asc"
        },{
                "SQL": "4",
                "SQLc": "limit"
        }],
    "retour": "table",
    "cross_tab": "normalised"}
# r=bwb.iot_inner(task_iot_inner_44)

task_insert_iot = {"data":[{"cc_m": "IN_MZC001,0", "value": "390"},{"cc_m": "IN_MZC001,1", "value": "300"}]}
r=bwb.insert_iot(task_insert_iot)

# r = bwb.iot_inner(task_iot_inner_test)
# r=bdb.update(update_task_4_add)

print(r)

# db = DBManager(database="vmzdb3",server="192.168.3.91")
# q='select * from "Email_V3"'
# db.export_csv(q,"aaaa.csv")

def paral():
    def process(i):
        tdb = Back_DB_bridge(
            database="backup_vmzdb3_15_06_2021_09_51", server="192.168.3.100"
        )
        # return tdb.display(fitler_task_1)
        # update_task["data"][0]["Email_Nom"]="Email2-{}".format(str(i))
        # return update_task
        return tdb.getMaxCode(getMaxCode_task)

    # return {"r1":r1,"r2":r2,"r3":r3}
    results = Parallel(n_jobs=13)(delayed(process)(i) for i in range(40))

    print(results)
