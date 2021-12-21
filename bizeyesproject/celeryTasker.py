from celeryWorker import iot_inner
import sys
# from dbManager import DBManager
from back_DB_bridge import Back_DB_bridge
from back_worker_bridge import Back_worker_bridge
# import psycopg2,multiprocessing
from joblib import Parallel, delayed
# db = DBManager(database="data")

# task_cluster = {
#     "ml": [{"m_code": "0_1", "m_name": "KWh^J"}, {"m_code": "1_1", "m_name": "KWh^S"}, {"m_code": "2_1", "m_name": "KWh^M"}, {"m_code": "3_1", "m_name": "KWh^A"}, {"m_code": "4_1", "m_name": "VARh^J"}, {"m_code": "5_1", "m_name": "VARh^S"}, {"m_code": "6_1", "m_name": "VARh^M"}, {"m_code": "7_1", "m_name": "VARh^A"}, {"m_code": "8_1", "m_name": "KWhT^J"}, {"m_code": "9_1", "m_name": "KWhT^S"}, {"m_code": "10_1", "m_name": "KWhT^M"}, {"m_code": "11_1", "m_name": "KWhT^A"}, {"m_code": "12_1", "m_name": "V1"}, {"m_code": "13_1", "m_name": "V2"}, {"m_code": "14_1", "m_name": "V3"}, {"m_code": "15_1", "m_name": "U1"}, {"m_code": "16_1", "m_name": "U2"}, {"m_code": "17_1", "m_name": "U3"}, {"m_code": "18_1", "m_name": "A1"}, {"m_code": "19_1", "m_name": "A2"}, {"m_code": "20_1", "m_name": "A3"}, {"m_code": "21_1", "m_name": "P1"}, {"m_code": "22_1", "m_name": "P2"}, {"m_code": "23_1", "m_name": "P3"}, {"m_code": "24_1", "m_name": "KWh"}, {"m_code": "25_1", "m_name": "VARh"}, {"m_code": "26_1", "m_name": "A MOY"}, {"m_code": "27_1", "m_name": "COS PHI Total"}, {"m_code": "28_1", "m_name": "INC"}, {"m_code": "29_1", "m_name": "INC^J"}, {"m_code": "30_1", "m_name": "INC^S"}, {"m_code": "31_1", "m_name": "INC^M"}, {"m_code": "32_1", "m_name": "INC^A"}, {"m_code": "33_1", "m_name": "KWh^J-1"}, {"m_code": "34_1", "m_name": "KWh^S-1"}, {"m_code": "35_1", "m_name": "KWh^M-1"}, {"m_code": "36_1", "m_name": "KWh^A-1"}, {"m_code": "37_1", "m_name": "VARh^J-1"}, {"m_code": "38_1", "m_name": "VARh^S-1"}, {"m_code": "39_1", "m_name": "VARh^M-1"}, {"m_code": "40_1", "m_name": "VARh^A-1"}, {"m_code": "41_1", "m_name": "Inc^J-1"}, {"m_code": "42_1", "m_name": "Inc^S-1"}, {"m_code": "43_1", "m_name": "Inc^M-1"}, {"m_code": "44_1", "m_name": "Inc^A-1"}, {"m_code": "45_1", "m_name": "KWhT^J-1"}, {"m_code": "46_1", "m_name": "KWhT^S-1"}, {"m_code": "47_1", "m_name": "KWhT^M-1"}, {"m_code": "48_1", "m_name": "KWhT^A-1"}],
#     "cl": [{"Code_Compteur": "OE117", "Le_Compteur": "Abattage Elec"}],
#     "retour": "json",
#     "cross_tab": "normalised"
# }
#

task_cluster = {
    "ml": [{"m_code": "11_2", "m_name": "Kg/h_J"}, {"m_code": "13_2", "m_name": "Ratio_J"}, {"m_code": "12_2", "m_name": "INC_J"}],
    "cl": [{"Code_Compteur": "MZV00A", "Le_Compteur": "ElMazraa_Cons_Vapeur"},
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
           {"Code_Compteur": "MZVA22", "Le_Compteur": "Petfood_Vapeur"}],
    "retour": "json",
    "cross_tab": "normalised"
}
#normalised
#
# task_cluster_coproduit ={
#      "ml":[

#     {
#         "m_code": "13_1",
#         "m_name": "Puissance Active (P)"
#     },
#     {
#         "m_code": "26_1",
#         "m_name": "Index E.Active (KWh)"
#     },
#     {
#         "m_code": "27_1",
#         "m_name": "Index E.Reactive (Varh)"
#     },
#     {
#         "m_code": "25_1",
#         "m_name": "Facteur de puissance"
#     },
#     {
#         "m_code": "37_1",
#         "m_name": "Consommation Jour"
#     },
#     {
#         "m_code": "41_1",
#         "m_name": "Consommation Jour-1"
#     },
#     {
#         "m_code": "39_1",
#         "m_name": "Incident jour"
#     },
#     {
#         "m_code": "43_1",
#         "m_name": "Incident jour-1"
#     }

# ],
#     "cl": [{ "Code_Compteur": "MZCB4B", "Le_Compteur": "Process_Elec_COP" }],
#     "retour": "json",
#     "cross_tab": "cross_tab_cl"
# }


task_cluster_coproduit ={
     "ml": [
                            {
                                "m_code": "13_1",
                                "m_name": "Puissance Active (P)"
                            },
                            {
                                "m_code": "26_1",
                                "m_name": "Index E.Active (KWh)"
                            },
                            {
                                "m_code": "27_1",
                                "m_name": "Index E.Reactive (Varh)"
                            },
                            {
                                "m_code": "25_1",
                                "m_name": "Facteur de puissance"
                            },
                            {
                                "m_code": "37_1",
                                "m_name": "Consommation Jour"
                            },
                            {
                                "m_code": "41_1",
                                "m_name": "Consommation Jour-1"
                            },
                            {
                                "m_code": "39_1",
                                "m_name": "Incident jour"
                            },
                            {
                                "m_code": "43_1",
                                "m_name": "Incident jour-1"
                            }
                        ],
                        "cl": [
                            { "Code_Compteur": "MZCB4B", "Le_Compteur": "Process_Elec_COP" }
                        ],
                        
                        "retour": "json",
                        "cross_tab": "cross_tab_ml"
}







# task_iot_inner = {
#     "ml": [{"m_code": "0_1", "m_name": "KWh^J"}, {"m_code": "1_1", "m_name": "KWh^S"}, {"m_code": "2_1", "m_name": "KWh^M"}, {"m_code": "3_1", "m_name": "KWh^A"}, {"m_code": "4_1", "m_name": "VARh^J"}, {"m_code": "5_1", "m_name": "VARh^S"}, {"m_code": "6_1", "m_name": "VARh^M"}, {"m_code": "7_1", "m_name": "VARh^A"}, {"m_code": "8_1", "m_name": "KWhT^J"}, {"m_code": "9_1", "m_name": "KWhT^S"}, {"m_code": "10_1", "m_name": "KWhT^M"}, {"m_code": "11_1", "m_name": "KWhT^A"}, {"m_code": "12_1", "m_name": "V1"}, {"m_code": "13_1", "m_name": "V2"}, {"m_code": "14_1", "m_name": "V3"}, {"m_code": "15_1", "m_name": "U1"}, {"m_code": "16_1", "m_name": "U2"}, {"m_code": "17_1", "m_name": "U3"}, {"m_code": "18_1", "m_name": "A1"}, {"m_code": "19_1", "m_name": "A2"}, {"m_code": "20_1", "m_name": "A3"}, {"m_code": "21_1", "m_name": "P1"}, {"m_code": "22_1", "m_name": "P2"}, {"m_code": "23_1", "m_name": "P3"}, {"m_code": "24_1", "m_name": "KWh"}, {"m_code": "25_1", "m_name": "VARh"}, {"m_code": "26_1", "m_name": "A MOY"}, {"m_code": "27_1", "m_name": "COS PHI Total"}, {"m_code": "28_1", "m_name": "INC"}, {"m_code": "29_1", "m_name": "INC^J"}, {"m_code": "30_1", "m_name": "INC^S"}, {"m_code": "31_1", "m_name": "INC^M"}, {"m_code": "32_1", "m_name": "INC^A"}, {"m_code": "33_1", "m_name": "KWh^J-1"}, {"m_code": "34_1", "m_name": "KWh^S-1"}, {"m_code": "35_1", "m_name": "KWh^M-1"}, {"m_code": "36_1", "m_name": "KWh^A-1"}, {"m_code": "37_1", "m_name": "VARh^J-1"}, {"m_code": "38_1", "m_name": "VARh^S-1"}, {"m_code": "39_1", "m_name": "VARh^M-1"}, {"m_code": "40_1", "m_name": "VARh^A-1"}, {"m_code": "41_1", "m_name": "Inc^J-1"}, {"m_code": "42_1", "m_name": "Inc^S-1"}, {"m_code": "43_1", "m_name": "Inc^M-1"}, {"m_code": "44_1", "m_name": "Inc^A-1"}, {"m_code": "45_1", "m_name": "KWhT^J-1"}, {"m_code": "46_1", "m_name": "KWhT^S-1"}, {"m_code": "47_1", "m_name": "KWhT^M-1"}, {"m_code": "48_1", "m_name": "KWhT^A-1"}],
#     "cl": [{"Code_Compteur": "OE117", "Le_Compteur": "Abattage Elec"}],
#     "tl": [{"SQL": "between '2021-06-02 14:15:00' AND  '2021-06-20 13:00:00' ", "SQLc": "where"},
#            {"SQL": "time_bucket_gapfill('30 minutes', iot.date) AS time,avg(iot.value)as valeur",
#             "SQLc": "select"},
#            {"SQL": "LIMIT 5", "SQLc": "limit"}],
#     "retour": "json",
#     "cross_tab": "normalised"
# }

# task_iot_inner_test = {
#     "ml": [{ "m_code": "26_1", "m_name": "Index E.Active (KWh)" }],
#     "cl": [ {"Code_Compteur": "MZCB4B","Le_Compteur":"Process_Elec_COP"}],
#     "tl": [{
#                 "SQL": "( iot.date between '2021-06-15 00:00:00' -INTERVAL '24 hour' and '2021-06-15 00:00:00')",
#                 "SQLc": "where"
#             }],
#     "retour": "json",
#     "cross_tab": "cross_tab_ml"
# }

[
    {
        "Tl_Sql": [
            {
                "SQL": "( iot.date between LOCALTIMESTAMP(0) -INTERVAL '24 hour' and LOCALTIMESTAMP(0)           )",
                "SQLc": "where"
            }
        ],
        "Tl_User": [
            {
                "att": "Entre",
                "valeur": "LOCALTIMESTAMP(0) -INTERVAL '24 hour' and LOCALTIMESTAMP(0)           ",
                "keyword": "Intervalles",
                "operateur": "Inclure",
                "valeurUser": "Maintenant -INTERVAL '24 Heure',Maintenant           "
            }
        ]
    }
]



# //////////////////////////////////////////////////////////////////
# [
#     {
#         "Tl_Sql": [
#             {
#                 "SQL": "( iot.date between LOCALTIMESTAMP(0)            and date\"2021-06-14 17:24:46\" -INTERVAL \"24 hour\")",
#                 "SQLc": "where"
#             }
#         ],
#         "Tl_User": [
#             {
#                 "att": "Entre",
#                 "valeur": "LOCALTIMESTAMP(0)            and date\"2021-06-14 17:24:46\" -INTERVAL \"24 hour\"",
#                 "keyword": "Intervalles",
#                 "operateur": "Inclure",
#                 "valeurUser": "Maintenant           ,date\"2021-06-14 17:24:46\" -INTERVAL \"24 Heure\""
#             }
#         ]
#     }
# ]
# _iot_inner = iot_inner.delay(task_iot_inner)
# _iot_inner = _iot_inner.get()
# print(_iot_inner)


update_task_1 = {"Table_name": "Email_V3",
                 "data": [{
                     "Email_Code": "M2",
                     "Email_Nom": "Email2-1",
                     "Email_To": {"1": "1"}, "Email_CC": {"1": "1"},
                     "Email_Subject": "1",
                     "Email_Body": "1",
                     "Email_Attachement": {"1": "1"},
                     "Email_Description": "1",
                     "To_Internal": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
                     "CC_Internl": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
                     "Report_FactBook": "",
                     "DBAction": "1"}]
                 }
update_task_2 = {"Table_name": "Email_V3",
                 "data": [{
                     "Email_Code": "M2",
                     "Email_Nom": "Email2-2",
                     "Email_To": {"2": "2"}, "Email_CC": {"2": "2"},
                     "Email_Subject": "2",
                     "Email_Body": "2",
                     "Email_Attachement": {"2": "2"},
                     "Email_Description": "2",
                     "To_Internal": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
                     "CC_Internl": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
                     "Report_FactBook": "",
                     "DBAction": "1"}]
                 }
update_task_3 = {"Table_name": "Email_V3",
                 "data": [{
                     "Email_Code": "M2",
                     "Email_Nom": "Email2-3",
                     "Email_To": {"3": "3"}, "Email_CC": {"3": "3"},
                     "Email_Subject": "3",
                     "Email_Body": "3",
                     "Email_Attachement": {"3": "3"},
                     "Email_Description": "3",
                     "To_Internal": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
                     "CC_Internl": "user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
                     "Report_FactBook": "",
                     "DBAction": "1"}]
                 }
update_task_alarme = {
    "Table_name": "Alarme_F_Reporting_V3",
    "data": [{"Alarme_Code": "A48", "Compteur_Incident": "MZC001$28", "Formule": "MZC001$2~+~#25~+~MZC110$2~",
              "Parsed_Formule": "IN_MZC001$28=MZC001$2~+~#25~+~MZC110$2~", "Operateur": "<",
              "Objectif": {"U_inputobjective": "objective : 78",
                           "Sys_inputobjective": [{"keyword": 0, "operateur": 0, "att": 0, "valeur": [{"type": "r", "content": "78"}],
                                                   "user_value": 0}]},
              "Frequence": {"Frequence": {"NbUnite": "3", "Periode": "Temps_Reel", "UniteTemp": "Min", "FrequenceUser": "3_Min"}, "OperateurValue": [{"keyword": "Intervalle", "operateur": "Inclure", "att": "Entre", "valeur": "''10:34'' and ''04:33''", "valeur_format": "time"}], "UserInterface": ["Inclure Periode  10:34  , 04:33  "]},
              "Next_Check": "25/06/2021 00:00:00", "U_Alarme_Name": "alarme1",
              "Description": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
              "U_Compteur": "ElMazraa_Cons_Elec$INC_0",
              "U_Formule": "ElMazraa_Cons_Elec$INC_0=ElMazraa_Cons_Elec$V3~+~#25~+~Ligne_Plume$V3~",
              "Nbr_Error": "0", "TAG_Formule": "TAG : ",
              "evaluation": 0, "DBAction": "2"}]
}
# DBAction=1 ==> mod
# DBAction=2 ==> add
# DBAction=3 ==> del
getMaxCode_task = {"Table_name": "Alarme_F_Reporting", "nbr_code": "1"}

display_task_1 = {"Table_name": "AllCompteur", "Header_list": "*", "Header_value": "*",
                  "Column_select_liste": "Code_Compteur;Le_Compteur", "Column_condition_select_list": "", "Column_orderby_list": ""}

display_task_2 = {"Table_name": "AllCompteur", "Header_list": "Energie", "Header_value": "Air Comprime",
                  "Column_select_liste": "Le_Compteur", "Column_condition_select_list": "DISTINCT", "Column_orderby_list": "desc"}

fitler_task_1 = {
    "Header_value": "*",
    "Column_select_liste": "Code_Compteur;Le_Compteur",
    "Column_condition_select_list": "*",
    "Header_list": "*",
    "identifier": "undefined3e6cc-f21-0d71-d75a-41a40c61ea26",
    "Column_orderby_list": "*",
    "Table_name": "AllCompteur",
}
# db = Back_DB_bridge(database="datamazraa", server="192.168.3.91")
# r = db.display(fitler_task_1)
# print(r)


def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size

# def process(i):
#     def pro(update_task,i):
#             tdb=Back_DB_bridge(database="data",server="192.168.3.100",transaction=True)
#             # return tdb.display(fitler_task_1)
#             update_task["data"][0]["Email_Nom"]="Email2-{}".format(str(i))
#             # return update_task
#             return tdb.update_V2(update_task)

#     r1=pro(update_task_1,i)
#     # r2=pro(update_task_2,i)
#     # r3=pro(update_task_3,i)

#     return {"r1":r1}
#     # return {"r1":r1,"r2":r2,"r3":r3}
# results = Parallel(n_jobs=13)(delayed(process)(i) for i in range(40))
# print(get_size(results))
# print(results)


# r=bridge.cluster(task_cluster)
# print(r)
# print(bridge.closeConnection())
db=Back_DB_bridge("vmzdb3")
# # r=db.cluster(task_cluster)
r=db.cluster(task_cluster_coproduit)
# test=Back_worker_bridge(rabbitAddress='192.168.3.100', rabbitPassword='test',rabbitUsername='test')
# r=test.iot_inner(task_iot_inner_test)
taskaddwithi = [
    {"Alarme_Code": "A5", "Compteur_Incident": "MZC110$28", "Formule": "MZC110$0~+~F~(~MZC120$0~)~",
     "Parsed_Formule": "IN_MZC110$28=MZC110$0~+~F~(~MZC120$0~)~", "Operateur": "<",
      "Objectif": {"U_inputobjective": "objective : 65", "Sys_inputobjective": [{"keyword": None, "operateur": None, "att": None, "valeur": [{"type": "r", "content": "65"}], "user_value": None}]},
      "Frequence": {"Frequence": {"NbUnite": "2", "Periode": "Temps_Reel", "UniteTemp": "Min",
                                  "FrequenceUser": "2_Min"},
                    "OperateurValue": [{"keyword": "Intervalle", "operateur": "Inclure", "att": "Entre", "valeur": "''11:04'' and ''05:02''", "valeur_format": "time"}],
                    "UserInterface": ["Inclure Periode  11:04  , 05:02  "]},
      "Next_Check": "24/06/2021 00:00:00", "U_Alarme_Name": "alarme2",
      "Description": "Ligne_Plume$INC_0=Ligne_Plume$V1~+~Fils~(~Ligne_Sang$V1~)~",
      "U_Compteur": "Ligne_Plume$INC_0", "U_Formule": "Ligne_Plume$INC_0=Ligne_Plume$V1~+~Fils~(~Ligne_Sang$V1~)~",
      "Nbr_Error": "0", "TAG_Formule": "TAG : ", "evaluation": None, "DBAction": "2"}
]
# db = Back_DB_bridge("vmzdb3", transaction=True)
# r = db.update(update_task_alarme)
print(r)
