import json

# tasker=TaskManager(taskQueue="tasks_queue")
tasker = TaskManager(taskQueue="tasks_queue",
                     exchangeName="taskRouter", server="192.168.3.100", username="test", password="test")


# task = {"service": "display", "Table_name": "Email", "Header_list": "*", "Header_value": "*",
#         "Column_select_liste": "", "Column_condition_select_list": "", "Column_orderby_list": ""}

task = {"service": "display", "Table_name": "Alarme_F_Reporting", "Header_list": "*", "Header_value": "*",
        "Column_select_liste": "Code_Compteur;Le_Compteur", "Column_condition_select_list": "*", "Column_orderby_list": "*"}

# task = {"service": "Update", "Table_name": "Email","data":[{
# "Email_Code":"M12",
# "Email_Nom":"Email2-modifff",
# "Email_To":"","Email_CC":"",
# "Email_Subject":"dede",
# "Email_Body":"cfef",
# "Email_Attachement":"",
# "Email_Description":"",
# "To_Internal":"user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
# "CC_Internl":"user1@email.com,12345678@sms.com,12345678,test 7 @email.com,test 7 fax,test 11 @email.com,test 11sms,test 11 fax",
# "Report_FactBook":"",
# "DBAction":"2"}]
#         }



# task = {"service": "Update", "Table_name": "Alarme_F_Reporting",
#  "data":
#  [{'Alarme_Code': 'A0000',
#   'Compteur_Incident': 'CC117$28',
#    'Formule': 'CC117$0~+~#6~',
#     'Parsed_Formule': 'E117$28=CC117$0~+~#6~',
#      'Operateur': '<',
#       'Objectif': {'U_inputobjective': 'objective : 78', 'Sys_inputobjective': [{'att': None, 
#  'valeur': [{'type': 'r', 'content': '78'}], 'keyword': None, 'operateur': None, 'user_value': None}]},
#       'Frequence': {'Frequence': {'NbUnite': '2', 'Periode': 'Temps_Reel', 'UniteTemp': 'Min',
#         'FrequenceUser': '2_Min'}, 
#         'OperateurValue': [{'att': 'Entre', 'valeur': " ''10:38'' and ''10:43''", 'keyword': 'Intervalle', 'operateur': 'Inclure', 'valeur_format': 'time'}], 
#         'UserInterface': ['Inclure Periode 10:38 ,10:43 ']},
#         'Next_Check': '16/06/2021 00:00:00', 
# 'U_Alarme_Name': 'eeeeeeeeeenourhene',
#          'Description': 'Abattage Elec$INC_LIVE=Abattage Elec$KWh^J~+~#6~',
#           'U_Compteur': 'Abattage Elec$INC_LIVE',
#            'U_Formule': 'Abattage Elec$INC_LIVE=Abattage Elec$KWh^J~+~#6~',
#             'Nbr_Error': 0, 'TAG_Formule': 'TAG : ', 'DBAction': '1'}]

# }


# task = {"service":"Update","Table_name":"Alarme_F_Reporting","data":[{'Alarme_Code': ['A890'], 'Compteur_Incident': 'CC117$28', 'Formule': 'CC117$2~', 'Parsed_Formule': 'E117$28=CC117$2~', 'Operateur': '<', 'Objectif': '{"U_inputobjective":"objective : 78","Sys_inputobjective":[{"keyword":null,"operateur":null,"att":null,"valeur":[{"type":"r","content":"78"}],"user_value":null}]}', 'Frequence': {'Frequence': '3_Min', 'OperateurValue': [{'keyword': 'Intervalle', 'operateur': 'Inclure', 'att': 'Entre', 'valeur': "''14:37'' and ''20:42''", 'valeur_format': 'time'}], 'UserInterface': ['Inclure Periode 14:37 ,20:42 ']}, 'Next_Check': '05/06/2021 00:00:00', 'U_Alarme_Name': 'test7', 'Description': 'Abattage Elec$INC_LIVE=Abattage Elec$KWh^M~', 'U_Compteur': 'Abattage Elec$INC_LIVE', 'U_Formule': 'Abattage Elec$INC_LIVE=Abattage Elec$KWh^M~', 'Nbr_Error': '0', 'TAG_Formule': 'TAG : ', 'DBAction': '2'}]}

r1 = tasker.execTask(task)
r1 = json.loads(r1)
print(r1)
# {'Frequence': {'Frequence': {'NbUnite': '2', 'Periode': 'Temps_Reel', 'UniteTemp': 'Min',
#         'FrequenceUser': '2_Min'}, 



#          'UserInterface': ['Inclure Periode 10:38 ,10:43 ']},

# 'Frequence': {'Frequence': {'NbUnite': '56', 'Periode': 'Temps_Reel', 'UniteTemp': 'Min', 
#  'FrequenceUser': '56_Min'},
#   'UserInterface': ['Inclure Periode 15:55 ,21:53 '], 


# 'OperateurValue': [{
#         'keyword': 'Intervalle',
#          'operateur': 'Inclure',
#           'att': 'Entre',
 #           'valeur': "''10:38'' and ''10:43''",
#             'valeur_format': 'time'}], 
       
#   'OperateurValue': [{
#           'att': 'Entre',
  #          'valeur': "'15:55' and '21:53'",
#             'keyword': 'Intervalle',
#              'operateur': 'Inclure',
#               'valeur_format': 'time'}]},
 

# }       