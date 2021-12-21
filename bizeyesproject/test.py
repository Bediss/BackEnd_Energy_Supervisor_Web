
def display(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        fields = data['fields']
        content = data['content']
        identifier = data['identifier']
        print(identifier)

        data["service"] = "display"
        tasker = TaskManager(taskQueue="tasks_queue",
                             exchangeName="taskRouter")

        task = {"service": "display", "Table_name": tablename, "Header_list": fields, "Header_value": content,
                "Column_select_liste": "", "Column_condition_select_list": "", "Column_orderby_list": ""}

        data = tasker.execTask(task)
        data = json.loads(data)
    return(HttpResponse(data, content_type="application/json"))

def updatedelete(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        datatomodified = data['datatomodified']

        print(datatomodified)

        task = {
            "service": "Update",
            "Table_name": tablename,
            "data": datatomodified
        }

        tasker = TaskManager(taskQueue="tasks_queue",
                             exchangeName="taskRouter")

        data = tasker.execTask(task)
        data = json.loads(data)
        
    return(HttpResponse(data,content_type="application/json"))

def sendnewid(request):
    if request.method == 'POST':
        data = json.loads(request.body)

        tablename = data['tablename']
        nombreid = data['nombermaxcode']

        task = {"service": "GetMaxCode",
                "Table_name": tablename, "nbr_code": nombreid}

        tasker = TaskManager(taskQueue="tasks_queue",
                             exchangeName="taskRouter")

        resp = tasker.execTask(task)
        resp = resp.replace(b"[", b"")
        resp = resp.replace(b"]", b"")
        resp = resp.split(b",")
        print(resp)
    return HttpResponse(resp)

