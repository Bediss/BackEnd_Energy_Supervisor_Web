from projectapp.apis.common import *
from .loginMgr import jwtVerifyRequest
import requests

def generatePDF(request):
    if request.method == 'POST':
        isValid=jwtVerifyRequest(request)
        if type(isValid) !=dict:
                return isValid
        try:
            body = json.loads(request.body)
        except:
            return (HttpResponseBadRequest())
        resPDF = requests.post("http://{}:{}/".format(pdfServer, pdfGeneratorPort), json=body)

        if resPDF.ok:
            response = HttpResponse(content_type='application/pdf')
            response['Content-Disposition'] = 'attachment; filename="somefilename.pdf"'
            response.write(resPDF.content)

            return response
    return HttpResponseBadRequest()