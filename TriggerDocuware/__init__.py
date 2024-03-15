import azure.functions as func
from APP import ExtraeDocuware
 
def main(req: func.HttpRequest) -> func.HttpResponse:
   
    req_body = req.get_json()
    ExtraeDocuware.getOrganization(req_body) #modificar cuando tenga la funcion
 
    return func.HttpResponse(f"This HTTP triggered function executed successfully.")