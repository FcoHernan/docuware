import azure.functions as func
from APP import ExtraeElina
 
def main(req: func.HttpRequest) -> func.HttpResponse:
   
    req_body = req.get_json()
    ExtraeElina.Func_Extrae_Elina(req_body)
 
    return func.HttpResponse(f"This HTTP triggered function executed successfully.")