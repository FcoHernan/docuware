import http.client
import json
import pandas as pd
from datetime import datetime
import pandas as pd
#import pyarrow as pa
#import pyarrow.parquet as pq

# ************************************************************************************************************************************
print('0. Variables y clases')
# ************************************************************************************************************************************

cookie = ".DWPLATFORMAUTH=49C233E68BF74FF3BBACC5DFC98D97338F58522BA18958BA473F6635D55EB38E09BE52F5E65A269049550EE27FB6E93BA9B84B55B23DD20A2D6F8E398DCF539E14CE57C81138C9D0393A71EBE934FDD816A83E802A697E9545CF59D8E2A4FCCD30AA7B14FEEB93E4A1CC87F14CAE4C7D2910B2E4CD7ED1684ED143ED8F9CD2D45509DE1E6538AA4DB221BC785CBC4D66F498DE63C8D2827DEA0FCDED8DC4817D57F647E5002E5580D61EB8606D7DFBAF7B462CC5D70791A09C8972033DD4F926BFB82FE8773830BB379CD6264BFEDFF74FAA9FFC3758FF1B6DC0300D852B77BB1118D46B30D2FD904CEE9A65A8BD348A098293CCEC4376AB36C92CABED3DB5DA789CF916E4DB2A005B8AE63450A3A254062FD6AE5547189B74E191DC47C6881432F18E0D0D4D8595E908385C8BC5DD59FC9FE46BF5A04BF0F573E2612188F29AA886D918B35F693AE0B0824871C17BAABF7F246A20A927062D87827B0593AC714A41CAA17EAE14723A19113D56F8DFCADB765653C543E82AE6EB5F13C64C05B427E4B840CE5ECCDB001EF5802722C171; DWPLATFORMBROWSERID=C1BDE9D45B1C908FFED7F5F3B5F46932C006D4AE5425AD3F8DBDF6340CE55BC1FC964506C7B3DE993B4367B4893E0A72C0985A8A04F4EA01555E7BC5B3C6381F49E4DE1C535CB448A70A9E665A15BCAC9C2D371CCF44AE7E29DAEF5DB7D820133CFED03BBB05BD7DE59FAE2D4287E0CB1EF8C7B4CC63EB21B8AFF927B7E4235312F59F8C65F15A9D23D2A34F22E51349E375A1A1DA6DFC7C407DF5350DE84B9E80F0DDA38E048C690558F18AA09018A73562717AD2251C0191321E00759CB2F476962CEAD617C83C0504BAE6F54F363F; dwingressplatform=1709299732.693.28.375375|559807ac2ae9d06d49a5f0e5a708bdd9"
urlDocuwareCloud = "tecnofast.docuware.cloud"
username = "gov_ms"
password = "g2328m"
organization_Name = "TECNOFAST"
fileCabinet_Name = "Compras - FE - Tecno Fast Montajes SPA"
documents_count_return = 5

class dialogInfo:
    def __init__(self, id_value, is_default, type):
        self.id_value = id_value
        self.is_default = is_default
        self.type = type


class DocumentInfo:
    def __init__(self, dw_doc_id, wf_en_actividad):
        self.dw_doc_id = dw_doc_id
        self.WfEnActividad = wf_en_actividad


class DocumentWorkflowInfo:
    def __init__(self, dw_doc_id, workflow_instance_id, workflow_id, name, version):
        self.dw_doc_id = dw_doc_id
        self.workflow_instance_id = workflow_instance_id
        self.workflow_id = workflow_id
        self.name = name
        self.version = version


class DocumentWorkflowStep:
    def __init__(self, dw_doc_id, step_number, step_date, activity_name, activity_type):
        self.dw_doc_id = dw_doc_id
        self.step_number = step_number
        self.step_date = step_date
        self.activity_name = activity_name
        self.activity_type = activity_type

# Crear un DataFrame vacío con la estructura deseada
columnasDocumeto = ['doc_id', 'tipo', 'rut_proveedor', 'no_factura', 'fecha_emision', 'no_oc', 'total_neto', 'tipo _solicitud', 'tipo_soc', 'no_solicitud_compra','nombre_proveedor','condiciones_pago','empresa','no_notacredito','no_guiadespacho','estado_fact','fecha_estado','usuario_solicitante','estado','link_doc','wf_fecha_inicio','wf_en_actividad','wf_usuario_actual','created_at','updated_at']
df_documento = pd.DataFrame(columns=columnasDocumeto)

# Crear un DataFrame vacío con la estructura deseada
columnas = ['id', 'doc_id', 'flujo', 'tipo', 'nombre', 'usuario', 'operacion', 'comentario', 'fecha', 'estado', 'create_at', 'update_at']
df = pd.DataFrame(columns=columnas)

#PARA CARGAR AL DF
#df = df.append(nuevo_elemento, ignore_index=True)


# ************************************************************************************************************************************
print('1. Get Responsible Identity Service')
# ************************************************************************************************************************************

conn = http.client.HTTPSConnection(urlDocuwareCloud)
payload = ''
headers = {
    'Accept': 'application/json',
    'Cookie': cookie
}
conn.request("GET", "/DocuWare/Platform/Home/IdentityServiceInfo", payload, headers)
res = conn.getresponse()
data = res.read()

# Decodificar la respuesta JSON en un diccionario
response_dict = json.loads(data)

# Obtener la URL que indica el servicio de identidad de Docuware para el cliente
identity_service_url = response_dict["IdentityServiceUrl"]

# Dividir la URL para obtener el dominio y el identificador
domain_identity_service_url = identity_service_url.split("/", 3)[2]
identifier_identity_service_url = identity_service_url.split("/", 3)[3]

# ************************************************************************************************************************************
print('2. Get Identity Service Configuration')
# ************************************************************************************************************************************

conn = http.client.HTTPSConnection(domain_identity_service_url)
conn.request("GET", f"/{identifier_identity_service_url}/.well-known/openid-configuration", payload, headers)
res = conn.getresponse()
data = res.read()

# Decodificar la respuesta JSON en un diccionario
response_dict = json.loads(data)

# Obtener la URL que indica el servicio de token de Docuware para el cliente
token_service_url = response_dict["token_endpoint"]

# Dividir la URL para obtener el dominio y el endpoint para obtener el token
domain_token_service_url = token_service_url.split("/", 3)[2]
endpoint_token_service_url = token_service_url.split("/", 3)[3]

# ************************************************************************************************************************************
print('3.a Request Token w/ Username & Password')
# ************************************************************************************************************************************

conn = http.client.HTTPSConnection(domain_token_service_url)
payload = f'grant_type=password&scope=docuware.platform&client_id=docuware.platform.net.client&username={username}&password={password}'
headers = {
    'Accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded'
}
conn.request("POST", f"/{endpoint_token_service_url}", payload, headers)
res = conn.getresponse()
data = res.read()

# Decodificar la respuesta JSON en un diccionario
response_dict = json.loads(data)

# Obtener el Token
token = response_dict["access_token"]

def getDocumentsAndWorkflowDocumentHistoryAndSteps(fileCabinet_Identifier, search_dialog_id_value):
    print('7. Obtener los documentos de un archivador por cuadro de diálogo de búsqueda')
    conn = http.client.HTTPSConnection(urlDocuwareCloud)
    payload = json.dumps({
        "Operation": "And"
    })
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}',
        'Cookie': cookie
    }
    #print(fileCabinet_Identifier)
    #print(search_dialog_id_value)
    conn.request("POST",
                 f"/DocuWare/Platform/FileCabinets/{fileCabinet_Identifier}/Query/DialogExpression?Count={documents_count_return}&DialogId={search_dialog_id_value}",
                 payload, headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))

    # Decodificar la respuesta JSON en un diccionario
    response_dict = json.loads(data)

    # Define una lista para almacenar las instancias del modelo
    document_info_list = []

    # Itera sobre cada elemento en la lista "Items" de la respuesta JSON
    for item in response_dict["Items"]:
        #print(item)
        #print('FileCabinetId: ' + item["FileCabinetId"])
        dw_doc_id = None
        wf_en_actividad = None

        # Itera sobre cada campo en "Fields" para encontrar los valores correspondientes
        for field in item["Fields"]:
            #print(field)
            if field["FieldName"] == "DWDOCID":
                dw_doc_id = field["Item"]
                df_doc_id = str(dw_doc_id)
                print('df_doc_id :' + df_doc_id)
            elif field["FieldName"] == "WFENACTIVIDAD":
                wf_en_actividad = field["Item"]
                print(wf_en_actividad)
            #elif #Agregar todas las extracciones error a proposito

        # Crea una instancia del modelo con los valores encontrados y agrégala a la lista
        document_info = DocumentInfo(dw_doc_id, wf_en_actividad)
        document_info_list.append(document_info)

    print('8. Obtener el historial de cada documento')
    response_dicts = []

    for document_info in document_info_list:
        conn.request("GET",
                     f"/DocuWare/Platform/FileCabinets/{fileCabinet_Identifier}/Documents/{document_info.dw_doc_id}/WorkflowHistory",
                     payload, headers)
        res = conn.getresponse()
        data = res.read()
        #print(data.decode("utf-8"))
        # Decodificar la respuesta JSON en un diccionario
        response_dict = json.loads(data)
        #print(response_dict)
        # Agregar el DwDocId al diccionario
        response_dict["DwDocId"] = document_info.dw_doc_id
        # Agregar el diccionario a la lista
        response_dicts.append(response_dict)
    # Define una lista para almacenar las instancias del modelo
    document_workflow_info_list = []

    # Recorrer la lista de diccionarios de respuesta
    for response_dict in response_dicts:
        # Obtener la lista de instancias de historial de workflow
        instance_history_list = response_dict.get("InstanceHistory", [])
        dw_doc_id = response_dict.get("DwDocId", [])

        # Recorrer cada instancia de historial de workflow
        for instance_history in instance_history_list:
            df_flujo = instance_history.get("Name", "")
            #print('df_flujo :' + df_flujo)
            #print(instance_history)
            # Obtener los valores de WorkflowId, Name y Version
            workflow_instance_id = instance_history.get("Id", "")
            workflow_id = instance_history.get("WorkflowId", "")
            name = instance_history.get("Name", "")
            version = instance_history.get("Version", "")
            # Crear objeto y agregarlo a la lista
            document_workflow_info = DocumentWorkflowInfo(dw_doc_id, workflow_instance_id, workflow_id, name,
                                                          version)
            document_workflow_info_list.append(document_workflow_info)   

    print('9. Obtener los pasos del historial de cada documento')
    response_dicts = []

    for document_workflow_info in document_workflow_info_list:
        conn.request("GET",
                     f"/DocuWare/Platform/Workflows/{document_workflow_info.workflow_id}/Instances/{document_workflow_info.workflow_instance_id}/History",
                     payload, headers)
        res = conn.getresponse()
        data = res.read()
        # Decodificar la respuesta JSON en un diccionario
        response_dict = json.loads(data)
        print(data)
        # Agregar el DwDocId al diccionario
        response_dict["DwDocId"] = document_workflow_info.dw_doc_id
        # Agregar el diccionario a la lista
        response_dicts.append(response_dict)
    
    # Define una lista para almacenar las instancias del modelo
    document_workflow_step_list = []

    # Recorrer la lista de diccionarios de respuesta
    for response_dict in response_dicts:
        # Obtener la lista de instancias de los pasos del historial de workflow
        instance_history_step_list = response_dict.get("HistorySteps", [])
        dw_doc_id = response_dict.get("DwDocId", [])

        # Recorrer cada instancia de los pasos del historial de workflow
        for instance_history_step in instance_history_step_list:
            if instance_history_step.get("ActivityType", "") in ('Start', 'General Task', 'TimeoutHistoryStep', 'End'):
                # Obtener los valores de WorkflowId, Name y Version
                step_number = instance_history_step.get("StepNumber", "")
                step_date = instance_history_step.get("StepDate", "")
                activity_name = instance_history_step.get("ActivityName", "")
                activity_type = instance_history_step.get("ActivityType", "")
                # Crear objeto y agregarlo a la lista
                document_workflow_step = DocumentWorkflowStep(dw_doc_id, step_number, step_date, activity_name,
                                                              activity_type)
                document_workflow_step_list.append(document_workflow_step)
    
    # Lista para almacenar los datos de los pasos del flujo de trabajo
    workflow_step_data = []
    # Iterar sobre la lista de pasos del flujo de trabajo
    for step in document_workflow_step_list:
        step_date = datetime.utcfromtimestamp(int(step.step_date[6:-2])/1000).strftime('%Y-%m-%d %H:%M:%S')
        # Crear un diccionario para almacenar los datos del paso
        step_data = {
            "Identificador del documento": step.dw_doc_id,
            "Paso número": step.step_number,
            "Fecha": step_date,
            "Nombre actividad": step.activity_name,
            "Tipo actividad": step.activity_type
        }
        # Agregar el diccionario a la lista
        workflow_step_data.append(step_data)

    # Convertir la lista de diccionarios en un DataFrame de Pandas
    df = pd.DataFrame(workflow_step_data)

    # Guardar el DataFrame en formato Parquet
    #pq.write_table(pa.Table.from_pandas(df), 'workflow_steps.parquet')

    df_nuevo = {
        'id_numeros': 1,
        'doc_id': df_doc_id,
        'flujo': df_flujo,
        'tipo': 'Tipo 1',
        'nombre': 'Nombre 1',
        'usuario': 'Usuario 1',
        'operacion': 'Operacion 1',
        'comentario': 'Comentario 1',
        'fecha': datetime(2022, 1, 1),
        'estado': 'Activo',
        'create_at': datetime(2022, 1, 1),
        'update_at': datetime(2022, 1, 1)
    }

    #df = df.append(df_nuevo, ignore_index=True)

def getDialogExpression(fileCabinet_Identifier):
    print('6. Obtener cuadro de diálogo de búsqueda predeterminado del archivador')
    #print('fileCabinet_Identifier: ' + fileCabinet_Identifier)
    conn = http.client.HTTPSConnection(urlDocuwareCloud)
    payload = ''
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Cookie': cookie
    }
    conn.request("GET", f"/DocuWare/Platform/FileCabinets/{fileCabinet_Identifier}/Dialogs", payload, headers)
    res = conn.getresponse()
    data = res.read()
    #print(data.decode("utf-8"))

    # Decodificar la respuesta JSON en un diccionario
    response_dict = json.loads(data)

    # Define una lista para almacenar las instancias del modelo
    dialog_info_list = []

    # Iterar sobre cada objeto en "Dialog"
    for obj in response_dict["Dialog"]:
        id_value = obj["Id"]
        is_default = obj["IsDefault"]
        type = obj["Type"]
        # Crear objeto y agregarlo a la lista
        dialog_info = dialogInfo(id_value, is_default, type)
        dialog_info_list.append(dialog_info)

    # Define un objeto para almacenar el resultado de la búsqueda
    search_dialog = []

    # Obtener el dialogo por el tipo y si es el de búsqueda predeterminado
    for item in dialog_info_list:
        if item.is_default and item.type == "Search":
            search_dialog = item
            getDocumentsAndWorkflowDocumentHistoryAndSteps(fileCabinet_Identifier, search_dialog.id_value)

def getFileCabinets(organization_guid):
    print('5. Obtener los archivadores de la organización')
    conn = http.client.HTTPSConnection(urlDocuwareCloud)
    payload = ''
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Cookie': cookie
    }
    conn.request("GET", f"/DocuWare/Platform/FileCabinets?OrgId={organization_guid}", payload, headers)
    res = conn.getresponse()
    data = res.read()

    # Decodificar la respuesta JSON en un diccionario
    response_dict = json.loads(data)

    # Crear un diccionario para almacenar Id y Name
    fileCabinets = {}

    # Iterar sobre cada objeto en "FileCabinet"
    for obj in response_dict["FileCabinet"]:
        id_value = obj["Id"]
        name_value = obj["Name"]
        fileCabinets[id_value] = name_value

    # Quitar comentario si desea visualizar todos los archivadores y sus identificadores por pantalla
    # for key, value in fileCabinets.items():
    #    print(f"Id: {key}, Name: {value}")

    # Obtener el archivador por el nombre, dentro de todos los archivadores
    #print(fileCabinets)
    for key, value in fileCabinets.items():
        #print(key + ' >>>>>> ' + value)
        #Eliminar IF cuando ya se pueda ver todos los cabinets
        if value == fileCabinet_Name:
            fileCabinet_Identifier = key
            getDialogExpression(key)
            break

def getOrganization():
    print('4. Obtener el identificador de la organización')
    conn = http.client.HTTPSConnection(urlDocuwareCloud)
    payload = ''
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}',
        'Cookie': cookie
    }
    conn.request("GET", "/DocuWare/Platform/Organizations", payload, headers)
    res = conn.getresponse()
    data = res.read()

    # Decodificar la respuesta JSON en un diccionario
    response_dict = json.loads(data)

    # Recorrer los elementos en la lista de organizaciones
    for organization in response_dict.get("Organization", []):
        # OJO solo se verifica 1 organizacion, si hay mas de una hay que anidar todo el proceso dentro de este
        # Porque despues de la organizacion hay que buscar todos los cabinet de cada uno
        # Verificar si el nombre de la organización es igual a la variable
        getFileCabinets(organization.get("Guid"))
        #if organization.get("Name") == organization_Name:
            #organization_guid = organization.get("Guid")
            #break

    #print('Nombre de la organizacion: ' + organization['Name'])
    #print('GUID de la organizacion: ' + organization['Guid'])
    #print('ID de la organizacion: ' + organization['Id'])

getOrganization()


##version 2024-03-15