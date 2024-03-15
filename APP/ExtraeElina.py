import azure.functions as func
import logging
import pandas as pd
import requests
import json
from io import StringIO, BytesIO
from azure.storage.blob import BlobServiceClient, __version__
import pyarrow as pa
import pyarrow.parquet as pq
from pandas import json_normalize
pd.set_option('display.max_columns', None)
import os
from datetime import datetime , timedelta


def Func_Extrae_Elina(req: func.HttpRequest) -> func.HttpResponse:
    #Definition var
    
    PARAMETROS = json.loads(req.get_body())

    # FECHAS YYYY-MMM-DD
    FECHAINICIO =      str(PARAMETROS["FECHAINICIO"])                                            #parametro
    FECHAFIN =         str(PARAMETROS["FECHAFIN"])                                               #parametro

    FECHAS = datetime.strptime(FECHAINICIO,'%Y-%m-%d')
    FECHASFIN = datetime.strptime(FECHAFIN,'%Y-%m-%d')
    DIA = FECHAS.day
    MES = FECHAS.month
    ANIO = FECHAS.year


    STORAGEACCOUNTURL= os.environ['STORAGEACCOUNTURL']                                           #parametro
    STORAGEACCOUNTKEY= os.environ['STORAGEACCOUNTKEY']                                           #parametro
    CONTAINERNAME=     os.environ['CONTAINERNAME']                                               #parametro
    ARCHIVENAME =      PARAMETROS["ARCHIVENAMEGUESTS"]                                           #parametro


    for FECHAACTUAL in range((FECHASFIN - FECHAS).days + 1):
        fecha = FECHAS + timedelta(days=FECHAACTUAL)
        FECHAINICIO = fecha.strftime('%Y-%m-%d')
        FECHAS = datetime.strptime(FECHAINICIO,'%Y-%m-%d')
        DIA = FECHAS.day
        MES = FECHAS.month
        ANIO = FECHAS.year
        BLOBDESTINY=       os.environ['BLOBDESTINY']+str(ANIO)+'/'+str(MES)+'/'+str(DIA)+'/'         #parametro
        # Lugar 
        LUGAR = str(os.environ['LUGAR'])                                                             #parametro

        #FUnction Upload
        def upload(df):
            BLOBDESTINY=  os.environ['BLOBDESTINY']+ARCHIVENAME+'/'+str(ANIO)+'/'+str(MES)+'/'+str(DIA)+'/'         #parametro
            blob_destiny_out = f"{BLOBDESTINY}{ARCHIVENAME}.parquet"    
             #Blob specified client (to upload data)  
            print('Upload')
            print('STORAGEACCOUNTURL '+STORAGEACCOUNTURL)  
            print('STORAGEACCOUNTKEY' + STORAGEACCOUNTKEY)
            blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
            blob_client = blob_service_client_instance.get_blob_client(CONTAINERNAME, blob_destiny_out)     
             #Upload blob
            logging.info('Compienza upload en '+ blob_destiny_out)
            output = BytesIO()
            df.to_parquet(output, engine='pyarrow')
            output.seek(0)
            workbook = output.read()
            blob_client.upload_blob(workbook, overwrite=True)         
            print ("All archives upload")
            return ("All archives upload")
        

        logging.info('Python HTTP trigger function processed a request.')
    #----------------------------------------------------------------
        logging.info('Entro al try')
        logging.info("Azure Blob Storage v" + __version__ + " - Python quickstart sample")
        USERNAMES = os.environ['USERNAMES']
        CONTRASENA = os.environ['CONTRASENA']
    #Creacion de PostMan
        url = "https://tb"+LUGAR+"-services.elinapms.com/api/reservation/list?startDate1="+FECHAINICIO+"&startDate2="+FECHAINICIO+" 23:59:59&include=1&include=2&include=3&include=6"

        payload = {}
        headers = {
          'X-Auth-Username': USERNAMES,                       #parametro
          'Authorization':   CONTRASENA                       #parametro
        }

        response = requests.request("GET", url, headers=headers, data=payload)

    
        #print(response.text)

        # LECTURA DE JSON Y SU MUESTRA EN DATA FRAME
        df1= response.json()

        guests = pd.json_normalize(df1, 'guests', 'id', sep='_').drop('customFields', axis=1)
        casos = pd.json_normalize(df1, sep='_').drop(['guests', 'guest_customFields'], axis=1)

        guests.columns = ['guests_'+col for col in guests.columns]
        guests = guests.rename(columns={'guests_id': 'reserva_id'})

        print(guests)
    
        #df_final.to_parquet('test.parquet', index=False)

    #---------------------------------------------------------------------
        TipoGets = guests.columns.values.tolist()
        print(TipoGets)

        #guests['guests_personId'] = guests['guests_personId'].astype(int)
        #guests['guests_reservationCount'] = guests ['guests_reservationCount'].astype(int)

        #guests['guests_reservationLastStay'] = pd.to_datetime(guests['guests_reservationLastStay'], format='%d-%m-%Y %H:%M:%S')
        #guests['guests_createdTime'] = pd.to_datetime(guests['guests_createdTime'], format='%Y-%m-%dT%H:%M:%S.%f')
        #guests['guests_lastChange'] = pd.to_datetime(guests['guests_lastChange'], format='%Y-%m-%dT%H:%M:%S.%f')

        #casos['startDate'] = pd.to_datetime(casos['startDate'],format='%Y-%m-%dT%H:%M:%S')
        #casos['endDate'] = pd.to_datetime(casos['endDate'],format='%Y-%m-%dT%H:%M:%S')
        #casos['checkinTime'] = pd.to_datetime(casos['checkinTime'], format= '%d-%m-%Y %H:%M:%S')
        #casos['checkoutTime'] = pd.to_datetime(casos['checkoutTime'],format='%d-%m-%Y %H:%M:%S')

        #casos['guestNum'] = casos['guestNum'].astype(int)

        #casos['creationDate'] = pd.to_datetime(casos['creationDate'],format='%Y-%m-%dT%H:%M:%S.%f')
        #casos['lastChange'] = pd.to_datetime(casos['lastChange'],format='%Y-%m-%dT%H:%M:%S.%f')


        #tiposdedatos = guests.dtypes
        #tiposdedatosCasos = casos.dtypes
        #print(tiposdedatos)
        #print(tiposdedatosCasos)
    
   
        #df_final = guests.merge(casos, how='left', left_on='guests_id', right_on='id')
   
  
        #----------------------------------------------------------------------company
        # Inicializar una lista para almacenar todos los datos normalizados
        all_company_json = []

    # Iterar sobre los elementos de la lista df1
        for data in df1:
            # Seleccionar la columna 'company' de cada elemento y convertir a JSON normalizado
            company_data = data['company']
            company_json = pd.json_normalize(company_data, sep='_')
            all_company_json.append(company_json)

        # Concatenar todos los JSON normalizados en un solo DataFrame
        all_company_df = pd.concat(all_company_json, ignore_index=True)
        # Eliminar la columna 'customfields'
        all_company_df = all_company_df.drop(columns= 'customFields')
        all_company_df = all_company_df.drop_duplicates()
        all_company_df.reset_index(drop=True, inplace=True)


    #-------------------------------------------------------------------guest

    # Inicializar una lista para almacenar todos los datos normalizados
        all_gests_json = []

    # Iterar sobre los elementos de la lista df1
        for data in df1:
            # Seleccionar la columna 'guests' de cada elemento y convertir a JSON normalizado
            gests_data = data['guest']
            gests_json = pd.json_normalize(gests_data, sep='_')
            all_gests_json.append(gests_json)

        # Concatenar todos los JSON normalizados en un solo DataFrame
        all_gests_df = pd.concat(all_gests_json, ignore_index=True)
        # Eliminar la columna 'customfields'
        all_gests_df = all_gests_df.drop(columns= 'customFields')

    #-------------------------------------------------------------------guest
        df = pd.DataFrame(df1)

        BookingDF = df[['id','bookingConfirmation','externalBookingConfirmation','startDate','endDate','checkinTime'
                   ,'checkoutTime','guestNum','currentStatus','accommodation','area','type','accomodationId',
                  'source','bookingTotal','packageTotal','currency', 'guestId','companyId','creationDate','lastChange']]



    #---------------------------------------------------------------------------------
        print(STORAGEACCOUNTURL)
        print(STORAGEACCOUNTURL)
        ARCHIVENAME = PARAMETROS["ARCHIVENAMEGUESTS"] 
        upload(guests)
        ARCHIVENAME = PARAMETROS["ARCHIVENAMECOMPANY"] 
        upload(all_company_df)
        ARCHIVENAME = PARAMETROS["ARCHIVENAMEGUEST"] 
        upload(all_gests_df)
        ARCHIVENAME = PARAMETROS["ARCHIVENAMEBOOKING"]
        upload(BookingDF)
        print("Archivo Parquet creado exitosamente.")   


    #---------------------------------------------------------------------------------
    name = req.params.get('name') 
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )