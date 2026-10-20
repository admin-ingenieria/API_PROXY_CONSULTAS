import pandas as pd
import cx_Oracle
import mysql.connector as mysql
import pyodbc
from concurrent.futures import ThreadPoolExecutor
import time
import datetime
from datetime import timedelta
import json
import os
import pandas_gbq
from google.oauth2 import service_account
import traceback
from controllers.subcipt_mqtt import mqtt_handler
from config import BASE_DIR, requerir, opcional

# cx_Oracle.init_oracle_client(lib_dir=r"C:\app\oracle_11g_64\product\11.2.0\client_1", config_dir=r"C:\app\oracle_11g_64\product\11.2.0\client_1\network\admin\tnsnames.ora")


def conectar_zeus():
    return mysql.connect(host=requerir('MYSQL_HOST'), port=opcional('MYSQL_PORT', '3306'),
                         user=requerir('MYSQL_USER'), password=requerir('MYSQL_PASSWORD'),
                         database=opcional('MYSQL_DATABASE', 'zeusdb'), ssl_disabled=False)


def ruta_query(nombre):
    return os.path.join(BASE_DIR, 'querys', nombre)


def anulaciones():
    start_funcion = time.time()

    # ZEUS
    cnxn_zeus = conectar_zeus()

    finish_connection = time.time()

    query_cambio_estado_read = open(
        r'./querys/query_anulaciones.txt').read()

    finish_procesamiento_query = time.time()

    executor = ThreadPoolExecutor(max_workers=5)

    df_cambio_estado = executor.submit(
        pd.read_sql, query_cambio_estado_read, cnxn_zeus).result()

    df_cambio_estado['ultima_fecha'] = pd.to_datetime(df_cambio_estado['ultima_fecha']).dt.strftime('%Y-%m-%d %H:%M')
    
    finish_consultas_bd = time.time()

    cnxn_zeus.close()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")

    return df_cambio_estado

def bot_rutas_masivas_tms():
    start_funcion = time.time()

    # ZEUS
    cnxn_zeus = conectar_zeus()

    dsn_srx = cx_Oracle.makedsn(
        host=requerir('ORACLE_BTL_HOST'),
        port=opcional('ORACLE_BTL_PORT', '1531'),
        service_name=requerir('ORACLE_BTL_SERVICE'))
    cnxn_btl = cx_Oracle.connect(user=requerir('ORACLE_BTL_USER'), password=requerir('ORACLE_BTL_PASSWORD'), dsn=dsn_srx)
    
    finish_connection = time.time()

    query = open(
        r'./querys/query_bot_rutas_masivas_tms.txt').read()
    query_btl = open(
        r'./querys/query_bot_rutas_masivas_tms_btl.txt').read()
    query_btl_log = open(
        r'./querys/query_bot_rutas_masivas_tms_btl_log.txt').read()

    finish_procesamiento_query = time.time()

    ##OBTENER DATA
    executor = ThreadPoolExecutor(max_workers=5)

    df = executor.submit(
        pd.read_sql, query, cnxn_zeus).result()
    
    
    query_log = """
    select distinct idJob,fecha_envio,placa,mensajero,codigo_recepcion AS codigoRecepcion ,cantidad_envios,
    response as response_tms,descripcion,estado as estado_tms,tipoRecepcion from rpa_ingenieria_test.rutas_tms_log rtl
   where 1=1 and fecha_envio >= curdate()
    """
    df_log = executor.submit(pd.read_sql, query_log, cnxn_zeus).result()
    
    df_btl = executor.submit(pd.read_sql, query_btl, cnxn_btl).result()
    
    df_btl_log = executor.submit(pd.read_sql, query_btl_log, cnxn_zeus).result()
    
    
    
    
    
    cnxn_btl.close()
    cnxn_zeus.close()
    
    ##PROCESAMIENTO DE DATA
    try:
        df_btl = pd.merge(df_btl, df_btl_log, left_on = 'PLACA', right_on='placa')
    except:
        df_btl['log_btl'] = ''
        
    print(df_btl)
    df_btl = df_btl[df_btl['tipoRecepcion'] != 'BTL-PROVINCIA']
    
    ###BOT RUTAS UM ZEUS
    df_rutas = df.drop_duplicates(subset=['codigoRecepcion']).reset_index(drop=True)
    df_rutas = pd.concat([df_rutas,df_log])
    df_rutas = df_rutas.drop_duplicates(subset=['codigoRecepcion']).reset_index(drop=True)
    df_bot = pd.DataFrame()
    df_sorted = df.sort_values(by='fechaEnvio_tms', ascending=False)
    last_records = df_sorted.drop_duplicates(subset='fechaEnvio_tms', keep='last').reset_index(drop=True)
    df_bot['ultima_fecha_bot_zeus'] = last_records['fechaEnvio_tms']
    df_bot['total_folios_pendiente_zeus'] = len(df[df['estado_tms']!='Enviado'])
    df_bot['total_folios_despacho_zeus'] = len(df[df['estado_tms']=='Enviado'])
    df_bot['log_zeus'] = ";".join(df_rutas[df_rutas['response_tms'] != '']['response_tms'].astype(str))
    df_bot['rutas_env_zeus'] = len(df_rutas[df_rutas['estado_tms']=='Enviado'])
    df_bot['rutas_pendientes_zeus'] = len(df_rutas[df_rutas['estado_tms']!='Enviado'])
    df_bot['ultima_fecha_bot_zeus'] = pd.to_datetime(df_bot['ultima_fecha_bot_zeus']).dt.strftime('%Y-%m-%d %H:%M')
    
    if(df.empty):
        data = {
        'ultima_fecha_bot_zeus': '',
        'total_folios_pendiente_zeus': 0,
        'total_folios_despacho_zeus': 0,
        'log_zeus': '',
        'rutas_env_zeus': 0,
        'rutas_pendientes_zeus': 0,
    }

    # Convertir el diccionario a un string JSON
        data = json.dumps(data, indent=4)
        df_bot = pd.read_json(data, orient='index')
        print('vacio zeus')
        df_bot['ultima_fecha_bot_zeus'] = ''
        df_bot['total_folios_pendiente_zeus'] = 0
        df_bot['total_folios_despacho_zeus'] = 0
        df_bot['log_zeus'] = ''
        df_bot['rutas_env_zeus'] = 0
        df_bot['rutas_pendientes_zeus'] = 0
        df_bot['ultima_fecha_bot_zeus'] = ''

        
    finish_consultas_bd = time.time()
    print(df_rutas)
    ###BOT RUTAS UM BTL
    df_rutas = df_btl.drop_duplicates(subset=['PLACA']).reset_index(drop=True)
    df_sorted = df_btl.sort_values(by='fecha_envio', ascending=False)
    last_records = df_sorted.drop_duplicates(subset='fecha_envio', keep='last').reset_index(drop=True)
    df_bot['ultima_fecha_bot_btl'] = last_records['fecha_envio']
    df_bot['total_folios_pendiente_btl'] = len(df_btl[df_btl['estado']!='Enviado'])
    df_bot['total_folios_despacho_btl'] = len(df_btl[df_btl['estado']=='Enviado'])
    log_btl = df_rutas[df_rutas['response'] != '']
    log_btl = log_btl[log_btl['response'] != ' ']
    df_bot['log_btl'] = ";".join(log_btl['response'].astype(str))
    df_bot['rutas_env_btl'] = len(df_rutas[df_rutas['estado']=='Enviado'])
    df_bot['rutas_pendientes_btl'] = len(df_rutas[df_rutas['estado']!='Enviado'])
    df_bot['ultima_fecha_bot_btl'] = pd.to_datetime(df_bot['ultima_fecha_bot_btl']).dt.strftime('%Y-%m-%d %H:%M')
    finish_consultas_bd = time.time()
    
    print(df_bot)
    ###BOT RUTAS UM BTL
    if(df_btl_log.empty):
        print('vacio btl')
        df_bot['ultima_fecha_bot_btl'] = ''
        df_bot['total_folios_pendiente_btl'] = 0
        df_bot['total_folios_despacho_btl'] = 0
        df_bot['log_btl'] = ''
        df_bot['rutas_env_btl'] = 0
        df_bot['rutas_pendientes_btl'] = 0
        df_bot['ultima_fecha_bot_btl'] = ''

    df_bot = df_bot.iloc[[0]]

    
    df_bot.info()
    print(df_bot)

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")

    return df_bot
def bot_optimizacion_rutas():
    start_funcion = time.time()

    # ZEUS
    cnxn_zeus = conectar_zeus()

    finish_connection = time.time()

    query_cambio_estado_read = open(
        r'./querys/query_bot_optimizados_rutas.txt').read()

    finish_procesamiento_query = time.time()

    executor = ThreadPoolExecutor(max_workers=5)

    df_cambio_estado = executor.submit(
        pd.read_sql, query_cambio_estado_read, cnxn_zeus).result()

    df_cambio_estado['ultima_fecha_bot'] = pd.to_datetime(df_cambio_estado['ultima_fecha_bot']).dt.strftime('%Y-%m-%d %H:%M')
    finish_consultas_bd = time.time()

    cnxn_zeus.close()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")

    return df_cambio_estado


def cambio_estado_tms():
    start_funcion = time.time()

    # ZEUS
    cnxn_zeus = conectar_zeus()

    finish_connection = time.time()

    query_cambio_estado_read = open(
        r'./querys/query_cambio_estado_tms.txt').read()

    finish_procesamiento_query = time.time()

    executor = ThreadPoolExecutor(max_workers=5)

    df_cambio_estado = executor.submit(
        pd.read_sql, query_cambio_estado_read, cnxn_zeus).result()

    # df_cambio_estado['ultima_fecha_error_api'] = pd.to_datetime(df_cambio_estado['ultima_fecha_error_api']).dt.strftime('%Y-%m-%d %H:%M')
    df_cambio_estado['ultimo_registro_zeus'] = pd.to_datetime(df_cambio_estado['ultimo_registro_zeus']).dt.strftime('%Y-%m-%d %H:%M')
    
    finish_consultas_bd = time.time()

    cnxn_zeus.close()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")

    return df_cambio_estado


def robot_fbs():
    start_funcion = time.time()

    # ZEUS
    cnxn_zeus = conectar_zeus()

    finish_connection = time.time()

    query_robot_fbs_read = open(ruta_query('query_robot_fbs.txt')).read()

    finish_procesamiento_query = time.time()

    executor = ThreadPoolExecutor(max_workers=5)

    df_robot_fbs = executor.submit(
        pd.read_sql, query_robot_fbs_read, cnxn_zeus).result()

    df_robot_fbs['ultima_fecha_registro_bot'] = pd.to_datetime(df_robot_fbs['ultima_fecha_registro_bot']).dt.strftime('%Y-%m-%d %H:%M')
    df_robot_fbs['ultima_fecha_registro_zeus'] = pd.to_datetime(df_robot_fbs['ultima_fecha_registro_zeus']).dt.strftime('%Y-%m-%d %H:%M')
     
    finish_consultas_bd = time.time()

    cnxn_zeus.close()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")

    return df_robot_fbs


def inserciones_tms():
    start_funcion = time.time()

    # INGENIERIA
    cnxn_ing = pyodbc.connect(f"DRIVER={{SQL Server}};SERVER={requerir('SQLSERVER_HOST')};DATABASE={opcional('SQLSERVER_DATABASE', 'IngenieriaBD')};"
                              f"UID={requerir('SQLSERVER_USER')};PWD={requerir('SQLSERVER_PASSWORD')}")

    finish_connection = time.time()

    query_inserciones_tms_read = open(r'./querys/query_inserciones_tms.txt').read()

    finish_procesamiento_query = time.time()

    executor = ThreadPoolExecutor(max_workers=5)

    df_inserciones_tms = executor.submit(pd.read_sql, query_inserciones_tms_read, cnxn_ing).result()

    # df_inserciones_tms['ultima_fecha_etl'] = pd.to_datetime(df_inserciones_tms['ultima_fecha_etl']).dt.strftime('%Y-%m-%d %H:%M')
    # df_inserciones_tms['ultima_fecha_api'] = pd.to_datetime(df_inserciones_tms['ultima_fecha_api']).dt.strftime('%Y-%m-%d %H:%M')
     

    finish_consultas_bd = time.time()

    cnxn_ing.close()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")


    return df_inserciones_tms


def bot_pickup():
    start_funcion = time.time()

    def encontrar_ruta(path_file, startwith, hours):
        now = datetime.datetime.now()
        hora_inicio = now - timedelta(hours = hours)
        ahora = hora_inicio.strftime('%Y-%m-%d %H')
        print(ahora)
        lista_descargas = os.listdir(path_file)
        lista_descargas = [x for x in lista_descargas if x.startswith(startwith)]
        data_decargas = pd.DataFrame(columns=['ruta','fecha_creacion'])
        print(data_decargas)
        for desc in lista_descargas:
            path = os.path.join(path_file, desc)
            fecha = datetime.datetime.fromtimestamp(os.stat(path).st_ctime).strftime('%Y-%m-%d %H:%M:%S')
            fila = {'ruta':[path],'fecha_creacion':[fecha]}
            fila = pd.DataFrame(fila)
            data_decargas = pd.concat([data_decargas,fila], ignore_index=True)
        filtro_descargas = data_decargas[data_decargas['fecha_creacion'] >= ahora]
        filtro_descargas = filtro_descargas.sort_values(by='fecha_creacion',ascending=False)
        print('filtro_descargas',filtro_descargas)
        lista_zip = [x for x in filtro_descargas['ruta']]
        print(lista_zip)

        ruta = lista_zip[0]
    
        return ruta

    data_files = pd.DataFrame(columns=['RUTA','FECHA_MODIFICACION'])

    try:
        archivo = encontrar_ruta(path_file = requerir('PICKUP_DATA_DIR'), startwith = 'OrderGlobal', hours = 24)
        print('file',archivo)
        fecha_m = datetime.datetime.fromtimestamp(os.lstat(archivo).st_ctime).strftime('%Y-%m-%d %H:%M:%S')
        print ('fecha_m',fecha_m)
        fila = {'RUTA':[archivo],'FECHA_MODIFICACION':[fecha_m]}
        fila = pd.DataFrame(fila)
    except:
        fila = pd.DataFrame()

    try:
        archivo = requerir('PICKUP_ARCHIVO_REPROGRAMACIONES')
        print('file',archivo)
        fecha_m = datetime.datetime.fromtimestamp(os.path.getmtime(archivo)).strftime('%Y-%m-%d %H:%M:%S')
        print ('fecha_m',fecha_m)
        fila1 = {'RUTA':[archivo],'FECHA_MODIFICACION':[fecha_m]}
        fila1 = pd.DataFrame(fila1)
    except:
        fila1 = pd.DataFrame()

    try:
        archivo = requerir('PICKUP_ARCHIVO_COORDENADAS')
        print('file',archivo)
        fecha_m = datetime.datetime.fromtimestamp(os.path.getmtime(archivo)).strftime('%Y-%m-%d %H:%M:%S')
        print ('fecha_m',fecha_m)
        fila2 = {'RUTA':[archivo],'FECHA_MODIFICACION':[fecha_m]}
        fila2 = pd.DataFrame(fila2)
    except:
        fila2 = pd.DataFrame()

    try:
        archivo = encontrar_ruta(path_file = requerir('PICKUP_REPORTE_INGRESO_DIR'), startwith = 'Reporte_Ingreso', hours = 24)
        print('file',archivo)
        fecha_m = datetime.datetime.fromtimestamp(os.lstat(archivo).st_ctime).strftime('%Y-%m-%d %H:%M:%S')
        print ('fecha_m',fecha_m)
        fila3 = {'RUTA':[archivo],'FECHA_MODIFICACION':[fecha_m]}
        fila3 = pd.DataFrame(fila3)
    except:
        fila3 = pd.DataFrame()

    data_files = pd.concat([data_files,fila,fila1,fila2,fila3], ignore_index=True)

    print(data_files)

    return data_files


def ingesta_gcp():
    start_funcion = time.time()

    # GCP
    credentials = service_account.Credentials.from_service_account_file(
    requerir('GCP_CREDENTIALS_PATH')
    )

    finish_connection = time.time()

    try:

        query_gcp = open('querys/query_ingesta_gcp.txt', encoding= 'utf-8').read()
        df_ingesta = pd.read_gbq(query_gcp, credentials= credentials)
        df_ingesta['FECHA_MAXIMA_CARGA'] = pd.to_datetime(df_ingesta['FECHA_MAXIMA_CARGA']).dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        traceback.print_exc()
        df_ingesta = pd.DataFrame()

    print(df_ingesta)

    finish_procesamiento_query = time.time()

    finish_consultas_bd = time.time()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")


    return df_ingesta


def ingesta_gcp_bi():
    start_funcion = time.time()
    ingesta_gcp_E_I()
    # GCP
    credentials = service_account.Credentials.from_service_account_file(
    requerir('GCP_CREDENTIALS_PATH')
    )

    finish_connection = time.time()

    try:
        print("GCP")
        query_gcp = open(ruta_query('query_ingesta_gcp_bi.txt'), encoding= 'utf-8').read()
        print(query_gcp)
        df_ingesta = pd.read_gbq(query_gcp, credentials= credentials)
        df_ingesta['FECHA_MAXIMA_CARGA'] = pd.to_datetime(df_ingesta['FECHA_MAXIMA_CARGA']).dt.strftime('%Y-%m-%d %H:%M')

    except:
        traceback.print_exc()
        df_ingesta = pd.DataFrame()

   
    finish_procesamiento_query = time.time()

    finish_consultas_bd = time.time()

    print(
        f"tiempo conexiones ejecución -> {int(finish_connection - start_funcion)} seg")
    print(
        f"tiempo extraccion querys -> {int(finish_procesamiento_query - finish_connection)} seg")
    print(
        f"tiempo consultas BD -> {int(finish_consultas_bd - finish_procesamiento_query)} seg")


    return df_ingesta


def ingesta_gcp_E_I():
    # GCP
    credentials = service_account.Credentials.from_service_account_file(
    requerir('GCP_CREDENTIALS_PATH')
    )


    try:
        print("GCP")
        query_gcp = open(ruta_query('query_evidencia_excepcion.txt'), encoding= 'utf-8').read()
        print(query_gcp)
        df_ingesta = pd.read_gbq(query_gcp, credentials= credentials)
        df_ingesta['FECHA_MAXIMA_CARGA'] = pd.to_datetime(df_ingesta['FECHA_MAXIMA_CARGA']).dt.strftime('%Y-%m-%d %H:%M')

        mascara = (
            (df_ingesta['ESTADO'] == 'NOK') &
            df_ingesta['TABLA'].notna() &
            (df_ingesta['TABLA'].astype(str).str.strip() != '')
        )
        df_nok = df_ingesta.loc[mascara, ['TABLA']].copy()

        if df_ingesta.empty:
            tabla = 'E_I'
            mqtt_handler.enviar_mensaje(tabla, "ERROR AL MONITOREAR LAS EVIDENCIAS DE EXCEPCION", "error") 
        else:
            # Limpieza final de TABLA a string sin espacios
            df_nok['TABLA'] = df_nok['TABLA'].astype(str).str.strip()

            # ===== OPCIÓN A: Enviar línea por línea (todas las ocurrencias) =====
            for tabla in df_nok['TABLA']:
                mqtt_handler.enviar_mensaje(
                    tabla,
                    "TABLA SIN EVIDENCIAS DE EXCEPCION EN EL TIEMPO ESTIPULADO",
                    "error"
                )
    except:
        traceback.print_exc()
       
    return 


def ingesta_gcp_tms():
    start_funcion = time.time()
    
    # GCP
    credentials = service_account.Credentials.from_service_account_file(
    requerir('GCP_CREDENTIALS_PATH')
    )

    finish_connection = time.time()

    try:
        query_gcp = open('querys/query_ingesta_tms.txt', encoding= 'utf-8').read()
        df_ingesta = pd.read_gbq(query_gcp, credentials= credentials)
        df_ingesta['FECHA_CARGA'] = pd.to_datetime(df_ingesta['FECHA_CARGA']).dt.strftime('%Y-%m-%d %H:%M')
        df_ingesta['EJECUCION_BOT'] = pd.to_datetime(df_ingesta['EJECUCION_BOT']).dt.strftime('%Y-%m-%d %H:%M')

    except:

        df_ingesta = pd.DataFrame()

    return df_ingesta
    

