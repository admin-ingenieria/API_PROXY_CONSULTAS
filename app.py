from flask import Flask, json,request
from flask_cors import CORS, cross_origin

#  Panel Control Tower
from controllers import seguimiento
import time
from datetime import datetime
import logging
import logging.handlers

import traceback
import requests

from config import requerir, opcional

app = Flask(__name__)


CORS(app)

START_TIME = time.time()


logger = logging.getLogger('api putlight')
logger.setLevel(logging.DEBUG)
fh = logging.handlers.TimedRotatingFileHandler(
    'debug-api-putlight.log', when='W0', interval=1, backupCount=3
)
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

# cx_Oracle.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_19_11", config_dir=r"C:\Oracle\instantclient_19_11\network\admin\tnsnames.ora")

### ====  PANEL CONTROL TOWER

@cross_origin
@app.route("/health", methods=['GET'])
def health():
    return {
        "status": "UP",
        "timestamp": datetime.now().isoformat(),
        "uptime_seconds": round(time.time() - START_TIME, 2),
        "service": "API Imperio Local"
    }, 200

@cross_origin
@app.route("/consultor_ingesta", methods=['GET'])
def consultor_3():
    data = seguimiento.ingesta_gcp()
    res = data.to_json(orient='records')
    return res 

@cross_origin
@app.route("/consultor_ingesta_bi", methods=['GET'])
def consultor_4():
    data = seguimiento.ingesta_gcp_bi()
    res = data.to_json(orient='records')
    return res 

@cross_origin
@app.route("/consultor_cambio_estado", methods=['GET'])
def consultor():
    data = seguimiento.cambio_estado_tms()
    res = data.to_json(orient='records')
    return res 



@cross_origin
@app.route("/consultor_inserciones_tms", methods=['GET'])
def consultor_2():
    data = seguimiento.inserciones_tms()
    res = data.to_json(orient='records')
    return res 

@cross_origin
@app.route("/consultor_bot_pickup", methods=['GET'])
def consultor_5():
    data = seguimiento.bot_pickup()
    res = data.to_json(orient='records')
    return res 


@cross_origin
@app.route("/consultor_ingesta_tms", methods=['GET'])
def consultor_6():
    data = seguimiento.ingesta_gcp_tms()
    res = data.to_json(orient='records')
    return res 


@cross_origin
@app.route("/consultor_anulaciones", methods=['GET'])
def consultor_anulaciones():
    data = seguimiento.anulaciones()
    res = data.to_json(orient='records')
    return res 

@cross_origin
@app.route("/consultor_bot_optz_rutas", methods=['GET'])
def consultor_bot_optz_rutas():
    data = seguimiento.bot_optimizacion_rutas()
    res = data.to_json(orient='records')
    return res 

@cross_origin
@app.route("/consultor_bot_rutas_masivas_tms", methods=['GET'])
def consultor_bot_rutas_masivas_tms():
    data = seguimiento.bot_rutas_masivas_tms()
    res = data.to_json(orient='records')
    return res 


@cross_origin
@app.route("/ptl/ligthConfirm", methods=['POST'])
def ligthConfirm():
    res =json.loads(request.data)
    print(res)
    logger.debug(res)
    for i in range(0,len(res)):
        
        print(res[i]["id"])
        print(res[i]["text"])

    logger.debug('Request Terminado') 
    return res 


@cross_origin
@app.route('/proxySincronizacionEstadoTMS', methods=['GET', 'POST'])
def proxy():
    start_time = time.time()
    request_id = str(int(time.time() * 1000))  # ID único para la request
    
    data = request.get_json()
    logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] === INICIO REQUEST ===")
    logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Method: {request.method}")
    logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Request recibido: {data}")
    
    codigo_oficina = data['oficina']
    codigo_operador = data['codigoOperador']
    numero_envio = data['numeroEnvio']
    codigo_estado = data['codigoEstado']
    codigo_dane_poblacion = data['codigoDANEPoblacion']
    codigoexcepcion = ""

    if 'codigoexcepcion' in data:
        codigoexcepcion = data['codigoexcepcion']

    logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Datos procesados - Oficina: {codigo_oficina}, Operador: {codigo_operador}, Envio: {numero_envio}, Estado: {codigo_estado}")

    try:
        url = "https://lp.logimas.co/Server.Web/dominio/envios/ENThirdPartyLogistic/v2/sincronizacion-estado"

        headers = {
            "Authorization-User" : requerir('LOGIMAS_AUTH_USER'),
            "Authorization-Token" : requerir('LOGIMAS_AUTH_TOKEN')
            }

        body = {
            "codigoOperador": f"{codigo_operador}",
            "numeroEnvio": f"{numero_envio}",
            "codigoEstado": codigo_estado,
            "codigoOficina": f"{codigo_oficina}",
            "codigoDANEPoblacion": f"{codigo_dane_poblacion}",
            "codigoexcepcion": codigoexcepcion
        }
        
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] URL destino: {url}")
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Body enviado: {body}")
        
        # Realizar la petición
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Iniciando petición HTTP POST...")
        res = requests.post(url = url,json = body, headers=headers, verify=False)
        
        # Log de respuesta
        execution_time = round((time.time() - start_time) * 1000, 2)  # en milisegundos
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Status recibido: {res.status_code}")
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Respuesta recibida: {res.text}")
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] Tiempo de ejecución: {execution_time}ms")
        
        data = res.json()
        if(res.status_code==200):
            logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] === REQUEST EXITOSA ===")
            return {"Categoria":"Correcto","Mensaje":"Ok"},200
        else:
            logger.warning(f"[proxySincronizacionEstadoTMS][{request_id}] Error en respuesta: {data}")
            return data
    except Exception as e:
        execution_time = round((time.time() - start_time) * 1000, 2)
        logger.error(f"[proxySincronizacionEstadoTMS][{request_id}] Error inesperado después de {execution_time}ms: {str(e)}")
        logger.error(f"[proxySincronizacionEstadoTMS][{request_id}] Traceback: {traceback.format_exc()}")
        return data
    
    finally:
        total_time = round((time.time() - start_time) * 1000, 2)
        logger.info(f"[proxySincronizacionEstadoTMS][{request_id}] === FIN REQUEST - Tiempo total: {total_time}ms ===")



@cross_origin
@app.route('/proxyGuiaRemision', methods=['POST'])
def proxyGuiaRemision():
    data = request.get_json()
    data = json.dumps(data)
    print(data)
    try:
        url = "https://lp.logimas.co/Server.Web/dominio/procesos/INDocumentoOperacion/guias-remision-generar/"
        headers = {
            "Authorization-User" : requerir('LOGIMAS_GUIA_USER'),
            "Authorization" : "Bearer " + requerir('LOGIMAS_BEARER_TOKEN')
            ,"Content-Type": "application/json"
            }
        body = data
        res = requests.post(url = url,data = body, headers=headers, verify=False)
        if(res.status_code==204):
            return {"Categoria":"Correcto","Mensaje":"Ok"},200
        else:
            data = res.json()
            return data
    except:
        traceback.print_exc()
        return data



@cross_origin
@app.route('/proxyCambioEstadoCorp', methods=['POST'])
def proxyCambioEstadoCorp():
    data = request.get_json()
    data = json.dumps(data)
    print(data)
    try:
        url = "https://us-east4-corp-logistics-prod.cloudfunctions.net/carrier-webhook-handler/ibis"
        headers = {
            "Authorization" : "Bearer " + requerir('CORP_WEBHOOK_BEARER')
            ,"Content-Type": "application/json"
            }
        body = data
        res = requests.post(url = url, data = body, headers=headers, verify=False)
        if(res.status_code==200):
            return {"Categoria":"Correcto","Mensaje":"Ok"},200
        else:
            data = res.json()
            return data
    except:
        traceback.print_exc()
        return data


@cross_origin
@app.route('/proxycerrarPlanillaTMS', methods=['GET', 'POST'])
def cerrarPlanillaTMS():
    
    data = request.get_json()
    numero_envio = data['numeroPlanilla']
    idOficina = data['idOficina']
    idUsuario = data['idUsuario']

    try:
        url = "https://lp.logimas.co/BCRepartos/core/repartos/COReparto/cerrar-planilla-reparto/"

        headers = {
            "Authorization-User" : requerir('LOGIMAS_AUTH_USER'),
            "Authorization" : "Bearer " + requerir('LOGIMAS_BEARER_TOKEN')
            }

        body = {
            "idOficina" : idOficina ,
            "idUsuario" : idUsuario,
        "nombreUsuario": "Api Zeus",  
            "numeroPlanilla" : numero_envio
        }
        res = requests.post(url = url,json = body, headers=headers, verify=False)
        data = res.json()
        if(res.status_code==200):
            return {"Categoria":"Correcto","Mensaje":"Ok"},200
        else:
            return data
    except:
        
        traceback.print_exc()
        return data



@cross_origin
@app.route('/proxyAnularEnvio', methods=['POST'])
def proxyAnularEnvio():
    
    data = request.get_json()
    numero_envio = data['numero']
    idOficina = data['idOficina']
    login = data['login']
    print(data)

    try:
        url = "https://lp.logimas.co/Server.Web/dominio/envios/EnEnvios/cancelacion-envio"

        headers = {
            "Authorization-User" : requerir('LOGIMAS_AUTH_USER'),
            "Authorization" : "Bearer " + requerir('LOGIMAS_BEARER_ANULACION')            }

        body = {
            "idOficina" : idOficina ,
            "login" : login,
            "numero" : numero_envio
        }
        res = requests.post(url = url,json = body, headers=headers, verify=False)
        data = res.json()
        if(res.status_code==200):
            return {"Categoria":"Correcto","Mensaje":"Ok"},200
        else:
            return data
    except:
        
        traceback.print_exc()
        return data
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(opcional('API_PORT', '8010')), debug=False) 