import json
import logging
import queue
import sqlite3
import threading
import time
from datetime import datetime

import requests

from config import requerir, opcional

logger = logging.getLogger('api putlight')

URL_LOGIMAS = "https://lp.logimas.co/Server.Web/dominio/envios/ENThirdPartyLogistic/v2/sincronizacion-estado"
CAMPOS_REQUERIDOS = ('oficina', 'codigoOperador', 'numeroEnvio', 'codigoEstado', 'codigoDANEPoblacion')

WORKERS = int(opcional('SYNC_WORKERS', '8'))
COLA_MAX = int(opcional('SYNC_QUEUE_MAX', '5000'))
REINTENTOS = int(opcional('SYNC_MAX_RETRIES', '3'))
TIMEOUT = float(opcional('SYNC_TIMEOUT', '15'))
DB_FILE = opcional('SYNC_DB_FILE', 'sincronizacion.db')

# Estados del ciclo de vida de cada request en la tabla de control.
PENDIENTE, PROCESANDO, OK, FALLIDO, RECHAZADO = 'PENDIENTE', 'PROCESANDO', 'OK', 'FALLIDO', 'RECHAZADO'

_cola = queue.Queue(maxsize=COLA_MAX)
_lock_inicio = threading.Lock()
_iniciado = False
_local = threading.local()


# ---------- Tabla de control ----------

def _ahora():
    return datetime.now().isoformat(timespec='milliseconds')


def _conn():
    con = sqlite3.connect(DB_FILE, timeout=10)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    con = _conn()
    try:
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("""
            CREATE TABLE IF NOT EXISTS sincronizacion_estado (
                request_id    TEXT PRIMARY KEY,
                numero_envio  TEXT,
                codigo_estado TEXT,
                body          TEXT NOT NULL,
                estado        TEXT NOT NULL,
                intentos      INTEGER NOT NULL DEFAULT 0,
                http_status   INTEGER,
                respuesta     TEXT,
                duracion_ms   REAL,
                creado_en     TEXT NOT NULL,
                actualizado_en TEXT NOT NULL,
                finalizado_en TEXT
            )""")
        con.execute("CREATE INDEX IF NOT EXISTS ix_sync_estado ON sincronizacion_estado(estado, creado_en)")
        con.execute("CREATE INDEX IF NOT EXISTS ix_sync_envio ON sincronizacion_estado(numero_envio)")
        con.commit()
    finally:
        con.close()


def _ejecutar(sql, params=()):
    con = _conn()
    try:
        con.execute(sql, params)
        con.commit()
    finally:
        con.close()


def _consultar(sql, params=()):
    con = _conn()
    try:
        return [dict(r) for r in con.execute(sql, params).fetchall()]
    finally:
        con.close()


def _registrar(request_id, body):
    ahora = _ahora()
    _ejecutar(
        "INSERT INTO sincronizacion_estado (request_id, numero_envio, codigo_estado, body, estado, creado_en, actualizado_en) "
        "VALUES (?,?,?,?,?,?,?)",
        (request_id, body['numeroEnvio'], str(body['codigoEstado']), json.dumps(body, ensure_ascii=False), PENDIENTE, ahora, ahora),
    )


def _marcar(request_id, estado, **campos):
    campos['estado'] = estado
    campos['actualizado_en'] = _ahora()
    if estado in (OK, FALLIDO, RECHAZADO):
        campos['finalizado_en'] = campos['actualizado_en']
    sets = ", ".join(f"{k}=?" for k in campos)
    _ejecutar(f"UPDATE sincronizacion_estado SET {sets} WHERE request_id=?", (*campos.values(), request_id))


# ---------- Validación y armado del body ----------

def validar(data):
    """Devuelve la lista de campos requeridos que faltan en el payload."""
    if not isinstance(data, dict):
        return list(CAMPOS_REQUERIDOS)
    return [c for c in CAMPOS_REQUERIDOS if c not in data]


def construir_body(data):
    return {
        "codigoOperador": f"{data['codigoOperador']}",
        "numeroEnvio": f"{data['numeroEnvio']}",
        "codigoEstado": data['codigoEstado'],
        "codigoOficina": f"{data['oficina']}",
        "codigoDANEPoblacion": f"{data['codigoDANEPoblacion']}",
        "codigoexcepcion": data.get('codigoexcepcion', ""),
    }


# ---------- Procesamiento ----------

def _sesion():
    # Una sesión por hilo: reutiliza conexiones TLS hacia Logimas sin compartir estado entre hilos.
    if not hasattr(_local, 'sesion'):
        _local.sesion = requests.Session()
    return _local.sesion


def _procesar(request_id, body):
    tag = f"[proxySincronizacionEstadoTMSAsync][{request_id}]"
    headers = {
        "Authorization-User": requerir('LOGIMAS_AUTH_USER'),
        "Authorization-Token": requerir('LOGIMAS_AUTH_TOKEN'),
    }
    http_status, respuesta, duracion = None, "", None
    _marcar(request_id, PROCESANDO)
    for intento in range(1, REINTENTOS + 1):
        inicio = time.time()
        try:
            res = _sesion().post(URL_LOGIMAS, json=body, headers=headers, timeout=TIMEOUT, verify=False)
            duracion = round((time.time() - inicio) * 1000, 2)
            http_status, respuesta = res.status_code, res.text[:1000]
            if res.status_code == 200:
                logger.info(f"{tag} OK intento {intento} ({duracion}ms) envio={body['numeroEnvio']}")
                _marcar(request_id, OK, intentos=intento, http_status=http_status, respuesta=respuesta, duracion_ms=duracion)
                return
            logger.warning(f"{tag} intento {intento} ({duracion}ms) HTTP {http_status}: {respuesta[:300]}")
            # Errores 4xx (salvo 408/429) no se arreglan reintentando.
            if 400 <= res.status_code < 500 and res.status_code not in (408, 429):
                _marcar(request_id, PROCESANDO, intentos=intento)
                break
        except Exception as e:
            http_status, respuesta = None, f"{type(e).__name__}: {e}"[:1000]
            logger.warning(f"{tag} intento {intento} excepción {respuesta}")
        _marcar(request_id, PROCESANDO, intentos=intento, http_status=http_status, respuesta=respuesta)
        if intento < REINTENTOS:
            time.sleep(2 ** intento)  # 2s, 4s, ...
    logger.error(f"{tag} FALLIDO envio={body['numeroEnvio']} http={http_status} respuesta={respuesta[:300]}")
    _marcar(request_id, FALLIDO, http_status=http_status, respuesta=respuesta, duracion_ms=duracion)


def _worker():
    while True:
        request_id, body = _cola.get()
        try:
            _procesar(request_id, body)
        except Exception as e:
            logger.exception(f"[proxySincronizacionEstadoTMSAsync][{request_id}] error no controlado en worker")
            try:
                _marcar(request_id, FALLIDO, respuesta=f"{type(e).__name__}: {e}"[:1000])
            except Exception:
                pass
        finally:
            _cola.task_done()


def _recuperar_pendientes():
    # Tras un reinicio, vuelve a encolar lo que quedó sin terminar.
    filas = _consultar("SELECT request_id, body FROM sincronizacion_estado WHERE estado IN (?,?) ORDER BY creado_en", (PENDIENTE, PROCESANDO))
    recuperados = 0
    for f in filas:
        try:
            _cola.put_nowait((f['request_id'], json.loads(f['body'])))
            recuperados += 1
        except queue.Full:
            break
    if filas:
        logger.info(f"[proxySincronizacionEstadoTMSAsync] Recuperados {recuperados}/{len(filas)} pendientes tras reinicio")


def _iniciar():
    global _iniciado
    if _iniciado:
        return
    with _lock_inicio:
        if _iniciado:
            return
        init_db()
        _recuperar_pendientes()
        for i in range(WORKERS):
            threading.Thread(target=_worker, name=f"sync-worker-{i}", daemon=True).start()
        _iniciado = True


def encolar(request_id, body):
    """Registra el envío en la tabla y lo encola. Devuelve False si la cola está llena."""
    _iniciar()
    _registrar(request_id, body)
    try:
        _cola.put_nowait((request_id, body))
        return True
    except queue.Full:
        _marcar(request_id, RECHAZADO, respuesta="Cola llena")
        return False


# ---------- Monitoreo ----------

def pendientes():
    return _cola.qsize()


def obtener(request_id):
    _iniciar()
    filas = _consultar("SELECT * FROM sincronizacion_estado WHERE request_id=?", (request_id,))
    return filas[0] if filas else None


def resumen(limite_fallidos=20):
    _iniciar()
    por_estado = {r['estado']: r['n'] for r in _consultar("SELECT estado, COUNT(*) n FROM sincronizacion_estado GROUP BY estado")}
    hoy = datetime.now().strftime('%Y-%m-%d')
    hoy_estado = {r['estado']: r['n'] for r in _consultar(
        "SELECT estado, COUNT(*) n FROM sincronizacion_estado WHERE creado_en >= ? GROUP BY estado", (hoy,))}
    mas_viejo = _consultar("SELECT MIN(creado_en) c FROM sincronizacion_estado WHERE estado IN (?,?)", (PENDIENTE, PROCESANDO))[0]['c']
    desde = datetime.fromtimestamp(time.time() - 3600).isoformat(timespec='milliseconds')
    ultima_hora = _consultar(
        "SELECT COUNT(*) n, AVG(duracion_ms) prom, MAX(duracion_ms) max FROM sincronizacion_estado WHERE estado=? AND finalizado_en >= ?",
        (OK, desde))[0]
    fallidos = _consultar(
        "SELECT request_id, numero_envio, codigo_estado, intentos, http_status, respuesta, creado_en, finalizado_en "
        "FROM sincronizacion_estado WHERE estado=? ORDER BY finalizado_en DESC LIMIT ?", (FALLIDO, limite_fallidos))
    return {
        "cola_en_memoria": pendientes(),
        "cola_capacidad": COLA_MAX,
        "workers": WORKERS,
        "por_estado_total": por_estado,
        "por_estado_hoy": hoy_estado,
        "pendiente_mas_antiguo": mas_viejo,
        "ultima_hora_ok": {
            "cantidad": ultima_hora['n'],
            "duracion_prom_ms": round(ultima_hora['prom'], 2) if ultima_hora['prom'] is not None else None,
            "duracion_max_ms": ultima_hora['max'],
        },
        "ultimos_fallidos": fallidos,
    }


def reprocesar_fallidos(numero_envio=None):
    """Vuelve a poner en cola los FALLIDO (opcionalmente solo los de un envío). Devuelve cuántos encoló."""
    _iniciar()
    sql, params = "SELECT request_id, body FROM sincronizacion_estado WHERE estado=?", [FALLIDO]
    if numero_envio:
        sql += " AND numero_envio=?"
        params.append(str(numero_envio))
    encolados = 0
    for f in _consultar(sql + " ORDER BY creado_en", params):
        try:
            _cola.put_nowait((f['request_id'], json.loads(f['body'])))
        except queue.Full:
            break
        _marcar(f['request_id'], PENDIENTE, intentos=0, finalizado_en=None)
        encolados += 1
    return encolados
