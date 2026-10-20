# API Proxy Consultas

API en Flask que sirve como intermediario (proxy) entre sistemas internos y sus fuentes de datos. Por un lado consulta bases de datos y Google BigQuery y devuelve los resultados en JSON. Por otro lado reenvía operaciones (sincronizar estados, anular envíos, cerrar planillas) a los servicios externos de Logimas y Corp Logistics.

## Tabla de contenido

- [Funcionalidades](#funcionalidades)
- [Endpoints](#endpoints)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Ejecución](#ejecución)
- [Estructura del proyecto](#estructura-del-proyecto)

## Funcionalidades

- **Consultas de seguimiento** sobre ZEUS (MySQL), BTL (Oracle), Ingeniería (SQL Server) y BigQuery.
- **Proxy de operaciones** hacia los servicios de Logimas y Corp Logistics, con las credenciales en el servidor (el cliente no las conoce).
- **Monitoreo de archivos** del bot de pickup (últimos archivos generados en carpetas configuradas).
- **Alertas MQTT** para las tablas con evidencias de excepción pendientes.
- **Health check** en `/health`.

Las consultas SQL están en la carpeta `querys/` y se leen en cada petición, así que se pueden cambiar sin tocar el código Python.

## Endpoints

### Consultas (GET)

| Endpoint | Función | Fuente |
|---|---|---|
| `/health` | Estado del servicio y tiempo activo | — |
| `/consultor_ingesta` | Ingesta de datos | BigQuery |
| `/consultor_ingesta_bi` | Ingesta BI | BigQuery |
| `/consultor_ingesta_tms` | Ingesta TMS | BigQuery |
| `/consultor_cambio_estado` | Cambios de estado TMS | MySQL (ZEUS) |
| `/consultor_inserciones_tms` | Inserciones TMS | SQL Server |
| `/consultor_anulaciones` | Anulaciones | MySQL (ZEUS) |
| `/consultor_bot_optz_rutas` | Bot de optimización de rutas | MySQL (ZEUS) |
| `/consultor_bot_rutas_masivas_tms` | Bot de rutas masivas | MySQL (ZEUS) y Oracle (BTL) |
| `/consultor_bot_pickup` | Archivos del bot de pickup | Sistema de archivos |

Las consultas devuelven una lista de registros en JSON (`orient='records'`).

### Operaciones (proxy)

| Endpoint | Método | Destino |
|---|---|---|
| `/proxySincronizacionEstadoTMS` | GET, POST | Logimas: sincronización de estado |
| `/proxyGuiaRemision` | POST | Logimas: generar guías de remisión |
| `/proxycerrarPlanillaTMS` | GET, POST | Logimas: cerrar planilla de reparto |
| `/proxyAnularEnvio` | POST | Logimas: cancelación de envío |
| `/proxyCambioEstadoCorp` | POST | Corp Logistics: webhook de estados |
| `/ptl/ligthConfirm` | POST | Recibe confirmaciones (id y texto) y las registra en el log |

## Requisitos

- Python 3.9 (versión del venv de referencia del proyecto). Las pruebas de este cambio se hicieron con Python 3.10.
- Cliente Oracle Instant Client, requerido por `cx_Oracle`.
- Driver ODBC para SQL Server ("ODBC Driver 17 for SQL Server" o superior), requerido por `pyodbc`.
- Acceso de red a las bases de datos, al broker MQTT y a los servicios de Logimas y Corp Logistics.

## Instalación

```bash
python -m venv env
env\Scripts\activate
pip install -r requeriments.txt
```

## Configuración

Las credenciales y rutas no van en el código. Se leen de variables de entorno, y opcionalmente de un archivo `.env` en la raíz del proyecto.

1. Copie la plantilla:

   ```bash
   copy .env.example .env
   ```

2. Complete los valores en `.env`.

Las variables que no estén en `.env` pueden definirse en el sistema operativo. Si una variable obligatoria falta, la petición falla con un mensaje que indica cuál es.

| Variable | Obligatoria | Descripción |
|---|---|---|
| `API_PORT` | No (por defecto `8010`) | Puerto de la API |
| `MYSQL_HOST`, `MYSQL_PORT`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` | Sí | Conexión a ZEUS |
| `ORACLE_BTL_HOST`, `ORACLE_BTL_PORT`, `ORACLE_BTL_SERVICE`, `ORACLE_BTL_USER`, `ORACLE_BTL_PASSWORD` | Sí | Conexión a BTL |
| `SQLSERVER_HOST`, `SQLSERVER_DATABASE`, `SQLSERVER_USER`, `SQLSERVER_PASSWORD` | Sí | Conexión a Ingeniería |
| `GCP_CREDENTIALS_PATH` | Sí | Ruta al JSON de la cuenta de servicio de GCP |
| `LOGIMAS_AUTH_USER`, `LOGIMAS_AUTH_TOKEN`, `LOGIMAS_GUIA_USER`, `LOGIMAS_BEARER_TOKEN`, `LOGIMAS_BEARER_ANULACION` | Sí | Credenciales de Logimas |
| `CORP_WEBHOOK_BEARER` | Sí | Token del webhook de Corp Logistics |
| `MQTT_BROKER_HOST`, `MQTT_BROKER_PORT` | No | Broker MQTT para alertas |
| `PICKUP_DATA_DIR`, `PICKUP_REPORTE_INGRESO_DIR`, `PICKUP_ARCHIVO_REPROGRAMACIONES`, `PICKUP_ARCHIVO_COORDENADAS` | Sí (para `/consultor_bot_pickup`) | Rutas de archivos que monitorea el bot de pickup |

> El archivo `.env` contiene credenciales. No debe subirse a git (ya está en `.gitignore`).

## Ejecución

```bash
env\Scripts\activate
python app.py
```

O con el archivo `bat_api_CONTROL_TOWER.bat`, que activa el entorno y levanta la API.

La API queda escuchando en `http://0.0.0.0:<API_PORT>`. Por ejemplo:

```bash
curl http://localhost:8010/health
```

Los registros de la aplicación se guardan en `debug-api-putlight.log`, con rotación semanal.

## Estructura del proyecto

```
.
├── app.py                      # Rutas Flask (endpoints y proxies)
├── config.py                   # Carga de variables de entorno y .env
├── controllers/
│   ├── seguimiento.py          # Consultas a MySQL, Oracle, SQL Server y BigQuery
│   ├── subcipt_mqtt.py         # Cliente MQTT para alertas
│   └── alertas.py              # Archivo sin uso actual (solo importa el cliente MQTT)
├── querys/                     # Consultas SQL que usa la API
├── requeriments.txt            # Dependencias de Python
├── bat_api_CONTROL_TOWER.bat   # Arranque en Windows
├── .env.example                # Plantilla de variables de entorno
└── .gitignore
```

## Notas

- Las llamadas a Logimas y Corp Logistics se hacen con `verify=False` (sin validar el certificado). Es el comportamiento actual; conviene revisarlo con el equipo de seguridad.
- `cx_Oracle` requiere el Instant Client instalado y accesible en el `PATH`.
