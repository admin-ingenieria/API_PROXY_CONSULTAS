import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def cargar_env(ruta):
    # Lee pares CLAVE=valor. Las variables que ya existen en el sistema tienen prioridad.
    if not os.path.isfile(ruta):
        return
    with open(ruta, encoding='utf-8') as archivo:
        for linea in archivo:
            linea = linea.strip()
            if not linea or linea.startswith('#') or '=' not in linea:
                continue
            clave, valor = linea.split('=', 1)
            os.environ.setdefault(clave.strip(), valor.strip())


cargar_env(os.path.join(BASE_DIR, '.env'))


def requerir(nombre):
    valor = os.environ.get(nombre)
    if not valor:
        raise RuntimeError(f"Falta la variable de entorno {nombre}")
    return valor


def opcional(nombre, valor_por_defecto=None):
    return os.environ.get(nombre, valor_por_defecto)
