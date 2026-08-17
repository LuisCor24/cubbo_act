import json
import logging
import os
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
import pandas as pd
import pymysql
import requests
from pandas import json_normalize
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv
load_dotenv()


# =============================================================================
# 1. CONFIGURACION
# =============================================================================

DB_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "charset": "utf8mb4",
}
# Aiven y otras bases en la nube exigen SSL. Se activa con MYSQL_SSL=1.
if os.getenv("MYSQL_SSL", "0") == "1":
    DB_CONFIG["ssl"] = {"ssl": {}}
DB_NAME = os.getenv("MYSQL_DATABASE", "cubbo_db")

CUBBO_BASE_URL = os.getenv("CUBBO_BASE_URL", "https://lastmile-mvp.emergent.host/api")
CUBBO_API_KEY = os.getenv("CUBBO_API_KEY", "")

ROUTAL_BASE_URL = os.getenv("ROUTAL_BASE_URL", "https://api.routal.com/v2")
ROUTAL_PRIVATE_KEY = os.getenv("ROUTAL_PRIVATE_KEY", "")
ROUTAL_PROJECT_ID = os.getenv("ROUTAL_PROJECT_ID", "636aaed54235630d2258fa6d")


DIAS_RECIENTES = int(os.getenv("DIAS_RECIENTES", "10"))
MAX_DIAS_RANGO = int(os.getenv("MAX_DIAS_RANGO", "30"))


def calcular_rango():
    """Devuelve (date_from, date_to). Si no hay fechas en el entorno, usa los
    ultimos DIAS_RECIENTES. Si las hay, valida que no excedan MAX_DIAS_RANGO."""
    hoy = date.today()
    df_env = os.getenv("DATE_FROM")
    dt_env = os.getenv("DATE_TO")

    if not df_env and not dt_env:
        inicio = hoy - timedelta(days=DIAS_RECIENTES - 1)
        return inicio.isoformat(), hoy.isoformat()

    date_to = dt_env or hoy.isoformat()
    date_from = df_env or date_to

    d0 = pd.to_datetime(date_from).date()
    d1 = pd.to_datetime(date_to).date()
    if (d1 - d0).days + 1 > MAX_DIAS_RANGO:
        raise SystemExit(
            f"Rango de {(d1 - d0).days + 1} dias excede el maximo de "
            f"{MAX_DIAS_RANGO}. Para recargas historicas grandes, sube "
            f"MAX_DIAS_RANGO o corre por tramos."
        )
    return date_from, date_to


_date_from, _date_to = calcular_rango()
params = {"date_from": _date_from, "date_to": _date_to}

headers = {"Authorization": f"Bearer {CUBBO_API_KEY}"}

# --- Banderas de comportamiento ---------------------------------------------

NORMALIZAR_ACENTOS = True
CONSERVAR_ENIE = True

# Convierte las fechas de UTC a hora local antes de guardar.
ZONA_HORARIA = "America/Mexico_City"

# Guarda las fechas con hora (DATETIME), salvo las de COLUMNAS_FECHA_PURA.
CONSERVAR_HORA = True

# Fechas de "dia de operacion": quedan como DATE, sin hora ni conversion de zona.
COLUMNAS_FECHA_PURA = {
    "incidents.journey_date",
    "journeys.date",
    "packages.journey_date",
    "plans.execution_date",
    "rutas.start_date",
    "rutas.end_date",
}

MIGRAR_TIPOS_FECHA = False

TAMANO_LOTE = 1000

# La API topa en 5000 por respuesta y no pagina; 1 dia por ventana evita el tope.
DIAS_POR_LOTE = 1
PLANS_PARADA_ANTICIPADA = True

# Descargas simultaneas de rutas (Routal). 4 es estable; 6 causaba cortes.
MAX_WORKERS_RUTAS = 4

# Descargas simultaneas de ventanas de dias en reportes de Cubbo. Cubbo es
# lento; 2 da estabilidad. Si aun aparecen timeouts, bajar a 1.
MAX_WORKERS_REPORTES = 2

# --- Sincronizacion con borrado (hard delete) -------------------------------

# Borra de la base los registros que la API ya no devuelve. False = nunca borra.
SINCRONIZAR_BORRADO = True

# Columna de fecha que acota el borrado por tabla. 'rutas' se excluye a
# proposito (se extrae por plan, no por su fecha).
COLUMNA_RANGO_BORRADO = {
    "incidents": "journey_date",
    "journeys":  "date",
    "packages":  "journey_date",
    "plans":     "execution_date",
}

# Freno: si la API trae menos de esta fraccion de lo que hay en base, no borra
# (asume fallo de la API, no borrado real).
UMBRAL_MINIMO_BORRADO = 0.5

# --- Configuracion de tablas ------------------------------------------------
TABLES_CONFIG = {
    "incidents": {"endpoint": "/reports/incidents", "id_col": "incident_id"},
    "journeys":  {"endpoint": "/reports/journeys",  "id_col": "journey_id"},
    "packages":  {"endpoint": "/reports/packages",  "id_col": "package_id"},
    "plans":     {"endpoint": "/plans", "id_col": "id", "source": "routal"},
    "rutas":     {"endpoint": None, "id_col": "id", "source": "routal_rutas"},
}

DATE_COLUMNS = {
    "incidents": ["journey_date", "occurred_at", "resolved_at", "created_at"],
    "journeys":  ["date", "departure_time", "closed_at", "created_at"],
    "packages":  ["journey_date", "reviewed_at"],
    "plans":     ["execution_date", "created_at", "updated_at"],
    "rutas":     ["start_date", "end_date", "estimated_end_time",
                  "created_at", "updated_at"],
}

# Traduccion de codigos a etiquetas legibles. Coincidencia EXACTA.
MAPEOS_ETIQUETAS = {
    "incidents": {
        "incident_type": {
            "autorizacion_tercero_incorrecta": "Autorización con tercero incorrecta",
            "evidencia_entrega_incorrecta": "Evidencia de entrega incorrecta",
            "evidencia_incidencia_incorrecta": "Evidencia de incidencia incorrecta",
            "notas_incorrectas": "Notas incorrectas",
            "otro": "Otro",
        },
    },
}


# =============================================================================
# 2. LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("etl_cubo.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("etl_cubo")


# =============================================================================
# 3. LIMPIEZA (funciones puras)
# =============================================================================

# Caracteres invisibles: zero-width, marcas de direccion, BOM, soft hyphen.
RE_INVISIBLES = re.compile(r"[\u200b-\u200f\u202a-\u202e\u2060\ufeff\u00ad]")
# Caracteres de control: se reemplazan por espacio.
RE_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
# Cualquier secuencia de espacios en blanco -> un solo espacio.
RE_ESPACIOS = re.compile(r"\s+")
# Firma tipica de mojibake: UTF-8 leido como latin-1.
RE_MOJIBAKE = re.compile(r"[\u00c3\u00c2][\u0080-\u00bf]")

# Fechas ISO: 2026-07-22 / 2026-07-22T16:08:35.496Z / 2026-07-22 16:08:35
RE_FECHA_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2})?)?")

# Marcadores temporales para proteger la enie durante la descomposicion NFD.
_MARCA_ENIE_MIN = "\ue000"
_MARCA_ENIE_MAY = "\ue001"


def reparar_mojibake(texto):
    """Corrige texto UTF-8 que fue decodificado como latin-1."""
    if RE_MOJIBAKE.search(texto):
        try:
            return texto.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            return texto
    return texto


def quitar_acentos(texto, conservar_enie=True):
    """Elimina diacriticos. Opcionalmente preserva la enie."""
    if conservar_enie:
        texto = texto.replace("\u00f1", _MARCA_ENIE_MIN)
        texto = texto.replace("\u00d1", _MARCA_ENIE_MAY)
    descompuesto = unicodedata.normalize("NFD", texto)
    sin_marcas = "".join(c for c in descompuesto if unicodedata.category(c) != "Mn")
    resultado = unicodedata.normalize("NFC", sin_marcas)
    if conservar_enie:
        resultado = resultado.replace(_MARCA_ENIE_MIN, "\u00f1")
        resultado = resultado.replace(_MARCA_ENIE_MAY, "\u00d1")
    return resultado


def limpiar_texto(texto):
    """Normaliza una cadena: mojibake, invisibles, espacios, acentos. Vacio -> None."""
    texto = reparar_mojibake(texto)
    texto = unicodedata.normalize("NFC", texto)
    texto = RE_INVISIBLES.sub("", texto)
    texto = RE_CONTROL.sub(" ", texto)
    texto = RE_ESPACIOS.sub(" ", texto).strip()
    if not texto:
        return None
    if NORMALIZAR_ACENTOS:
        texto = quitar_acentos(texto, CONSERVAR_ENIE)
    return texto


def limpiar_valor(valor):
    """Limpia texto de forma recursiva (tambien dentro de dicts y listas)."""
    if isinstance(valor, str):
        return limpiar_texto(valor)
    if isinstance(valor, dict):
        return {k: limpiar_valor(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [limpiar_valor(v) for v in valor]
    return valor


def detectar_columnas_fecha(df, pistas=(), excluir=()):
    """Detecta columnas de fecha por patron ISO (>=90% de la muestra)."""
    columnas = {c for c in pistas if c in df.columns and c not in excluir}

    for col in df.columns:
        if col in columnas or col in excluir:
            continue
        serie = df[col].dropna()
        if serie.empty:
            continue
        muestra = serie.head(50).tolist()
        textos = [v for v in muestra if isinstance(v, str)]
        if not textos or len(textos) < len(muestra) * 0.9:
            continue
        aciertos = sum(1 for v in textos if RE_FECHA_ISO.match(v))
        if aciertos >= len(textos) * 0.9:
            columnas.add(col)
    return columnas


def normalizar_fechas(df, columnas, zona=None, conservar_hora=False, columnas_fecha_pura=()):
    """Convierte fechas: las puras quedan DATE sin zona; el resto DATETIME en hora local."""
    columnas_fecha_pura = set(columnas_fecha_pura)
    for col in columnas:
        if col not in df.columns:
            continue
        try:
            serie = pd.to_datetime(df[col], errors="coerce", utc=True, format="mixed")
        except (TypeError, ValueError):
            serie = pd.to_datetime(df[col], errors="coerce", utc=True)

        es_pura = col in columnas_fecha_pura

        # La zona solo aplica a timestamps, no a fechas puras.
        if zona and not es_pura:
            serie = serie.dt.tz_convert(zona)

        if conservar_hora and not es_pura:
            serie = serie.dt.tz_localize(None)
            valores = [None if pd.isna(v) else v.to_pydatetime() for v in serie]
        else:
            valores = [None if pd.isna(v) else v for v in serie.dt.date]

        df[col] = pd.Series(valores, index=df.index, dtype=object)
    return df


def serializar_anidados(valor):
    """dict/list -> texto JSON (MySQL no almacena objetos de Python)."""
    if isinstance(valor, (dict, list)):
        return json.dumps(valor, ensure_ascii=False, default=str)
    return valor

def aplicar_etiquetas(df, tabla):
    """Traduce codigos a etiquetas legibles usando coincidencia exacta.
    Los valores que no esten en el diccionario se conservan sin cambio."""
    mapeos = MAPEOS_ETIQUETAS.get(tabla, {})
    if not mapeos:
        return df
    df = df.copy()
    for columna, dicc in mapeos.items():
        if columna in df.columns:
            df[columna] = df[columna].map(lambda v: dicc.get(v, v))
    return df


def transformar_df(df, table_name, id_col):
    """Limpia texto, normaliza fechas y serializa anidados. Devuelve (df, cols_fecha)."""
    if df.empty:
        return df, set()

    df = df.copy()

    # Limpieza valor por valor (no por dtype: en pandas 3.x el texto es 'str').
    for col in df.columns:
        df[col] = pd.Series(
            [limpiar_valor(v) for v in df[col].tolist()],
            index=df.index,
            dtype=object,
        )

    # La llave primaria no puede quedar nula ni duplicada.
    if id_col in df.columns:
        nulos = int(df[id_col].isna().sum())
        if nulos:
            log.warning("  %s filas descartadas por %s nulo", nulos, id_col)
            df = df[df[id_col].notna()].copy()
        duplicados = int(df[id_col].duplicated().sum())
        if duplicados:
            log.warning("  %s %s duplicados en el origen; se conserva el ultimo",
                        duplicados, id_col)
            df = df.drop_duplicates(subset=[id_col], keep="last").copy()

    cols_fecha = detectar_columnas_fecha(
        df, pistas=DATE_COLUMNS.get(table_name, ()), excluir={id_col}
    )
    if cols_fecha:
        log.info("  Columnas de fecha: %s", ", ".join(sorted(cols_fecha)))
        cols_puras = {
            c for c in cols_fecha
            if f"{table_name}.{c}" in COLUMNAS_FECHA_PURA
        }
        df = normalizar_fechas(df, cols_fecha, zona=ZONA_HORARIA,
                               conservar_hora=CONSERVAR_HORA,
                               columnas_fecha_pura=cols_puras)

    for col in df.columns:
        if any(isinstance(v, (dict, list)) for v in df[col].tolist()):
            df[col] = df[col].map(serializar_anidados)

    return df, cols_fecha


# =============================================================================
# 4. EXTRACCION
# =============================================================================

def crear_sesion():
    """Sesion HTTP con reintentos automaticos ante errores transitorios."""
    sesion = requests.Session()
    reintentos = Retry(
        total=4,
        backoff_factor=1.5,               # espera 0s, 1.5s, 3s, 6s...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adaptador = HTTPAdapter(max_retries=reintentos,
                            pool_connections=20, pool_maxsize=20)
    sesion.mount("https://", adaptador)
    sesion.mount("http://", adaptador)
    return sesion


SESION = crear_sesion()
TIMEOUT = (10, 120)   # (conexion, lectura). Cubbo tarda >60s en dias pesados.


def fetch_report(endpoint, params, headers, limite_api=5000):
    """Trae un reporte de Cubbo. La API topa en 5000 y no pagina; ver DIAS_POR_LOTE."""
    p = {k: v for k, v in params.items() if v}
    p["limit"] = limite_api
    respuesta = SESION.get(
        f"{CUBBO_BASE_URL}{endpoint}",
        headers=headers,
        params=p,
        timeout=TIMEOUT,
    )
    respuesta.raise_for_status()
    datos = respuesta.json().get("data", [])

    # Si topa el limite, hay datos sin traer: avisa en vez de perderlos.
    if len(datos) >= limite_api:
        log.warning(
            "  %s (%s..%s) TOPO el limite de %s registros -> FALTAN DATOS en "
            "este tramo. Reducir el tamano de ventana (partir por horas).",
            endpoint, params.get("date_from"), params.get("date_to"), limite_api,
        )

    return json_normalize(datos) if datos else pd.DataFrame()


def iterar_ventanas_fecha(fecha_inicio, fecha_fin, dias):
    """Parte un rango de fechas en ventanas de N dias (inclusivas)."""
    inicio = pd.to_datetime(fecha_inicio).date()
    fin = pd.to_datetime(fecha_fin).date()
    actual = inicio
    while actual <= fin:
        ultimo = min(actual + timedelta(days=dias - 1), fin)
        yield actual.isoformat(), ultimo.isoformat()
        actual = ultimo + timedelta(days=1)


def fetch_report_particionado(endpoint, headers, fecha_inicio, fecha_fin,
                              dias=DIAS_POR_LOTE):
    """Consulta un reporte de Cubbo en ventanas de dias, en paralelo, y concatena."""
    ventanas = list(iterar_ventanas_fecha(fecha_inicio, fecha_fin, dias))
    partes = []
    fallidas = []

    def traer(ventana):
        desde, hasta = ventana
        parte = fetch_report(endpoint, {"date_from": desde, "date_to": hasta}, headers)
        return desde, hasta, parte

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_REPORTES) as pool:
        futuros = {pool.submit(traer, v): v for v in ventanas}
        for futuro in as_completed(futuros):
            desde, hasta = futuros[futuro]
            try:
                _, _, parte = futuro.result()
                log.info("  %s..%s -> %s registros", desde, hasta, len(parte))
                if not parte.empty:
                    partes.append(parte)
            except Exception as exc:
                fallidas.append(f"{desde}..{hasta}")
                log.error("  %s..%s fallo: %s", desde, hasta, exc)

    if fallidas and not partes:
        raise RuntimeError(
            f"Fallaron las {len(fallidas)} ventanas del rango; no hay datos que cargar"
        )
    if fallidas:
        log.warning("  %s de %s ventanas fallaron (%s). La tabla queda INCOMPLETA "
                    "en esos tramos.", len(fallidas), len(ventanas), ", ".join(fallidas))

    return pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()


def fetch_all_plans(base_url, private_key, project_id, page_size=500,
                    fecha_inicio=None):
    """Descarga planes de Routal; se detiene al pasar fecha_inicio si vienen ordenados."""
    limite = None
    if fecha_inicio and PLANS_PARADA_ANTICIPADA:
        limite = pd.to_datetime(fecha_inicio).date()

    documentos = []
    offset = 0
    orden_descendente = True
    fecha_previa = None
    paginas_fuera_rango = 0

    while True:
        respuesta = SESION.get(
            f"{base_url}/plans",
            params={
                "private_key": private_key,
                "project_id": project_id,
                "limit": page_size,
                "offset": offset,
            },
            timeout=TIMEOUT,
        )
        respuesta.raise_for_status()
        pagina = respuesta.json().get("docs", [])
        if not pagina:
            break

        documentos.extend(pagina)

        # Fechas de esta pagina + verificacion del orden
        fechas_pagina = []
        for doc in pagina:
            valor = pd.to_datetime(doc.get("execution_date"), errors="coerce", utc=True)
            if pd.isna(valor):
                continue
            fecha = valor.date()
            fechas_pagina.append(fecha)
            if fecha_previa is not None and fecha > fecha_previa:
                orden_descendente = False
            fecha_previa = fecha

        offset += page_size
        if len(pagina) < page_size:
            break

        # Parada anticipada: dos paginas seguidas fuera de rango.
        if limite and orden_descendente and fechas_pagina:
            if max(fechas_pagina) < limite:
                paginas_fuera_rango += 1
                if paginas_fuera_rango >= 2:
                    log.info("  Parada anticipada: %s planes revisados, el resto "
                             "es anterior a %s", len(documentos), limite)
                    break
            else:
                paginas_fuera_rango = 0

    if limite and not orden_descendente:
        log.info("  Los planes no vienen ordenados por fecha; se descargo el "
                 "historico completo (%s)", len(documentos))

    return json_normalize(documentos) if documentos else pd.DataFrame()


def filtrar_planes_por_fecha(df_planes, fecha_inicio, fecha_fin):
    """Filtra planes por execution_date; el resultado se reutiliza para 'rutas'."""
    if df_planes.empty or "execution_date" not in df_planes.columns:
        return df_planes

    df = df_planes.copy()
    fechas = pd.to_datetime(df["execution_date"], errors="coerce", utc=True)
    if ZONA_HORARIA:
        fechas = fechas.dt.tz_convert(ZONA_HORARIA)
    df["_fecha_filtro"] = fechas.dt.date

    inicio = pd.to_datetime(fecha_inicio).date()
    fin = pd.to_datetime(fecha_fin).date()
    df = df[
        df["_fecha_filtro"].notna()
        & (df["_fecha_filtro"] >= inicio)
        & (df["_fecha_filtro"] <= fin)
    ].drop(columns=["_fecha_filtro"]).reset_index(drop=True)

    log.info("  Planes en el rango %s..%s: %s", fecha_inicio, fecha_fin, len(df))
    return df


def _descargar_rutas_de_plan(plan_id, private_key, project_id, base_url, page_size=500):
    """Descarga (paginando) las rutas de un solo plan. Devuelve lista de rutas."""
    offset = 0
    rutas_plan = []
    while True:
        respuesta = SESION.get(
            f"{base_url}/plan/{plan_id}/routes",
            params={"private_key": private_key, "limit": page_size, "offset": offset},
            timeout=TIMEOUT,
        )
        respuesta.raise_for_status()
        pagina = respuesta.json()
        if isinstance(pagina, dict):
            pagina = pagina.get("docs", [])
        if not isinstance(pagina, list) or not pagina:
            break
        rutas_plan.extend(pagina)
        offset += page_size
        if len(pagina) < page_size:
            break

    for ruta in rutas_plan:
        ruta["plan_id_consulta"] = plan_id
        ruta["project_id_consulta"] = project_id
    return rutas_plan


def fetch_rutas(df_planes_filtrado, private_key, project_id, base_url, page_size=500):
    """Descarga las rutas de los planes en paralelo (MAX_WORKERS_RUTAS a la vez)."""
    if df_planes_filtrado is None or df_planes_filtrado.empty:
        return pd.DataFrame()

    plan_ids = df_planes_filtrado["id"].tolist()
    total = len(plan_ids)
    rutas = []
    fallidos = []
    hechos = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_RUTAS) as pool:
        futuros = {
            pool.submit(_descargar_rutas_de_plan, pid, private_key,
                        project_id, base_url, page_size): pid
            for pid in plan_ids
        }
        for futuro in as_completed(futuros):
            pid = futuros[futuro]
            hechos += 1
            try:
                rutas.extend(futuro.result())
            except Exception as exc:
                fallidos.append(str(pid))
                log.error("  Fallo el plan %s: %s", pid, exc)
            if hechos % 50 == 0 or hechos == total:
                log.info("  Rutas: %s/%s planes (%s rutas acumuladas)",
                         hechos, total, len(rutas))

    if fallidos:
        log.warning("  %s planes sin rutas por error: %s",
                    len(fallidos), ", ".join(fallidos[:10]))

    return pd.DataFrame(rutas) if rutas else pd.DataFrame()


def fetch_table_data(table_name, cfg, df_planes_cache=None):
    """Enruta cada tabla a su fuente correspondiente."""
    origen = cfg.get("source")
    if origen == "routal":
        completo = fetch_all_plans(
            ROUTAL_BASE_URL, ROUTAL_PRIVATE_KEY, ROUTAL_PROJECT_ID,
            fecha_inicio=params["date_from"],
        )
        return filtrar_planes_por_fecha(completo, params["date_from"], params["date_to"])
    if origen == "routal_rutas":
        return fetch_rutas(df_planes_cache, ROUTAL_PRIVATE_KEY,
                           ROUTAL_PROJECT_ID, ROUTAL_BASE_URL)
    return fetch_report_particionado(
        cfg["endpoint"], headers, params["date_from"], params["date_to"]
    )


# =============================================================================
# 5. CARGA MYSQL
# =============================================================================

def _es_nulo(valor):
    """pd.isna seguro: no revienta con listas, dicts ni cadenas."""
    if valor is None:
        return True
    try:
        resultado = pd.isna(valor)
    except (TypeError, ValueError):
        return False
    if isinstance(resultado, bool):
        return resultado
    return False


def tipo_mysql(serie, columna, cols_fecha, id_col):
    """Determina el tipo MySQL de una columna segun sus valores reales."""
    if columna == id_col:
        return "VARCHAR(255) NOT NULL"
    if columna in cols_fecha:
        # DATETIME si los valores traen hora (datetime), DATE si son solo fecha.
        muestra = [v for v in serie.tolist() if not _es_nulo(v)]
        if muestra and all(isinstance(v, datetime) for v in muestra):
            return "DATETIME"
        return "DATE"

    valores = [v for v in serie.tolist() if not _es_nulo(v)]
    if not valores:
        return "MEDIUMTEXT"
    if all(isinstance(v, bool) for v in valores):
        return "TINYINT(1)"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in valores):
        return "BIGINT"
    if all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in valores):
        return "DOUBLE"
    return "MEDIUMTEXT"


def create_table_if_not_exists(cursor, table_name, df, id_col, cols_fecha):
    columnas_sql = [
        f"`{col}` {tipo_mysql(df[col], col, cols_fecha, id_col)}"
        for col in df.columns
    ]
    sentencia = (
        f"CREATE TABLE IF NOT EXISTS `{table_name}` (\n  "
        + ",\n  ".join(columnas_sql)
        + f",\n  PRIMARY KEY (`{id_col}`)\n) CHARACTER SET utf8mb4;"
    )
    cursor.execute(sentencia)


def sincronizar_columnas(cursor, db_name, table_name, df, id_col, cols_fecha):
    """Agrega a la tabla las columnas nuevas que la API haya empezado a devolver."""
    cursor.execute(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (db_name, table_name),
    )
    existentes = {fila[0] for fila in cursor.fetchall()}
    faltantes = [c for c in df.columns if c not in existentes]

    for col in faltantes:
        tipo = tipo_mysql(df[col], col, cols_fecha, id_col).replace(" NOT NULL", "")
        cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{col}` {tipo}")
        log.warning("  columna nueva en %s -> %s (%s)", table_name, col, tipo)


def migrar_tipos_fecha(cursor, db_name, table_name, cols_fecha):
    """Convierte a DATE columnas de fecha que sigan como DATETIME/texto (opcional)."""
    if not cols_fecha:
        return
    cursor.execute(
        "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
        (db_name, table_name),
    )
    for columna, tipo_actual in cursor.fetchall():
        if columna in cols_fecha and str(tipo_actual).lower() != "date":
            try:
                cursor.execute(f"ALTER TABLE `{table_name}` MODIFY `{columna}` DATE")
                log.info("  %s.%s: %s -> DATE", table_name, columna, tipo_actual)
            except Exception as exc:
                log.error("  No se pudo migrar %s.%s: %s", table_name, columna, exc)


def _valor_sql(valor):
    """NaN / NaT / pd.NA -> None, para que MySQL guarde NULL real."""
    return None if _es_nulo(valor) else valor


def upsert_dataframe(cursor, table_name, df, lote=TAMANO_LOTE):
    """INSERT ... ON DUPLICATE KEY UPDATE, ejecutado por lotes."""
    if df.empty:
        return 0

    columnas = df.columns.tolist()
    columnas_sql = ", ".join(f"`{c}`" for c in columnas)
    marcadores = ", ".join(["%s"] * len(columnas))
    actualizacion = ", ".join(f"`{c}`=VALUES(`{c}`)" for c in columnas)

    sentencia = (
        f"INSERT INTO `{table_name}` ({columnas_sql}) VALUES ({marcadores}) "
        f"ON DUPLICATE KEY UPDATE {actualizacion}"
    )

    filas = [
        tuple(_valor_sql(v) for v in fila)
        for fila in df.itertuples(index=False, name=None)
    ]

    procesadas = 0
    for i in range(0, len(filas), lote):
        bloque = filas[i:i + lote]
        cursor.executemany(sentencia, bloque)
        procesadas += len(bloque)
    return procesadas


def sincronizar_borrado(cursor, table_name, id_col, ids_en_api,
                        fecha_inicio, fecha_fin):
    """Borra del rango los registros que la API ya no trae. Con freno de seguridad.
    Devuelve (borrados, motivo)."""
    col_rango = COLUMNA_RANGO_BORRADO.get(table_name)
    if not col_rango:
        return 0, "tabla sin columna de rango configurada"

    # IDs que la base tiene en este rango
    cursor.execute(
        f"SELECT `{id_col}` FROM `{table_name}` "
        f"WHERE `{col_rango}` >= %s AND `{col_rango}` <= %s",
        (fecha_inicio, fecha_fin),
    )
    ids_en_base = {fila[0] for fila in cursor.fetchall()}

    if not ids_en_base:
        return 0, "la base no tiene registros en este rango"

    # FRENO DE SEGURIDAD: si la API trajo demasiado poco, no borrar.
    if len(ids_en_api) < len(ids_en_base) * UMBRAL_MINIMO_BORRADO:
        return 0, (
            f"FRENO: la API trajo {len(ids_en_api)} pero la base tiene "
            f"{len(ids_en_base)} en el rango (posible fallo de la API); "
            f"no se borra nada"
        )

    # Los que estan en base pero YA NO en la API -> eliminados en el origen
    a_borrar = ids_en_base - set(ids_en_api)
    if not a_borrar:
        return 0, "nada que borrar (la base coincide con la API)"

    # Borrar por lotes
    a_borrar = list(a_borrar)
    borrados = 0
    for i in range(0, len(a_borrar), TAMANO_LOTE):
        lote = a_borrar[i:i + TAMANO_LOTE]
        marcadores = ", ".join(["%s"] * len(lote))
        cursor.execute(
            f"DELETE FROM `{table_name}` WHERE `{id_col}` IN ({marcadores})",
            lote,
        )
        borrados += cursor.rowcount
    return borrados, f"{borrados} registros eliminados (ya no existen en la API)"


def limpiar_valores_vacios_mysql(conn, db_name, id_cols=None):
    """Mantenimiento manual: pasa a NULL las celdas de texto vacias (solo tablas base).
    No se llama en cada corrida; transformar_df ya limpia al insertar."""
    id_cols = id_cols or {}
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT c.TABLE_NAME, c.COLUMN_NAME "
            "FROM INFORMATION_SCHEMA.COLUMNS c "
            "JOIN INFORMATION_SCHEMA.TABLES t "
            "  ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME "
            "WHERE c.TABLE_SCHEMA = %s "
            "  AND t.TABLE_TYPE = 'BASE TABLE' "
            "  AND c.DATA_TYPE IN ('varchar','text','mediumtext','longtext','char')",
            (db_name,),
        )
        columnas = cursor.fetchall()

    total = 0
    for tabla, columna in columnas:
        if columna == id_cols.get(tabla):
            continue
        with conn.cursor() as cursor:
            cursor.execute(
                f"UPDATE `{tabla}` SET `{columna}` = NULL "
                f"WHERE `{columna}` IS NOT NULL "
                f"AND REPLACE(REPLACE(`{columna}`, UNHEX('C2A0'), ' '), CHAR(9), ' ') "
                f"REGEXP '^[[:space:]]*$'"
            )
            if cursor.rowcount > 0:
                log.info("  %s.%s: %s filas vacias -> NULL",
                         tabla, columna, cursor.rowcount)
                total += cursor.rowcount
    conn.commit()
    if total == 0:
        log.info("  Sin valores vacios pendientes.")
    return total


def conectar(con_base=True):
    cfg = dict(DB_CONFIG)
    if con_base:
        cfg["database"] = DB_NAME
    return pymysql.connect(**cfg)


# =============================================================================
# 6. MAIN
# =============================================================================

def validar_configuracion():
    faltantes = []
    if not DB_CONFIG["password"]:
        faltantes.append("MYSQL_PASSWORD")
    if not CUBBO_API_KEY:
        faltantes.append("CUBBO_API_KEY")
    if not ROUTAL_PRIVATE_KEY:
        faltantes.append("ROUTAL_PRIVATE_KEY")
    if faltantes:
        raise SystemExit(
            "Faltan credenciales: " + ", ".join(faltantes) + "\n"
            "Definelas como variables de entorno antes de ejecutar, por ejemplo:\n"
            '  PowerShell:  $env:MYSQL_PASSWORD="tu_password"\n'
            "  CMD:         set MYSQL_PASSWORD=tu_password\n"
            "O escribelas directamente en la seccion CONFIGURACION de este archivo."
        )


def main():
    validar_configuracion()

    # --- Crear la base si no existe ----------------------------------------
    conn = conectar(con_base=False)
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}` CHARACTER SET utf8mb4;"
            )
        conn.commit()
    finally:
        conn.close()

    conn = conectar()
    resumen = {}
    df_planes_cache = None

    try:
        for table_name, cfg in TABLES_CONFIG.items():
            log.info("Procesando %s...", table_name)
            try:
                df = fetch_table_data(table_name, cfg, df_planes_cache)
                if table_name == "plans":
                    df_planes_cache = df.copy()

                log.info("  %s registros obtenidos", len(df))
                if df.empty:
                    resumen[table_name] = "sin datos"
                    continue

                df, cols_fecha = transformar_df(df, table_name, cfg["id_col"])
                df = aplicar_etiquetas(df, table_name)

                with conn.cursor() as cursor:
                    create_table_if_not_exists(
                        cursor, table_name, df, cfg["id_col"], cols_fecha
                    )
                    sincronizar_columnas(
                        cursor, DB_NAME, table_name, df, cfg["id_col"], cols_fecha
                    )
                    if MIGRAR_TIPOS_FECHA:
                        migrar_tipos_fecha(cursor, DB_NAME, table_name, cols_fecha)
                conn.commit()

                with conn.cursor() as cursor:
                    filas = upsert_dataframe(cursor, table_name, df)
                conn.commit()

                # --- Sincronizacion con borrado -----------------------------
                # Borra de la base lo que la API ya no trae en este rango
                # (registros eliminados en el origen). Con freno de seguridad.
                borrados_info = ""
                if SINCRONIZAR_BORRADO and cfg["id_col"] in df.columns:
                    ids_api = df[cfg["id_col"]].dropna().tolist()
                    with conn.cursor() as cursor:
                        borrados, motivo = sincronizar_borrado(
                            cursor, table_name, cfg["id_col"], ids_api,
                            params["date_from"], params["date_to"],
                        )
                    conn.commit()
                    if borrados > 0:
                        log.warning("  %s: %s", table_name, motivo)
                        borrados_info = f", {borrados} borrados"
                    elif "FRENO" in motivo:
                        log.warning("  %s: %s", table_name, motivo)
                        borrados_info = ", borrado OMITIDO (freno)"

                resumen[table_name] = f"{filas} filas{borrados_info}"
                log.info("  %s cargada correctamente (%s filas%s)",
                         table_name, filas, borrados_info)

            except Exception as exc:
                conn.rollback()
                resumen[table_name] = f"ERROR: {exc}"
                log.exception("  Fallo la tabla %s", table_name)

    finally:
        conn.close()

    log.info("=" * 60)
    log.info("RESUMEN")
    for tabla, estado in resumen.items():
        log.info("  %-12s %s", tabla, estado)


if __name__ == "__main__":
    main()