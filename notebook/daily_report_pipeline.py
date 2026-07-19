# =============================================================================
# AUTOMATED DAILY PORTFOLIO REPORT PIPELINE
# =============================================================================
#
# Autor     : Ronaldo Chiche Surco
# Stack     : Microsoft Fabric · Spark SQL · Python · Power Automate
# Propósito : Pipeline end-to-end de reportería automática de cartera
#             crediticia para microfinanzas.
#
# ARQUITECTURA DEL PIPELINE:
# ┌─────────────────────────────────────────────────────────────┐
# │  Fabric Pipeline (scheduled Mon–Fri, 22:34  UTC-5)          │
# │  └── Notebook Spark SQL + Python                            │
# │       ├── CELDA 1 · Parámetros centralizados (auto-fecha)   │
# │       ├── CELDA 2 · Query cartera detalle (analisisAF_*)    │
# │       ├── CELDA 3 · Query PAR8d diario                      │
# │       ├── CELDA 4 · Query tasa recuperación por agencia     │
# │       ├── CELDA 5 · Construcción del objeto D (métricas)    │
# │       └── CELDA 6 · Generación HTML + envío Power Automate  │
# └─────────────────────────────────────────────────────────────┘
#
# INDICADORES CLAVE MONITOREADOS:
#   - Avance de desembolsos vs presupuesto (total, semanal, catorcenal)
#   - PAR8d por segmento (semanal recurrentes/nuevos, catorcenal)
#   - Tasa de recuperación de desertores por agencia y perfil
#   - Nuevos clientes por agencia vs meta
#   - Ranking de agencias por desembolso y calidad de cartera
#
# CONFIGURACIÓN REQUERIDA:
#   1. Reemplazar FLOW_URL con la URL de tu flujo en Power Automate
#   2. Actualizar DESTINATARIOS_TO y DESTINATARIOS_CC
#   3. Ajustar ID_EMPRESA y PRODUCTOS según la entidad
#   4. Configurar la tabla RCH_CONFIG_AGENCIAS con agencias críticas/piloto
#
# =============================================================================

# ============================================================================
# CELDA 1 · PARÁMETROS CENTRALIZADOS · Pipeline Entidad Microfinanciera
#
# Todo dinámico — sin fechas hardcodeadas.
#
# Lógica de fecha de corte (zona horaria Centroamérica):
#   Lunes  → sábado anterior (último día hábil de semana)
#   Demás  → ayer
#
# Para correr un mes cerrado: poner periodo_reporte="202606" y fecha_corte="20260630"
# Para auto-resolución: dejar ambos en ""
#
# Ejecutar esta celda PRIMERO. Las variables quedan disponibles
# para todas las celdas siguientes del notebook.
# ============================================================================

import calendar
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from calendar import monthrange

# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  PARÁMETROS EDITABLES — dejar en "" para auto-resolución                 ║
# ╚══════════════════════════════════════════════════════════════════════════╝
periodo_reporte = ""    # ej. "202607" — vacío = automático
fecha_corte_par = ""    # ej. "20260708" — vacío = automático

# ── AUTO-RESOLUCIÓN (zona horaria Guatemala/Centroamérica) ──────────────────
hoy = datetime.now(ZoneInfo("America/Lima")).date()  # Ajustar a la zona horaria de la entidad

if not periodo_reporte and not fecha_corte_par:
    # Ambos vacíos → derivar de ayer (o sábado si hoy es lunes)
    _retro  = 2 if hoy.weekday() == 0 else 1
    _fc_dt  = hoy - timedelta(days=_retro)
    fecha_corte_par  = _fc_dt.strftime("%Y%m%d")
    periodo_reporte  = fecha_corte_par[:6]
elif periodo_reporte and not fecha_corte_par:
    # Solo periodo → último día del mes (cierre formal)
    _anio, _mes = int(periodo_reporte[:4]), int(periodo_reporte[4:])
    fecha_corte_par = date(_anio, _mes, monthrange(_anio, _mes)[1]).strftime("%Y%m%d")
elif fecha_corte_par and not periodo_reporte:
    periodo_reporte = fecha_corte_par[:6]
# Ambos dados → respetar sin modificar

# Validaciones
assert len(periodo_reporte) == 6 and periodo_reporte.isdigit(), f"periodo_reporte inválido: {periodo_reporte}"
assert len(fecha_corte_par) == 8 and fecha_corte_par.isdigit(),  f"fecha_corte inválido: {fecha_corte_par}"
assert fecha_corte_par[:6] == periodo_reporte,     f"Inconsistencia: fecha_corte ({fecha_corte_par}) fuera del mes ({periodo_reporte})"

FECHA_CORTE     = fecha_corte_par              # "20260708"  (STRING — queries 3 y 5)
FECHA_CORTE_INT = int(FECHA_CORTE)             # 20260708   (INT    — query 4 y 5)

# ── PERIODOS ────────────────────────────────────────────────────────────────
PERIODO      = periodo_reporte                 # "202607"
_anio_p, _mes_p = int(PERIODO[:4]), int(PERIODO[4:])
if _mes_p == 1:
    PERIODO_PREV = f"{_anio_p - 1}12"
else:
    PERIODO_PREV = f"{_anio_p}{_mes_p - 1:02d}"

# Confirmación visual
_modo = "📅 MES EN CURSO" if PERIODO == hoy.strftime("%Y%m") else "🗓️  MES CERRADO"
_dias = ['lunes','martes','miércoles','jueves','viernes','sábado','domingo']
print("═" * 55)
print(f"  {_modo}")
print(f"  PERIODO      : {PERIODO}  |  PERIODO_PREV : {PERIODO_PREV}")
print(f"  FECHA_CORTE  : {FECHA_CORTE}")
print(f"  Hoy (GT)     : {hoy.strftime('%Y-%m-%d')} ({_dias[hoy.weekday()]})")
print("═" * 55)

PERIODO_INICIO = PERIODO_PREV                  # ventana del detalle: mes prev + mes actual

# ── PRIMER DÍA DEL MES DEL CORTE ────────────────────────────────────────────
# Usado como fecha_inicio_recup en query 5 y fecha_inicio en query 4
FECHA_INICIO_MES     = int(f"{PERIODO}01")   # 20260701 (INT)
FECHA_INICIO_MES_STR = f"{PERIODO}01"        # "20260701" (STRING)

# ── BASE SEMANAL ACTIVA (query 5 · fecha_corte_base) ────────────────────────
# = FECHA_VIGENCIA más reciente disponible en RCH_DESERTORES_AFG
_base_row = spark.sql(f"""
    SELECT MAX(FECHA_VIGENCIA) AS max_vig
    FROM LKH_Operaciones.dbo.RCH_DESERTORES_AFG
    WHERE FECHA_VIGENCIA <= {FECHA_CORTE_INT}
""").collect()[0]
FECHA_BASE = int(_base_row["max_vig"])         # 20260707 (INT)

# ── PRESUPUESTO desde hec.presupuesto ───────────────────────────────────────
_ppto = {
    r["ProductoAgrupado"]: float(r["ppto_ml"])
    for r in spark.sql(f"""
        SELECT b.ProductoAgrupado, SUM(a.ppto_ml) AS ppto_ml
        FROM LKH_Operaciones.hec.presupuesto a
        INNER JOIN LKH_Operaciones.dim.productos b ON a.idproducto = b.idproducto
        WHERE a.aniomes     = {int(PERIODO)}
          AND a.idempresa   = 8
          AND a.idoperacion = 20
          AND a.moneda      = 'ML'
        GROUP BY b.ProductoAgrupado
    """).collect()
}
PPTO_SEM   = _ppto.get("MICRO SEMANAL",    0.0)
PPTO_CAT   = _ppto.get("MICRO CATORCENAL", 0.0)
PPTO_TOTAL = PPTO_SEM + PPTO_CAT

# ── EMPRESA Y PRODUCTOS ──────────────────────────────────────────────────────
ID_EMPRESA = 8
EMPRESA    = "Entidad Microfinanciera"
PRODUCTOS  = ["MICRO SEMANAL", "MICRO CATORCENAL"]

# ── CORREO ───────────────────────────────────────────────────────────────────
DESTINATARIOS_TO = ["gerencia@institucion-financiera.com"]
DESTINATARIOS_CC = [
    # Agregar destinatarios reales aquí
    "analitica@institucion-financiera.com",
    "riesgos@institucion-financiera.com",
    "negocios@institucion-financiera.com",
]
# [Lista de distribución configurada en DESTINATARIOS_TO y DESTINATARIOS_CC]

# Asunto usa FECHA_CORTE (fuente de verdad) para evitar discrepancias de zona horaria
ASUNTO   = f"Tablero Seguimiento Medidas Entidad Microfinanciera · Corte {FECHA_CORTE[6:8]}/{FECHA_CORTE[4:6]}/{FECHA_CORTE[:4]}"
FLOW_URL = "YOUR_POWER_AUTOMATE_FLOW_URL_HERE"  # Reemplazar con URL real del flujo

# ── RUTAS ─────────────────────────────────────────────────────────────────────
OUTPUT_DIR  = "/lakehouse/default/Files/reportes_af/"
OUTPUT_NAME = f"Tablero_AFGT_{PERIODO}_{FECHA_CORTE}.html"

# ── RESUMEN ───────────────────────────────────────────────────────────────────
print("=" * 52)
print("  PARÁMETROS AF GUATEMALA")
print("=" * 52)
print(f"  Fecha corte:    {FECHA_CORTE[6:8]}/{FECHA_CORTE[4:6]}/{FECHA_CORTE[:4]}  ({FECHA_CORTE})")
print(f"  Periodo:        {PERIODO}  (prev: {PERIODO_PREV})")
print(f"  Inicio mes:     {FECHA_INICIO_MES}")
print(f"  Base activa:    {FECHA_BASE}")
print(f"  PPTO Semanal:   Q{PPTO_SEM:>14,.2f}")
print(f"  PPTO Catorc.:   Q{PPTO_CAT:>14,.2f}")
print(f"  Output:         {OUTPUT_NAME}")
print("=" * 52)
if PPTO_SEM == 0: print("⚠️  PPTO_SEM = 0 — verificar hec.presupuesto")
if PPTO_CAT == 0: print("⚠️  PPTO_CAT = 0 — verificar hec.presupuesto")
if FECHA_BASE < FECHA_CORTE_INT - 14:
    print(f"⚠️  Base con {FECHA_CORTE_INT - FECHA_BASE} días de antigüedad — ¿llegó nueva base?")



# ============================================================================
# CELDA 2 · QUERY 3 — analisisAF_Cartera_Detalle
# ============================================================================

spark.sql(f"""
DROP TABLE IF EXISTS LKH_Operaciones.dbo.analisisAF_Cartera_Detalle
""")

spark.sql(f"""
CREATE TABLE LKH_Operaciones.dbo.analisisAF_Cartera_Detalle AS
WITH
Parametros AS (
    SELECT
        '{PERIODO_INICIO}'  AS periodo_inicio,
        '{FECHA_CORTE}'     AS fecha_corte_actual,
        '{PERIODO}'         AS periodo_actual
),
TipoCambioEmpresa AS (
    SELECT e.IdEmpresa,
           COALESCE(MAX(CAST(tc.Tc AS DOUBLE)), MAX(CAST(e.TipoCambio AS DOUBLE))) AS TC
    FROM LKH_Operaciones.dim.Empresas e
    CROSS JOIN Parametros p
    LEFT JOIN LKH_Operaciones.dbo.tipocambio tc
        ON tc.IdEmpresa = e.IdEmpresa AND tc.Aniomes = p.periodo_actual
    WHERE e.IdEmpresa IN (6, 7, 8)
    GROUP BY e.IdEmpresa
),
CiclosAgr AS (
    SELECT idempresa, ncodcred, ncodage,
           MAX(ciclo) AS ciclo, MAX(rangociclos) AS rangociclos
    FROM LKH_Operaciones.dim.ciclos_cliente
    GROUP BY idempresa, ncodcred, ncodage
),
EvalCap AS (
    SELECT idempresa, idcontrato,
           MAX(nnivelventas) AS niveldeventas, MAX(nUtilidadNeta) AS utilidadneta
    FROM LKH_Operaciones.dim.evalcapacidadpago
    GROUP BY idempresa, idcontrato
),
Cosechas AS (
    SELECT date_format(to_timestamp(FECHA), 'yyyyMM') AS PERIODO,
           idempresa, ncodpignoraticio, ncodage,
           MAX(score) AS score, MAX(rangoscore) AS rangoscore,
           MAX(ImporteDesembolsoAnterior) AS ImporteDesembolsoAnterior,
           MAX(TIR) AS TIR, MAX(TasaCalculada) AS TasaCalculada,
           MAX(RangoTasaTIR_Agrupada) AS RangoTasaTIR_Agrupada,
           MAX(NivelVentas) AS NivelVentas, SUM(Cartera_B) AS Cartera_B
    FROM LKH_Operaciones.hec.contratoscosechas
    CROSS JOIN Parametros p
    WHERE date_format(to_timestamp(FECHA), 'yyyyMM') >= p.periodo_inicio
      AND date_format(to_timestamp(FECHA), 'yyyyMM') <= p.periodo_actual
    GROUP BY date_format(to_timestamp(FECHA), 'yyyyMM'), idempresa, ncodpignoraticio, ncodage
),
FechaFinMes AS (
    SELECT t.aniomes AS Periodo, MAX(t.idFecha) AS fecha_findemes
    FROM LKH_Operaciones.dim.Tiempo t
    CROSS JOIN Parametros p
    WHERE t.aniomes >= p.periodo_inicio AND t.aniomes < p.periodo_actual AND t.findemes = 1
    GROUP BY t.aniomes
),
FechaCorteActual AS (
    SELECT periodo_actual AS Periodo, CAST(fecha_corte_actual AS BIGINT) AS idFechaCorte
    FROM Parametros
),
FechasCorte AS (
    SELECT Periodo, fecha_findemes AS idFechaCorte, 'CIERRE' AS tipo_corte FROM FechaFinMes
    UNION ALL
    SELECT Periodo, idFechaCorte, 'EN_CURSO' FROM FechaCorteActual
),
desembolsos_origen AS (
    SELECT op.IdEmpresa, op.IdContrato,
           MIN(op.IdFechaOperacion) AS fecha_desembolso,
           SUM(op.Importe) AS imp_ml, SUM(op.ImporteUSD) AS imp_usd
    FROM LKH_Operaciones.hec.Operaciones op
    CROSS JOIN Parametros par
    WHERE op.idempresa IN (6, 7, 8) AND op.idoperacion IN (2, 10)
      AND CAST(op.IdFechaOperacion AS STRING) <= par.fecha_corte_actual
    GROUP BY op.IdEmpresa, op.IdContrato
),
desembolsos_detalle AS (
    SELECT SUBSTR(CAST(op.IdFechaOperacion AS STRING), 1, 6) AS Periodo,
           op.IdEmpresa, op.idcontrato, op.ncodpignoraticio, op.ncodage,
           op.idcliente, op.idproducto, e.NomEmpresa, p.productoagrupado, ag.cnomage,
           MIN(op.IdFechaOperacion) AS fecha_desembolso,
           SUM(op.Importe) AS imp_ml, SUM(op.ImporteUSD) AS imp_usd
    FROM LKH_Operaciones.hec.Operaciones op
    INNER JOIN LKH_Operaciones.dim.empresas  e  ON op.IdEmpresa  = e.IdEmpresa
    INNER JOIN LKH_Operaciones.dim.productos p  ON op.idproducto = p.idproducto
    INNER JOIN LKH_Operaciones.dim.agencias  ag ON op.ncodage    = ag.ncodage AND op.idempresa = ag.idempresa
    CROSS JOIN Parametros par
    WHERE op.idempresa IN (6, 7, 8) AND op.idoperacion IN (2, 10)
      AND SUBSTR(CAST(op.IdFechaOperacion AS STRING), 1, 6) >= par.periodo_inicio
      AND CAST(op.IdFechaOperacion AS STRING) <= par.fecha_corte_actual
    GROUP BY SUBSTR(CAST(op.IdFechaOperacion AS STRING), 1, 6),
             op.IdEmpresa, op.idcontrato, op.ncodpignoraticio, op.ncodage,
             op.idcliente, op.idproducto, e.NomEmpresa, p.productoagrupado, ag.cnomage
),
Cartera AS (
    SELECT
        c.aniomes AS Periodo, k.NomEmpresa AS Empresa, a.IdEmpresa, a.IdContrato,
        a.ncodpignoraticio AS ncodcred, a.ncodage, a.idcliente,
        d.productoagrupado, b.cnomage, b.cZonaConsol2, e.cnomusu, a.ntasaint,
        a.diastranscurridos, f.ciclo, f.rangociclos, g.cnomcod,
        ct.nnrocuotas, ct.categoriacontrato,
        DES.imp_ml, DES.imp_usd, DES.fecha_desembolso,
        CASE WHEN COALESCE(ct.breadecuacion,0)=1 THEN 'READECUACION'
             WHEN COALESCE(ct.breprestamo,0)=1   THEN 'REPRESTAMO'
             WHEN COALESCE(ct.brefinanciado,0)=1 THEN 'REFINANCIADO'
             ELSE 'OTRO' END AS tipo_credito,
        ct.nmontocuota, ec.niveldeventas, ec.utilidadneta,
        ch.score, ch.rangoscore, ch.importedesembolsoanterior,
        ch.TIR, ch.TasaCalculada, ch.RangoTasaTIR_Agrupada, ch.NivelVentas,
        est.Desestratificacion AS Estratificacion,
        SUM(a.Importe) AS SALDO_CARTERA
    FROM LKH_Operaciones.hec.Cartera a
    CROSS JOIN Parametros par
    LEFT JOIN LKH_Operaciones.dim.agencias b ON a.ncodage = b.ncodage AND a.idempresa = b.idempresa
    INNER JOIN LKH_Operaciones.dim.Tiempo c ON a.idFechaOperacion = c.idFecha
    INNER JOIN FechasCorte fc ON c.aniomes = fc.Periodo AND a.idFechaOperacion = fc.idFechaCorte
    INNER JOIN LKH_Operaciones.dim.Productos d ON a.idproducto = d.idproducto
    LEFT JOIN LKH_Operaciones.dim.winusuarios e ON a.idoficialcredito = e.idusuario AND a.IdEmpresa = e.IdEmpresa
    LEFT JOIN CiclosAgr f ON a.idempresa = f.idempresa AND a.ncodpignoraticio = f.ncodcred AND a.ncodage = f.ncodage
    LEFT JOIN LKH_Operaciones.dim.categoriascontratos g ON a.idempresa = g.idempresa AND a.idcategoriacontrato = g.idcategoriacontrato
    LEFT JOIN LKH_Operaciones.dim.contratos ct ON a.IdEmpresa = ct.IdEmpresa AND a.IdContrato = ct.IdContrato
    LEFT JOIN desembolsos_origen DES ON a.IdEmpresa = DES.IdEmpresa AND a.IdContrato = DES.IdContrato
    LEFT JOIN LKH_Operaciones.dim.Empresas k ON a.IdEmpresa = k.IdEmpresa
    LEFT JOIN Cosechas ch ON a.idempresa = ch.idempresa AND a.ncodpignoraticio = ch.ncodpignoraticio
        AND a.ncodage = ch.ncodage AND LEFT(CAST(a.idFechaOperacion AS STRING), 6) = ch.PERIODO
    LEFT JOIN EvalCap ec ON a.idempresa = ec.idempresa AND a.IdContrato = ec.idcontrato
    LEFT JOIN LKH_Operaciones.dim.estratificacioncorporativa est ON a.idestratificacion = est.idestratificacion
    WHERE a.idOperacion = 1 AND a.IdEmpresa IN (6, 7, 8)
      AND c.aniomes >= par.periodo_inicio AND c.aniomes <= par.periodo_actual
    GROUP BY c.aniomes, k.NomEmpresa, a.IdEmpresa, a.IdContrato, a.ncodpignoraticio, a.ncodage,
             a.idcliente, d.productoagrupado, b.cnomage, b.cZonaConsol2, e.cnomusu, a.ntasaint,
             a.diastranscurridos, f.ciclo, f.rangociclos, g.cnomcod, ct.nnrocuotas, ct.categoriacontrato,
             DES.imp_ml, DES.imp_usd, DES.fecha_desembolso,
             CASE WHEN COALESCE(ct.breadecuacion,0)=1 THEN 'READECUACION'
                  WHEN COALESCE(ct.breprestamo,0)=1 THEN 'REPRESTAMO'
                  WHEN COALESCE(ct.brefinanciado,0)=1 THEN 'REFINANCIADO'
                  ELSE 'OTRO' END,
             ct.nmontocuota, ec.niveldeventas, ec.utilidadneta,
             ch.score, ch.rangoscore, ch.importedesembolsoanterior,
             ch.TIR, ch.TasaCalculada, ch.RangoTasaTIR_Agrupada, ch.NivelVentas, est.Desestratificacion
),
CarteraClasificada AS (
    SELECT
        ca.Periodo, ca.Empresa, ca.IdEmpresa, ca.IdContrato, ca.ncodcred, ca.ncodage, ca.IdCliente,
        ca.diastranscurridos,
        CASE WHEN ca.diastranscurridos <= 0  THEN 'Al día'
             WHEN ca.diastranscurridos <= 3  THEN '1-3 días'
             WHEN ca.diastranscurridos <= 7  THEN '4-7 días'
             WHEN ca.diastranscurridos <= 15 THEN '8-15 días'
             WHEN ca.diastranscurridos <= 30 THEN '16-30 días'
             WHEN ca.diastranscurridos <= 60 THEN '31-60 días'
             WHEN ca.diastranscurridos <= 90 THEN '61-90 días'
             ELSE '90+días' END AS rango_atraso,
        ca.CICLO,
        CASE WHEN ca.ciclo IS NULL THEN 'SIN REGISTRO'
             WHEN ca.ciclo <= 1    THEN 'Nuevos'
             ELSE                       'Recurrentes' END AS TipoCliente,
        ca.productoagrupado, ca.cnomage, ca.cZonaConsol2, ca.cnomusu, ca.ntasaint,
        ca.cnomcod, ca.nnrocuotas, ca.categoriacontrato,
        ca.imp_ml, ca.imp_usd, ca.fecha_desembolso, ca.tipo_credito, ca.nmontocuota,
        CASE WHEN tcm.TC IS NULL OR tcm.TC = 0 THEN NULL ELSE ca.nmontocuota / tcm.TC END AS nmontocuota_usd,
        ca.score, ca.rangoscore, ca.importedesembolsoanterior,
        ca.TIR, ca.TasaCalculada, ca.RangoTasaTIR_Agrupada, ca.NivelVentas, ca.Estratificacion,
        CASE WHEN ca.nnrocuotas IS NULL THEN 'SIN REGISTRO'
             WHEN ca.nnrocuotas <= 3  THEN '1 a 3'  WHEN ca.nnrocuotas <= 6  THEN '3 a 6'
             WHEN ca.nnrocuotas <= 9  THEN '6 a 9'  WHEN ca.nnrocuotas <= 12 THEN '9 a 12'
             WHEN ca.nnrocuotas <= 18 THEN '12 a 18' WHEN ca.nnrocuotas <= 24 THEN '18 a 24'
             ELSE '24 a más' END AS rango_plazo,
        CASE WHEN ca.niveldeventas = 1 THEN 'Subsistencia'
             WHEN ca.niveldeventas = 2 THEN 'Acumulación simple'
             WHEN ca.niveldeventas = 3 THEN 'Acumulación ampliada'
             ELSE 'SIN REGISTRO' END AS niveldeventas_desc,
        ca.utilidadneta / 4.0 AS capacidad_pago,
        CASE WHEN ca.utilidadneta IS NULL OR ca.utilidadneta = 0 THEN NULL
             ELSE ca.nmontocuota / (ca.utilidadneta / 4.0) END AS ratio_capacidad_pago,
        ca.imp_ml - COALESCE(ca.importedesembolsoanterior, 0) AS incremento_desembolso,
        CASE WHEN ca.importedesembolsoanterior IS NULL OR ca.importedesembolsoanterior = 0 THEN NULL
             ELSE (ca.imp_ml - ca.importedesembolsoanterior) / ca.importedesembolsoanterior END AS incremento_desembolso_pct,
        ca.SALDO_CARTERA,
        CASE WHEN tcm.TC IS NULL OR tcm.TC = 0 THEN NULL ELSE ca.SALDO_CARTERA / tcm.TC END AS SALDO_CARTERA_USD
    FROM Cartera ca
    LEFT JOIN TipoCambioEmpresa tcm ON ca.IdEmpresa = tcm.IdEmpresa
),
ClavesCartera AS (SELECT DISTINCT IdEmpresa, IdContrato, Periodo FROM Cartera),
SoloContratos AS (
    SELECT
        DES.Periodo, c.NomEmpresa AS Empresa, DES.IdEmpresa,
        DES.idcontrato AS IdContrato, DES.ncodpignoraticio AS ncodcred, DES.ncodage,
        CAST(NULL AS BIGINT) AS IdCliente,
        0 AS diastranscurridos, 'Al día' AS rango_atraso, f.ciclo,
        CASE WHEN f.ciclo IS NULL THEN 'SIN REGISTRO'
             WHEN f.ciclo <= 1    THEN 'Nuevos'
             ELSE                      'Recurrentes' END AS TipoCliente,
        DES.productoagrupado, b.cnomage, b.cZonaConsol2,
        CAST(NULL AS STRING) AS cnomusu, h.ntasaint, CAST(NULL AS STRING) AS cnomcod,
        h.nnrocuotas, h.categoriacontrato, DES.imp_ml, DES.imp_usd, DES.fecha_desembolso,
        CASE WHEN COALESCE(h.breadecuacion,0)=1 THEN 'READECUACION'
             WHEN COALESCE(h.breprestamo,0)=1   THEN 'REPRESTAMO'
             WHEN COALESCE(h.brefinanciado,0)=1 THEN 'REFINANCIADO'
             ELSE 'OTRO' END AS tipo_credito,
        h.nmontocuota,
        CASE WHEN tcm.TC IS NULL OR tcm.TC = 0 THEN NULL ELSE h.nmontocuota / tcm.TC END AS nmontocuota_usd,
        CAST(NULL AS INT) AS score, CAST(NULL AS STRING) AS rangoscore,
        ch.importedesembolsoanterior,
        CAST(NULL AS DOUBLE) AS TIR, CAST(NULL AS DOUBLE) AS TasaCalculada,
        CAST(NULL AS STRING) AS RangoTasaTIR_Agrupada, CAST(NULL AS STRING) AS NivelVentas,
        CAST(NULL AS STRING) AS Estratificacion,
        CASE WHEN h.nnrocuotas IS NULL THEN 'SIN REGISTRO'
             WHEN h.nnrocuotas <= 3  THEN '1 a 3'  WHEN h.nnrocuotas <= 6  THEN '3 a 6'
             WHEN h.nnrocuotas <= 9  THEN '6 a 9'  WHEN h.nnrocuotas <= 12 THEN '9 a 12'
             WHEN h.nnrocuotas <= 18 THEN '12 a 18' WHEN h.nnrocuotas <= 24 THEN '18 a 24'
             ELSE '24 a más' END AS rango_plazo,
        CASE WHEN ec.niveldeventas = 1 THEN 'Subsistencia'
             WHEN ec.niveldeventas = 2 THEN 'Acumulación simple'
             WHEN ec.niveldeventas = 3 THEN 'Acumulación ampliada'
             ELSE 'SIN REGISTRO' END AS niveldeventas_desc,
        ec.utilidadneta / 4.0 AS capacidad_pago,
        CASE WHEN ec.utilidadneta IS NULL OR ec.utilidadneta = 0 THEN NULL
             ELSE h.nmontocuota / (ec.utilidadneta / 4.0) END AS ratio_capacidad_pago,
        DES.imp_ml - COALESCE(ch.importedesembolsoanterior, 0) AS incremento_desembolso,
        CASE WHEN ch.importedesembolsoanterior IS NULL OR ch.importedesembolsoanterior = 0 THEN NULL
             ELSE (DES.imp_ml - ch.importedesembolsoanterior) / ch.importedesembolsoanterior END AS incremento_desembolso_pct,
        0.0 AS SALDO_CARTERA, 0.0 AS SALDO_CARTERA_USD
    FROM desembolsos_detalle DES
    CROSS JOIN Parametros par
    LEFT JOIN LKH_Operaciones.dim.agencias b ON DES.ncodage = b.ncodage AND DES.idempresa = b.idempresa
    LEFT JOIN LKH_Operaciones.dim.empresas c ON DES.IdEmpresa = c.IdEmpresa
    LEFT JOIN CiclosAgr f ON DES.idempresa = f.idempresa AND DES.ncodpignoraticio = f.ncodcred AND DES.ncodage = f.ncodage
    LEFT JOIN LKH_Operaciones.dim.contratos h ON DES.IdEmpresa = h.IdEmpresa AND DES.IdContrato = h.IdContrato
    LEFT JOIN EvalCap ec ON DES.idempresa = ec.idempresa AND DES.IdContrato = ec.idcontrato
    LEFT JOIN Cosechas ch ON DES.idempresa = ch.idempresa AND DES.ncodpignoraticio = ch.ncodpignoraticio
        AND DES.ncodage = ch.ncodage AND DES.PERIODO = ch.PERIODO
    LEFT JOIN TipoCambioEmpresa tcm ON DES.IdEmpresa = tcm.IdEmpresa
    WHERE DES.idempresa IN (6, 7, 8)
      AND NOT EXISTS (SELECT 1 FROM ClavesCartera k
          WHERE k.IdEmpresa = DES.IdEmpresa AND k.IdContrato = DES.idcontrato AND k.Periodo = DES.PERIODO)
),
PerfilesDedup AS (
    SELECT IdCliente, UPPER(PRODUCTO) AS PRODUCTO, UPPER(TIPO_CLIENTE) AS TIPO_CLIENTE,
           FECHA_VIGENCIA, FECHA_VIGENCIA_FIN, MAX(PERFIL_CLIENTE) AS PERFIL_CLIENTE
    FROM LKH_Operaciones.dbo.RCH_PERFILES_AFG
    GROUP BY IdCliente, UPPER(PRODUCTO), UPPER(TIPO_CLIENTE), FECHA_VIGENCIA, FECHA_VIGENCIA_FIN
),
ClavesGT AS (
    SELECT DISTINCT u.Periodo, u.IdEmpresa, u.IdContrato, u.IdCliente,
        UPPER(u.productoagrupado) AS productoagrupado, UPPER(u.TipoCliente) AS TipoCliente,
        CASE WHEN u.fecha_desembolso IS NOT NULL
                  AND SUBSTR(CAST(u.fecha_desembolso AS STRING), 1, 6) = u.Periodo
             THEN u.fecha_desembolso
             ELSE fc.idFechaCorte END AS fecha_ref_perfil
    FROM (
        SELECT Periodo, IdEmpresa, IdContrato, IdCliente, productoagrupado, TipoCliente, fecha_desembolso FROM CarteraClasificada
        UNION ALL
        SELECT Periodo, IdEmpresa, IdContrato, IdCliente, productoagrupado, TipoCliente, fecha_desembolso FROM SoloContratos
    ) u
    LEFT JOIN FechasCorte fc ON fc.Periodo = u.Periodo
    WHERE u.IdEmpresa = 8 AND u.IdCliente IS NOT NULL
),
PerfilAplicable AS (
    SELECT Periodo, IdEmpresa, IdContrato, PERFIL_CLIENTE, FECHA_VIGENCIA FROM (
        SELECT c.Periodo, c.IdEmpresa, c.IdContrato, p.PERFIL_CLIENTE, p.FECHA_VIGENCIA,
               ROW_NUMBER() OVER (PARTITION BY c.Periodo, c.IdEmpresa, c.IdContrato ORDER BY p.FECHA_VIGENCIA DESC) AS rn
        FROM ClavesGT c
        INNER JOIN PerfilesDedup p
            ON p.IdCliente = c.IdCliente AND p.PRODUCTO = c.productoagrupado
            AND p.TIPO_CLIENTE = c.TipoCliente
            AND c.fecha_ref_perfil BETWEEN p.FECHA_VIGENCIA AND p.FECHA_VIGENCIA_FIN
        WHERE c.fecha_ref_perfil IS NOT NULL
    ) x WHERE rn = 1
),
DesertoresDedup AS (
    SELECT IdCliente, UPPER(PRODUCTO) AS PRODUCTO, UPPER(TIPO_CLIENTE) AS TIPO_CLIENTE,
           FECHA_VIGENCIA, FECHA_VIGENCIA_FIN, MAX(PERFIL_CLIENTE) AS PERFIL_CLIENTE
    FROM dbo.RCH_DESERTORES_AFG
    GROUP BY IdCliente, UPPER(PRODUCTO), UPPER(TIPO_CLIENTE), FECHA_VIGENCIA, FECHA_VIGENCIA_FIN
),
DesertorAplicable AS (
    SELECT Periodo, IdEmpresa, IdContrato, PERFIL_CLIENTE, FECHA_VIGENCIA FROM (
        SELECT c.Periodo, c.IdEmpresa, c.IdContrato, d.PERFIL_CLIENTE, d.FECHA_VIGENCIA,
               ROW_NUMBER() OVER (PARTITION BY c.Periodo, c.IdEmpresa, c.IdContrato ORDER BY d.FECHA_VIGENCIA DESC) AS rn
        FROM ClavesGT c
        INNER JOIN DesertoresDedup d
            ON d.IdCliente = c.IdCliente AND d.PRODUCTO = c.productoagrupado
            AND d.FECHA_VIGENCIA <= c.fecha_ref_perfil
        WHERE c.fecha_ref_perfil IS NOT NULL
    ) x WHERE rn = 1
),
solicitudes AS (
    SELECT e.NomEmpresa AS sol_empresa, s.nCodCred AS sol_ncodcred, s.nCodAge AS sol_ncodage, MAX(s.cUser) AS cuser
    FROM (
        SELECT 8 AS IdEmpresa, ncodcred AS nCodCred, ncodage AS nCodAge, nestado AS nEstado, cuser AS cUser FROM LKH_Operaciones.afg.credestados
        UNION ALL SELECT 6, ncodcred, ncodage, nestado, cuser FROM LKH_Operaciones.afh.credestados
        UNION ALL SELECT 7, ncodcred, ncodage, nestado, cuser FROM LKH_Operaciones.afs.credestados
    ) s
    INNER JOIN LKH_Operaciones.dim.Empresas e ON s.IdEmpresa = e.IdEmpresa
    WHERE s.nEstado = 10
    GROUP BY e.NomEmpresa, s.nCodCred, s.nCodAge
),
producto_anterior AS (
    SELECT h.IdEmpresa, h.IdContrato, pr.productoagrupado AS producto_anterior
    FROM (
        SELECT d.IdEmpresa, d.IdCliente, d.IdContrato,
               LAG(d.IdProducto) OVER (PARTITION BY d.IdEmpresa, d.IdCliente ORDER BY d.fecha_desembolso, d.IdContrato) AS IdProducto_anterior
        FROM (
            SELECT op.IdEmpresa, op.IdCliente, op.IdContrato,
                   MIN(op.IdFechaOperacion) AS fecha_desembolso, MAX(op.IdProducto) AS IdProducto
            FROM LKH_Operaciones.hec.Operaciones op
            WHERE op.idoperacion IN (2, 10) AND op.IdEmpresa IN (6, 7, 8)
            GROUP BY op.IdEmpresa, op.IdCliente, op.IdContrato
        ) d
    ) h
    LEFT JOIN LKH_Operaciones.dim.productos pr ON h.IdProducto_anterior = pr.idproducto
)
SELECT
    x.Periodo, x.Empresa, x.IdEmpresa, x.IdContrato, x.ncodcred, x.ncodage, x.IdCliente,
    x.diastranscurridos AS dias_atraso, x.rango_atraso, x.ciclo, x.TipoCliente,
    x.productoagrupado, pa.producto_anterior, x.cnomage AS agencia, x.cZonaConsol2 AS zona,
    TRIM(REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(x.cnomusu, '[\\\\r\\\\n]', ' '), ',', ''), '\\t', ' '), ' +', ' ')) AS usuario,
    sol.cuser AS cuser_aprobador, x.ntasaint, x.cnomcod AS categoria_contrato, x.nnrocuotas,
    x.imp_ml AS montodesembolso, x.imp_usd AS montodesembolso_usd, x.fecha_desembolso,
    CASE WHEN x.fecha_desembolso IS NULL THEN NULL
         ELSE (CAST(x.Periodo AS INT) DIV 100 * 12 + CAST(x.Periodo AS INT) % 100)
            - (CAST(x.fecha_desembolso AS BIGINT) DIV 10000 * 12 + (CAST(x.fecha_desembolso AS BIGINT) DIV 100) % 100)
    END AS maduracion_meses,
    x.tipo_credito, x.nmontocuota AS monto_cuota, x.nmontocuota_usd AS monto_cuota_usd,
    x.score, x.rangoscore,
    ap.PERFIL_CLIENTE AS P_Propuesto,
    CASE WHEN des.PERFIL_CLIENTE IS NOT NULL THEN 1 ELSE 0 END AS flag_desertor,
    des.PERFIL_CLIENTE AS perfil_desertor,
    x.importedesembolsoanterior AS monto_desembolso_anterior,
    x.incremento_desembolso, x.incremento_desembolso_pct,
    x.TIR, x.TasaCalculada, x.RangoTasaTIR_Agrupada, x.NivelVentas, x.Estratificacion,
    x.rango_plazo, x.niveldeventas_desc, x.capacidad_pago, x.ratio_capacidad_pago,
    SUM(x.SALDO_CARTERA) AS saldo, SUM(x.SALDO_CARTERA_USD) AS saldo_usd
FROM (SELECT * FROM CarteraClasificada UNION ALL SELECT * FROM SoloContratos) x
LEFT JOIN PerfilAplicable ap ON ap.Periodo = x.Periodo AND ap.IdEmpresa = x.IdEmpresa AND ap.IdContrato = x.IdContrato
LEFT JOIN solicitudes sol ON x.Empresa = sol.sol_empresa AND x.ncodcred = sol.sol_ncodcred AND x.ncodage = sol.sol_ncodage
LEFT JOIN producto_anterior pa ON pa.IdEmpresa = x.IdEmpresa AND pa.IdContrato = x.IdContrato
LEFT JOIN DesertorAplicable des ON des.Periodo = x.Periodo AND des.IdEmpresa = x.IdEmpresa AND des.IdContrato = x.IdContrato
GROUP BY
    x.Periodo, x.Empresa, x.IdEmpresa, x.IdContrato, x.ncodcred, x.ncodage, x.IdCliente,
    x.diastranscurridos, x.rango_atraso, x.ciclo, x.TipoCliente, x.productoagrupado,
    pa.producto_anterior, x.cnomage, x.cZonaConsol2,
    TRIM(REPLACE(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(x.cnomusu, '[\\\\r\\\\n]', ' '), ',', ''), '\\t', ' '), ' +', ' ')),
    sol.cuser, x.ntasaint, x.cnomcod, x.nnrocuotas, x.imp_ml, x.imp_usd, x.fecha_desembolso,
    CASE WHEN x.fecha_desembolso IS NULL THEN NULL
         ELSE (CAST(x.Periodo AS INT) DIV 100 * 12 + CAST(x.Periodo AS INT) % 100)
            - (CAST(x.fecha_desembolso AS BIGINT) DIV 10000 * 12 + (CAST(x.fecha_desembolso AS BIGINT) DIV 100) % 100) END,
    x.tipo_credito, x.nmontocuota, x.nmontocuota_usd, x.score, x.rangoscore,
    ap.PERFIL_CLIENTE, des.PERFIL_CLIENTE, x.importedesembolsoanterior,
    x.incremento_desembolso, x.incremento_desembolso_pct,
    x.TIR, x.TasaCalculada, x.RangoTasaTIR_Agrupada, x.NivelVentas, x.Estratificacion,
    x.rango_plazo, x.niveldeventas_desc, x.capacidad_pago, x.ratio_capacidad_pago
""")
print(f"✅ Query 3 completado — analisisAF_Cartera_Detalle  corte {FECHA_CORTE}")


# ============================================================================
# CELDA 3 · QUERY 4 — analisisAF_PAR8d_Diario
# ============================================================================

spark.sql("DROP TABLE IF EXISTS LKH_Operaciones.dbo.analisisAF_PAR8d_Diario")

spark.sql(f"""
CREATE TABLE LKH_Operaciones.dbo.analisisAF_PAR8d_Diario AS
WITH
Parametros AS (
    SELECT
        {FECHA_INICIO_MES} AS fecha_inicio,
        {FECHA_CORTE_INT}  AS fecha_fin,
        8                  AS id_empresa
),
CiclosAgr AS (
    SELECT idempresa, ncodcred, ncodage,
           MAX(ciclo) AS ciclo,
           UPPER(CASE WHEN MAX(ciclo) IS NULL THEN 'SIN REGISTRO'
                      WHEN MAX(ciclo) <= 1    THEN 'NUEVOS'
                      ELSE                         'RECURRENTES' END) AS TipoCliente
    FROM LKH_Operaciones.dim.ciclos_cliente
    CROSS JOIN Parametros par
    WHERE idempresa = par.id_empresa
    GROUP BY idempresa, ncodcred, ncodage
),
PerfilesDedup AS (
    SELECT IdCliente, UPPER(PRODUCTO) AS PRODUCTO, UPPER(TIPO_CLIENTE) AS TIPO_CLIENTE,
           FECHA_VIGENCIA, FECHA_VIGENCIA_FIN,
           MAX(CONCAT('P', REGEXP_REPLACE(UPPER(TRIM(PERFIL_CLIENTE)), '^P', ''))) AS PERFIL_CLIENTE
    FROM LKH_Operaciones.dbo.RCH_PERFILES_AFG
    GROUP BY IdCliente, UPPER(PRODUCTO), UPPER(TIPO_CLIENTE), FECHA_VIGENCIA, FECHA_VIGENCIA_FIN
),
DesembolsosOrigen AS (
    SELECT op.IdEmpresa, op.IdContrato, MIN(op.IdFechaOperacion) AS fecha_desembolso
    FROM LKH_Operaciones.hec.Operaciones op
    CROSS JOIN Parametros par
    WHERE op.idempresa = par.id_empresa AND op.idoperacion IN (2, 10)
      AND op.IdFechaOperacion <= par.fecha_fin
    GROUP BY op.IdEmpresa, op.IdContrato
),
CarteraConPerfil AS (
    SELECT * FROM (
        SELECT a.idFechaOperacion AS Fecha, UPPER(d.productoagrupado) AS Producto,
            a.IdContrato, a.Importe, a.diastranscurridos, p.PERFIL_CLIENTE,
            ROW_NUMBER() OVER (PARTITION BY a.idFechaOperacion, a.IdContrato ORDER BY p.FECHA_VIGENCIA DESC NULLS LAST) AS rn
        FROM LKH_Operaciones.hec.Cartera a
        INNER JOIN LKH_Operaciones.dim.Productos d ON a.idproducto = d.idproducto
        LEFT JOIN CiclosAgr cc ON cc.idempresa = a.IdEmpresa AND cc.ncodcred = a.ncodpignoraticio AND cc.ncodage = a.ncodage
        LEFT JOIN DesembolsosOrigen des ON des.IdEmpresa = a.IdEmpresa AND des.IdContrato = a.IdContrato
        LEFT JOIN PerfilesDedup p
            ON p.IdCliente = a.IdCliente AND p.PRODUCTO = UPPER(d.productoagrupado)
            AND p.TIPO_CLIENTE = cc.TipoCliente
            AND CASE WHEN des.fecha_desembolso IS NOT NULL
                          AND SUBSTR(CAST(des.fecha_desembolso AS STRING), 1, 6) = SUBSTR(CAST(a.idFechaOperacion AS STRING), 1, 6)
                     THEN des.fecha_desembolso ELSE a.idFechaOperacion END
                BETWEEN p.FECHA_VIGENCIA AND p.FECHA_VIGENCIA_FIN
        CROSS JOIN Parametros par
        WHERE a.idOperacion = 1 AND a.IdEmpresa = par.id_empresa
          AND a.idFechaOperacion >= par.fecha_inicio AND a.idFechaOperacion <= par.fecha_fin
    ) x WHERE rn = 1
)
SELECT
    Fecha, Producto,
    COALESCE(PERFIL_CLIENTE, 'SIN PERFIL') AS Perfil,
    COUNT(DISTINCT IdContrato) AS n_contratos,
    SUM(Importe) AS saldo_total,
    SUM(CASE WHEN diastranscurridos > 8 THEN Importe ELSE 0 END) AS saldo_atraso8d,
    CASE WHEN SUM(Importe) = 0 THEN NULL
         ELSE SUM(CASE WHEN diastranscurridos > 8 THEN Importe ELSE 0 END) / SUM(Importe)
    END AS PAR8d
FROM CarteraConPerfil
GROUP BY Fecha, Producto, COALESCE(PERFIL_CLIENTE, 'SIN PERFIL')
""")
print(f"✅ Query 4 completado — analisisAF_PAR8d_Diario  corte {FECHA_CORTE}")


# ============================================================================
# CELDA 4 · QUERY 5 — analisisAF_TasaRecuperacion_Agencia
# ============================================================================

spark.sql("DROP TABLE IF EXISTS LKH_Operaciones.dbo.analisisAF_TasaRecuperacion_Agencia")

spark.sql(f"""
CREATE TABLE LKH_Operaciones.dbo.analisisAF_TasaRecuperacion_Agencia AS
WITH
Parametros AS (
    SELECT
        {FECHA_INICIO_MES} AS fecha_inicio_recup,
        {FECHA_CORTE_INT}  AS fecha_corte_actual,
        {FECHA_BASE}       AS fecha_corte_base,
        8                  AS id_empresa
),
DesertoresDedup AS (
    SELECT r.IdContrato, r.IdCliente,
           UPPER(r.PRODUCTO) AS PRODUCTO, UPPER(r.TIPO_CLIENTE) AS TIPO_CLIENTE,
           r.PERFIL_CLIENTE, r.FECHA_VIGENCIA,
           ROW_NUMBER() OVER (
               PARTITION BY r.IdCliente, UPPER(r.PRODUCTO), UPPER(r.TIPO_CLIENTE)
               ORDER BY r.FECHA_VIGENCIA DESC
           ) AS rn_global
    FROM LKH_Operaciones.dbo.RCH_DESERTORES_AFG r
    CROSS JOIN Parametros par
    WHERE r.FECHA_VIGENCIA <= par.fecha_corte_actual
),
DesertoresAlgunaVez AS (
    SELECT IdContrato, IdCliente, PRODUCTO, TIPO_CLIENTE, PERFIL_CLIENTE, FECHA_VIGENCIA
    FROM DesertoresDedup WHERE rn_global = 1
),
DesertoresAlgunaVezConAgencia AS (
    SELECT ct.IdEmpresa, ct.nCodAge, ag.cnomage AS agencia, ag.cZonaConsol2 AS zona,
        d.IdContrato, d.IdCliente, d.PRODUCTO, d.TIPO_CLIENTE, d.PERFIL_CLIENTE, d.FECHA_VIGENCIA
    FROM DesertoresAlgunaVez d
    INNER JOIN LKH_Operaciones.dim.Contratos ct ON ct.IdContrato = d.IdContrato
    INNER JOIN LKH_Operaciones.dim.Agencias ag ON ag.nCodAge = ct.nCodAge AND ag.IdEmpresa = ct.IdEmpresa
    CROSS JOIN Parametros par
    WHERE ct.IdEmpresa = par.id_empresa
),
VigenciaActiva AS (
    SELECT UPPER(PRODUCTO) AS PRODUCTO, UPPER(TIPO_CLIENTE) AS TIPO_CLIENTE,
           FECHA_VIGENCIA AS FECHA_VIGENCIA_ACTIVA
    FROM LKH_Operaciones.dbo.RCH_DESERTORES_AFG
    CROSS JOIN Parametros par
    WHERE FECHA_VIGENCIA = par.fecha_corte_base
      AND PERIODO = SUBSTR(CAST(par.fecha_corte_base AS STRING), 1, 6)
    GROUP BY UPPER(PRODUCTO), UPPER(TIPO_CLIENTE), FECHA_VIGENCIA
),
DesertoresActivosConAgencia AS (
    SELECT ct.IdEmpresa, ct.nCodAge, ag.cnomage AS agencia, ag.cZonaConsol2 AS zona,
        r.IdContrato, r.IdCliente,
        UPPER(r.PRODUCTO) AS PRODUCTO, UPPER(r.TIPO_CLIENTE) AS TIPO_CLIENTE,
        r.PERFIL_CLIENTE, r.FECHA_VIGENCIA
    FROM LKH_Operaciones.dbo.RCH_DESERTORES_AFG r
    INNER JOIN VigenciaActiva v ON UPPER(r.PRODUCTO) = v.PRODUCTO
        AND UPPER(r.TIPO_CLIENTE) = v.TIPO_CLIENTE AND r.FECHA_VIGENCIA = v.FECHA_VIGENCIA_ACTIVA
    INNER JOIN LKH_Operaciones.dim.Contratos ct ON ct.IdContrato = r.IdContrato
    INNER JOIN LKH_Operaciones.dim.Agencias ag ON ag.nCodAge = ct.nCodAge AND ag.IdEmpresa = ct.IdEmpresa
    CROSS JOIN Parametros par
    WHERE ct.IdEmpresa = par.id_empresa
      AND r.PERIODO = SUBSTR(CAST(par.fecha_corte_base AS STRING), 1, 6)
),
DesembolsosRecuperados AS (
    SELECT DISTINCT det.IdEmpresa, det.IdCliente,
        UPPER(det.productoagrupado) AS PRODUCTO, UPPER(det.TipoCliente) AS TIPO_CLIENTE
    FROM LKH_Operaciones.dbo.analisisAF_Cartera_Detalle det
    CROSS JOIN Parametros par
    WHERE det.IdEmpresa = par.id_empresa AND det.flag_desertor = 1
      AND det.fecha_desembolso >= par.fecha_inicio_recup
      AND det.fecha_desembolso <= par.fecha_corte_actual
),
BasePorAgencia AS (
    SELECT IdEmpresa, nCodAge, agencia, zona, PRODUCTO, TIPO_CLIENTE, PERFIL_CLIENTE,
           COUNT(DISTINCT IdCliente) AS base_asignada
    FROM DesertoresActivosConAgencia
    GROUP BY IdEmpresa, nCodAge, agencia, zona, PRODUCTO, TIPO_CLIENTE, PERFIL_CLIENTE
),
RecuperadosPorAgencia AS (
    SELECT au_dedup.IdEmpresa, au_dedup.nCodAge,
           dr.PRODUCTO, dr.TIPO_CLIENTE, au_dedup.PERFIL_CLIENTE,
           COUNT(DISTINCT au_dedup.IdCliente) AS recuperados
    FROM (
        SELECT au.IdEmpresa, au.IdCliente, au.PRODUCTO, au.nCodAge,
               au.PERFIL_CLIENTE, au.FECHA_VIGENCIA,
               ROW_NUMBER() OVER (
                   PARTITION BY au.IdEmpresa, au.IdCliente, au.PRODUCTO
                   ORDER BY CASE WHEN au.FECHA_VIGENCIA <= par.fecha_corte_base THEN 0 ELSE 1 END,
                            au.FECHA_VIGENCIA DESC
               ) AS rn
        FROM DesertoresAlgunaVezConAgencia au
        CROSS JOIN Parametros par
    ) au_dedup
    INNER JOIN DesembolsosRecuperados dr
        ON dr.IdEmpresa = au_dedup.IdEmpresa AND dr.IdCliente = au_dedup.IdCliente
        AND dr.PRODUCTO = au_dedup.PRODUCTO
    WHERE au_dedup.rn = 1
    GROUP BY au_dedup.IdEmpresa, au_dedup.nCodAge, dr.PRODUCTO, dr.TIPO_CLIENTE, au_dedup.PERFIL_CLIENTE
)
SELECT
    COALESCE(b.IdEmpresa, r.IdEmpresa) AS IdEmpresa,
    COALESCE(b.nCodAge,   r.nCodAge)   AS nCodAge,
    COALESCE(b.agencia,   ag.cnomage)  AS agencia,
    COALESCE(b.zona,      ag.cZonaConsol2) AS zona,
    COALESCE(b.PRODUCTO,  r.PRODUCTO)  AS PRODUCTO,
    COALESCE(b.TIPO_CLIENTE, r.TIPO_CLIENTE) AS TIPO_CLIENTE,
    COALESCE(b.PERFIL_CLIENTE, r.PERFIL_CLIENTE) AS perfil_desertor,
    COALESCE(b.base_asignada, 0) AS base_asignada,
    COALESCE(r.recuperados, 0)   AS recuperados,
    CASE WHEN COALESCE(b.base_asignada, 0) = 0 THEN NULL
         ELSE COALESCE(r.recuperados, 0) * 1.0 / b.base_asignada
    END AS tasa_recuperacion
FROM BasePorAgencia b
FULL OUTER JOIN RecuperadosPorAgencia r
    ON r.IdEmpresa = b.IdEmpresa AND r.nCodAge = b.nCodAge
    AND r.PRODUCTO = b.PRODUCTO AND r.TIPO_CLIENTE = b.TIPO_CLIENTE
    AND r.PERFIL_CLIENTE IS NOT DISTINCT FROM b.PERFIL_CLIENTE
LEFT JOIN LKH_Operaciones.dim.Agencias ag ON ag.nCodAge = r.nCodAge AND ag.IdEmpresa = r.IdEmpresa
""")
print(f"✅ Query 5 completado — analisisAF_TasaRecuperacion_Agencia  corte {FECHA_CORTE}")

print("\n🏁 Pipeline de datos completado. Continuar con la generación del HTML.")

# ============================================================================
# CELDA 5 · CONSTRUCCIÓN DEL OBJETO D + GENERACIÓN DEL HTML
#
# Prerequisito: CELDA 1 del notebook 00_parametros.py ya ejecutada
# (variables PERIODO, PERIODO_PREV, FECHA_CORTE, PPTO_*, etc. disponibles)
#
# Fuentes:
#   - LKH_Operaciones.dbo.analisisAF_Cartera_Detalle   (queries 3)
#   - LKH_Operaciones.dbo.analisisAF_TasaRecuperacion_Agencia (query 5)
#
# Bloques de D que se construyen:
#   kpi, totales, perfiles, agencias, piloto,
#   nuevos, nuevos_resumen, efectividad, react, efe_resumen, top200
# ============================================================================

from pyspark.sql import functions as F

# ── Variables de sesión (definidas en 00_parametros.py) ──────────────────────
# Si este notebook se corre independiente, se usan estos valores de respaldo.
try:
    _ = EMPRESA
except NameError:
    from datetime import date, timedelta
    _hoy = date.today()
    _corte = _hoy - timedelta(days=1) if _hoy.weekday() != 0 else _hoy - timedelta(days=2)
    PERIODO      = _corte.strftime("%Y%m")
    PERIODO_PREV = f"{_corte.year - 1}12" if _corte.month == 1 else f"{_corte.year}{_corte.month - 1:02d}"
    FECHA_CORTE  = _corte.strftime("%Y%m%d")
    PPTO_SEM     = 0.0
    PPTO_CAT     = 0.0
    PPTO_TOTAL   = 0.0
    EMPRESA      = "Entidad Microfinanciera"
    PRODUCTOS    = ["MICRO SEMANAL", "MICRO CATORCENAL"]
    OUTPUT_DIR   = "/lakehouse/default/Files/reportes_af/"
    OUTPUT_NAME  = f"Tablero_AFGT_{PERIODO}_{FECHA_CORTE}.html"
    print(f"⚠️  Variables de sesión no encontradas — usando fallback (PERIODO={PERIODO})")
    print("   Ejecuta primero 00_parametros.py para valores correctos de PPTO.")

import json, os

# ── Listas de agencias desde tabla de configuración ──────────────────────────
# Para agregar/quitar agencias: editar RCH_CONFIG_AGENCIAS_AFG directamente.
_config = spark.sql("""
    SELECT tipo, agencia
    FROM LKH_Operaciones.dbo.RCH_CONFIG_AGENCIAS_AFG
    WHERE activo = true
""").toPandas()

AGENCIAS_CRITICAS = set(_config[_config["tipo"] == "CRITICA"]["agencia"].tolist())
AGENCIAS_PILOTO   = set(_config[_config["tipo"] == "PILOTO"]["agencia"].tolist())
print(f"  Críticas ({len(AGENCIAS_CRITICAS)}): {sorted(AGENCIAS_CRITICAS)}")
print(f"  Piloto   ({len(AGENCIAS_PILOTO)}):   {sorted(AGENCIAS_PILOTO)}")

# ── Metas de nuevos por agencia (hardcode — confirmar con stakeholder de negocio si cambian)
META_NUEVOS_SEM = 5
META_NUEVOS_CAT = 2
META_NUEVOS_MEN = 2

# ── Descripción de perfiles ──────────────────────────────────────────────────
DESC_PERFIL = {
    "P1": "Cliente Ancla",
    "P2": "Cliente Sólido",
    "P3": "Cliente Regular",
    "P4": "En Alerta",
    "P5": "Crítico · Solo cobranza",
}

# ── Helpers ──────────────────────────────────────────────────────────────────
def par8d_expr():
    return F.when(
        F.sum("saldo") > 0,
        F.round(F.sum(F.when(F.col("dias_atraso") > 8, F.col("saldo")).otherwise(0)) / F.sum("saldo"), 6)
    ).otherwise(F.lit(0.0))

def norm_expr():
    return F.when(
        F.sum("saldo") > 0,
        F.round(F.sum(F.when(F.col("dias_atraso") <= 0, F.col("saldo")).otherwise(0)) / F.sum("saldo"), 6)
    ).otherwise(F.lit(0.0))

def _f(v):
    """Convierte a float seguro (None si nulo o NaN)."""
    try:
        if v is None: return None
        f = float(v)
        return None if f != f else f   # f != f es True solo si NaN
    except: return None

def _i(v):
    """Convierte a int seguro (0 si nulo o NaN)."""
    try:
        if v is None: return 0
        f = float(v)
        if f != f: return 0            # NaN
        return int(f)
    except: return 0

# ── Cargar tablas ────────────────────────────────────────────────────────────
# Filtro por empresa: se usa IdEmpresa=8 (AFG) directamente para evitar
# dependencia del texto exacto de la columna "Empresa"
det  = (spark.table("LKH_Operaciones.dbo.analisisAF_Cartera_Detalle")
        .filter(F.col("IdEmpresa") == 8)
        .filter(F.col("productoagrupado").isin(PRODUCTOS)))

tasa = spark.table("LKH_Operaciones.dbo.analisisAF_TasaRecuperacion_Agencia")

# Universos del periodo en curso
stock      = det.filter(F.col("Periodo") == PERIODO)           # cartera vigente
desem      = (stock                                             # desembolsos del mes actual
              .filter(F.col("maduracion_meses") == 0)
              .filter(F.col("fecha_desembolso").isNotNull())
              .filter(F.col("montodesembolso").isNotNull()))
stock_prev = det.filter(F.col("Periodo") == PERIODO_PREV)      # cartera mes anterior

# ── Diagnóstico rápido ────────────────────────────────────────────────────────
_n_desem = desem.count()
_n_prev  = stock_prev.count()
print(f"  PERIODO={PERIODO} | PERIODO_PREV={PERIODO_PREV} | FECHA_CORTE={FECHA_CORTE}")
print(f"  desem={_n_desem:,} | stock_prev={_n_prev:,}")
_tipos = sorted(set(str(r[0]) for r in desem.select("TipoCliente").distinct().collect()))
_perfs = [r[0] for r in desem.select("P_Propuesto").distinct().collect()]
_perfs_ok  = sorted(set(str(x) for x in _perfs if x))
_perfs_nil = sum(1 for x in _perfs if not x)
print(f"  TipoCliente: {_tipos}")
print(f"  P_Propuesto: {_perfs_ok}  (sin perfil={_perfs_nil})")

D = {}

# ============================================================================
# BLOQUE kpi
# ============================================================================
def _agg_desem(df):
    r = df.agg(
        F.countDistinct("IdContrato").alias("c"),
        F.round(F.sum("montodesembolso"), 2).alias("m")
    ).collect()[0]
    return _i(r["c"]), _f(r["m"]) or 0.0

c_tot, m_tot = _agg_desem(desem)
c_sem, m_sem = _agg_desem(desem.filter(F.col("productoagrupado") == "MICRO SEMANAL"))
c_cat, m_cat = _agg_desem(desem.filter(F.col("productoagrupado") == "MICRO CATORCENAL"))

D["kpi"] = {
    "contratos":    c_tot,
    "presupuesto":  round(PPTO_TOTAL, 2),
    "desembolsado": round(m_tot, 2),
    "avance":       round(m_tot / PPTO_TOTAL, 6) if PPTO_TOTAL else None,
    "avance_sem":   round(m_sem / PPTO_SEM, 6)   if PPTO_SEM   else None,
    "avance_cat":   round(m_cat / PPTO_CAT, 6)   if PPTO_CAT   else None,
}
_av = D['kpi']['avance']
print(f"✅ kpi: {c_tot} contratos | Q{m_tot:,.2f} desembolsado | {(_av or 0):.1%} avance")

# ============================================================================
# BLOQUE totales
# ============================================================================
TIENE_PERFIL = F.col("P_Propuesto").isin(["P1","P2","P3","P4","P5"])

def seg_metrics(prod, tipo):
    """Totales — SOLO contratos con perfil P1-P5. Campo sin_perfil para la fila amarilla."""
    f   = (F.col("productoagrupado") == prod) & (F.col("TipoCliente") == tipo)
    fd  = desem.filter(f & TIENE_PERFIL)
    fds = desem.filter(f & ~TIENE_PERFIL)
    fs  = stock.filter(f & TIENE_PERFIL)
    d   = fd.agg(
        F.countDistinct("IdContrato").alias("c"),
        F.round(F.sum("montodesembolso"), 2).alias("m"),
        par8d_expr().alias("par"), norm_expr().alias("norm")
    ).collect()[0]
    ds  = fds.agg(F.countDistinct("IdContrato").alias("c")).collect()[0]
    s   = fs.agg(
        F.round(F.sum("saldo"), 2).alias("cart"),
        par8d_expr().alias("par"), norm_expr().alias("norm")
    ).collect()[0]
    return {
        "contratos":   _i(d["c"]),
        "monto":       _f(d["m"]) or 0.0,
        "par_mes":     _f(d["par"]) or 0.0,
        "norm_mes":    _f(d["norm"]) or 0.0,
        "par_stock":   _f(s["par"]) or 0.0,
        "norm_stock":  _f(s["norm"]) or 0.0,
        "cartera":     _f(s["cart"]) or 0.0,
        "sin_perfil":  _i(ds["c"]),
    }

D["totales"] = {
    "sem_rec": seg_metrics("MICRO SEMANAL",    "Recurrentes"),
    "sem_new": seg_metrics("MICRO SEMANAL",    "Nuevos"),
    "cat_rec": seg_metrics("MICRO CATORCENAL", "Recurrentes"),
    "cat_new": seg_metrics("MICRO CATORCENAL", "Nuevos"),
}
print(f"✅ totales: sem_rec={D['totales']['sem_rec']['contratos']} (+{D['totales']['sem_rec']['sin_perfil']} sin perfil) | sem_new={D['totales']['sem_new']['contratos']} | cat_rec={D['totales']['cat_rec']['contratos']}")

# ============================================================================
# BLOQUE perfiles
# ============================================================================
def perfil_metrics(prod, tipo):
    """Métricas por perfil — stock calculado solo sobre clientes clasificados P1-P5."""
    f  = (F.col("productoagrupado") == prod) & (F.col("TipoCliente") == tipo)
    fd = desem.filter(f)
    fs = stock.filter(f & TIENE_PERFIL)
    result = []
    for p in ["P1", "P2", "P3", "P4", "P5"]:
        fp  = F.col("P_Propuesto") == p
        d   = fd.filter(fp).agg(
            F.countDistinct("IdContrato").alias("c"),
            F.round(F.sum("montodesembolso"), 2).alias("m"),
            par8d_expr().alias("par"), norm_expr().alias("norm")
        ).collect()[0]
        s   = fs.filter(fp).agg(
            F.round(F.sum("saldo"), 2).alias("cart"),
            par8d_expr().alias("par"), norm_expr().alias("norm")
        ).collect()[0]
        result.append({
            "perfil":     p,
            "desc":       DESC_PERFIL[p],
            "contratos":  _i(d["c"]),
            "monto":      _f(d["m"]) or 0.0,
            "par_mes":    _f(d["par"]),
            "norm_mes":   _f(d["norm"]),
            "par_stock":  _f(s["par"]) or 0.0,
            "norm_stock": _f(s["norm"]) or 0.0,
            "cartera":    _f(s["cart"]) or 0.0,
        })
    return result

D["perfiles"] = {
    "sem_rec": perfil_metrics("MICRO SEMANAL",    "Recurrentes"),
    "sem_new": perfil_metrics("MICRO SEMANAL",    "Nuevos"),
    "cat_rec": perfil_metrics("MICRO CATORCENAL", "Recurrentes"),
    "cat_new": perfil_metrics("MICRO CATORCENAL", "Nuevos"),
}
print(f"✅ perfiles: construidos para 4 segmentos x 5 perfiles")

# ============================================================================
# BLOQUE agencias + piloto
# ============================================================================
# Cartera mes anterior por agencia
cart_prev = (stock_prev
    .groupBy("agencia")
    .agg(F.round(F.sum("saldo"), 2).alias("cart_prev"))
    .toPandas().set_index("agencia")["cart_prev"].to_dict())

# Cartera actual desglosada por producto
cart_ag = (stock
    .groupBy("agencia")
    .agg(
        F.round(F.sum("saldo"), 2).alias("cart_jul"),
        F.round(F.sum(F.when(F.col("productoagrupado") == "MICRO SEMANAL",    F.col("saldo"))), 2).alias("cart_sem"),
        F.round(F.sum(F.when(F.col("productoagrupado") == "MICRO CATORCENAL", F.col("saldo"))), 2).alias("cart_cat"),
        F.round(F.sum(F.when(~F.col("productoagrupado").isin(PRODUCTOS),       F.col("saldo"))), 2).alias("cart_men"),
    ).toPandas().set_index("agencia"))

# Desembolsos semanal por agencia
desem_sem = (desem.filter(F.col("productoagrupado") == "MICRO SEMANAL")
    .groupBy("agencia", "zona")
    .agg(
        F.round(F.sum("montodesembolso"), 2).alias("monto_sem"),
        F.countDistinct("IdContrato").alias("ndesemb_sem"),
        norm_expr().alias("sem_norm_mes"),
        par8d_expr().alias("sem_par_desem"),
    ).toPandas().set_index("agencia"))

# PAR/Norm stock semanal
stock_sem = (stock.filter(F.col("productoagrupado") == "MICRO SEMANAL")
    .groupBy("agencia")
    .agg(par8d_expr().alias("sem_par_stock"), norm_expr().alias("sem_norm_stock"))
    .toPandas().set_index("agencia"))

# Desembolsos catorcenal por agencia
desem_cat = (desem.filter(F.col("productoagrupado") == "MICRO CATORCENAL")
    .groupBy("agencia")
    .agg(
        F.round(F.sum("montodesembolso"), 2).alias("monto_cat"),
        F.countDistinct("IdContrato").alias("ndesemb_cat"),
        norm_expr().alias("cat_norm_mes"),
    ).toPandas().set_index("agencia"))

# PAR/Norm stock catorcenal
stock_cat = (stock.filter(F.col("productoagrupado") == "MICRO CATORCENAL")
    .groupBy("agencia")
    .agg(par8d_expr().alias("cat_par_stock"), norm_expr().alias("cat_norm_stock"))
    .toPandas().set_index("agencia"))

# Zona por agencia
zonas = (stock.select("agencia","zona").distinct()
    .toPandas().set_index("agencia")["zona"].to_dict())

# Construir lista de agencias
agencias_list = []
for ag in sorted(zonas.keys()):
    z  = zonas.get(ag, "")
    cj = _f(cart_ag.loc[ag, "cart_jul"])  if ag in cart_ag.index else 0.0
    cs = _f(cart_ag.loc[ag, "cart_sem"])  if ag in cart_ag.index else 0.0
    cc = _f(cart_ag.loc[ag, "cart_cat"])  if ag in cart_ag.index else 0.0
    cm = _f(cart_ag.loc[ag, "cart_men"])  if ag in cart_ag.index else 0.0
    row = {
        "zona":           z,
        "agencia":        ag,
        "cart_jun":       _f(cart_prev.get(ag, 0.0)),
        "cart_jul":       cj or 0.0,
        "cart_sem":       cs or 0.0,
        "cart_cat":       cc or 0.0,
        "cart_men":       cm or 0.0,
        "monto_sem":      _f(desem_sem.loc[ag, "monto_sem"])    if ag in desem_sem.index else 0.0,
        "ndesemb_sem":    _i(desem_sem.loc[ag, "ndesemb_sem"])  if ag in desem_sem.index else 0,
        "sem_norm_mes":   _f(desem_sem.loc[ag, "sem_norm_mes"]) if ag in desem_sem.index else 0.0,
        "sem_par_stock":  _f(stock_sem.loc[ag, "sem_par_stock"])  if ag in stock_sem.index else 0.0,
        "sem_norm_stock": _f(stock_sem.loc[ag, "sem_norm_stock"]) if ag in stock_sem.index else 0.0,
        "monto_cat":      _f(desem_cat.loc[ag, "monto_cat"])    if ag in desem_cat.index else 0.0,
        "ndesemb_cat":    _i(desem_cat.loc[ag, "ndesemb_cat"])  if ag in desem_cat.index else 0,
        "cat_norm_mes":   _f(desem_cat.loc[ag, "cat_norm_mes"]) if ag in desem_cat.index else 0.0,
        "cat_par_stock":  _f(stock_cat.loc[ag, "cat_par_stock"])  if ag in stock_cat.index else 0.0,
        "cat_norm_stock": _f(stock_cat.loc[ag, "cat_norm_stock"]) if ag in stock_cat.index else 0.0,
    }
    agencias_list.append(row)

D["agencias"] = agencias_list
D["piloto"]   = [r for r in agencias_list if r["agencia"] in AGENCIAS_PILOTO]
print(f"✅ agencias: {len(D['agencias'])} agencias | piloto: {len(D['piloto'])}")

# ============================================================================
# BLOQUE nuevos + nuevos_resumen
# ============================================================================
nuevos_sem = (desem.filter(F.col("productoagrupado") == "MICRO SEMANAL")
    .groupBy("agencia", "zona")
    .agg(
        F.countDistinct(F.when(F.col("ciclo") == 0, F.col("IdCliente"))).alias("sem_sin"),
        F.countDistinct(F.when(F.col("ciclo") == 1, F.col("IdCliente"))).alias("sem_con"),
    ).toPandas().set_index("agencia"))

nuevos_cat = (desem.filter(F.col("productoagrupado") == "MICRO CATORCENAL")
    .groupBy("agencia")
    .agg(
        F.countDistinct(F.when(F.col("ciclo") == 0, F.col("IdCliente"))).alias("cat_sin"),
        F.countDistinct(F.when(F.col("ciclo") == 1, F.col("IdCliente"))).alias("cat_con"),
    ).toPandas().set_index("agencia"))

# Mensual: consulta directa desde det (no desde desem que filtra por PRODUCTOS sem+cat)
# Solo aparece en la hoja de nuevos — no afecta ningún otro bloque
_desem_men = (spark.table("LKH_Operaciones.dbo.analisisAF_Cartera_Detalle")
    .filter(F.col("IdEmpresa") == 8)
    .filter(F.col("productoagrupado") == "MICRO MENSUAL")
    .filter(F.col("Periodo") == PERIODO)
    .filter(F.col("maduracion_meses") == 0)
    .filter(F.col("fecha_desembolso").isNotNull())
    .filter(F.col("montodesembolso").isNotNull()))

nuevos_men = (_desem_men
    .groupBy("agencia")
    .agg(
        F.countDistinct(F.when(F.col("ciclo") == 0, F.col("IdCliente"))).alias("men_sin"),
        F.countDistinct(F.when(F.col("ciclo") == 1, F.col("IdCliente"))).alias("men_con"),
    ).toPandas().set_index("agencia"))

def estado_nuevos(tot, meta):
    if tot == 0:    return "EN 0"
    if tot >= meta: return "OK"
    return "AVANCE"

nuevos_list = []
for ag in sorted(zonas.keys()):
    z       = zonas.get(ag, "")
    ss      = _i(nuevos_sem.loc[ag, "sem_sin"]) if ag in nuevos_sem.index else 0
    sc      = _i(nuevos_sem.loc[ag, "sem_con"]) if ag in nuevos_sem.index else 0
    cs      = _i(nuevos_cat.loc[ag, "cat_sin"]) if ag in nuevos_cat.index else 0
    cc_val  = _i(nuevos_cat.loc[ag, "cat_con"]) if ag in nuevos_cat.index else 0
    ms      = _i(nuevos_men.loc[ag, "men_sin"]) if ag in nuevos_men.index else 0
    mc      = _i(nuevos_men.loc[ag, "men_con"]) if ag in nuevos_men.index else 0
    st      = ss + sc
    ct      = cs + cc_val
    mt      = ms + mc
    nuevos_list.append({
        "zona":       z, "agencia":   ag,
        "sem_sin":    ss, "sem_con":  sc, "sem_tot":    st,
        "cat_sin":    cs, "cat_con":  cc_val, "cat_tot": ct,
        "men_sin":    ms, "men_con":  mc, "men_tot":    mt,
        "sem_estado": estado_nuevos(st, META_NUEVOS_SEM),
        "cat_estado": estado_nuevos(ct, META_NUEVOS_CAT),
        "men_estado": estado_nuevos(mt, META_NUEVOS_MEN),
    })

D["nuevos"] = nuevos_list
D["nuevos_resumen"] = {
    "meta_sem": META_NUEVOS_SEM,
    "meta_cat": META_NUEVOS_CAT,
    "meta_men": META_NUEVOS_MEN,
    "sem_ok":   sum(1 for r in nuevos_list if r["sem_estado"] == "OK"),
    "sem_av":   sum(1 for r in nuevos_list if r["sem_estado"] == "AVANCE"),
    "sem_0":    sum(1 for r in nuevos_list if r["sem_estado"] == "EN 0"),
    "cat_ok":   sum(1 for r in nuevos_list if r["cat_estado"] == "OK"),
    "cat_av":   sum(1 for r in nuevos_list if r["cat_estado"] == "AVANCE"),
    "cat_0":    sum(1 for r in nuevos_list if r["cat_estado"] == "EN 0"),
    "men_ok":   sum(1 for r in nuevos_list if r["men_estado"] == "OK"),
    "men_av":   sum(1 for r in nuevos_list if r["men_estado"] == "AVANCE"),
    "men_0":    sum(1 for r in nuevos_list if r["men_estado"] == "EN 0"),
}
print(f"✅ nuevos: sem OK={D['nuevos_resumen']['sem_ok']} AV={D['nuevos_resumen']['sem_av']} 0={D['nuevos_resumen']['sem_0']}")

# ============================================================================
# BLOQUE efectividad + react + efe_resumen
# ============================================================================
efe_ag = (tasa
    .groupBy("agencia", "zona")
    .agg(
        F.sum("base_asignada").alias("base"),
        F.sum("recuperados").alias("recup"),
    ).toPandas())

# Marcar agencias críticas basado en PAR8d del stock semanal
efe_list = []
for _, row in efe_ag.iterrows():
    ag    = row["agencia"]
    base  = _i(row["base"])
    recup = _i(row["recup"])
    efect = round(recup / base, 6) if base > 0 else 0.0
    critica = "Sí" if ag in AGENCIAS_CRITICAS else ""
    efe_list.append({
        "agencia": ag,
        "zona":    row["zona"],
        "critica": critica,
        "base":    base,
        "recup":   recup,
        "efect":   efect,
    })

# Ordenar por agencia
efe_list.sort(key=lambda x: x["agencia"])

base_total  = sum(r["base"]  for r in efe_list)
recup_total = sum(r["recup"] for r in efe_list)
nag_total   = tasa.select("agencia").distinct().count()
sin_venta   = sum(1 for r in efe_list if r["recup"] == 0)

D["efectividad"] = efe_list
D["react"] = {
    "base":  base_total,
    "recup": recup_total,
    "sin":   sin_venta,
    "nag":   nag_total,
    "efect": round(recup_total / base_total, 6) if base_total > 0 else 0.0,
}
D["efe_resumen"] = {
    "base":      base_total,
    "recup":     recup_total,
    "efect":     round(recup_total / base_total, 6) if base_total > 0 else 0.0,
    "sin_venta": sin_venta,
}
print(f"✅ efectividad: {len(efe_list)} agencias | base={base_total} | recup={recup_total}")

# ============================================================================
# BLOQUE top200
# ============================================================================
top200_df = (stock
    .filter(F.col("dias_atraso") > 0)
    .filter(F.col("productoagrupado").isin(PRODUCTOS))
    .orderBy(F.col("saldo").desc())
    .limit(200)
    .select(
        "ncodcred", "ncodage", "fecha_desembolso", "IdCliente", "zona", "agencia",
        "usuario", "productoagrupado", "P_Propuesto",
        "montodesembolso", "saldo", "monto_cuota",
        "ciclo", "TipoCliente", "dias_atraso", "rango_atraso",
    ).toPandas())

def _fmt_codigo(row):
    """Formato: ncodcred-ncodage  ej: 8750-22"""
    try:    return f"{int(row['ncodcred'])}-{int(row['ncodage'])}"
    except: return str(row.get("ncodcred", ""))

D["top200"] = [
    {
        "codigo":   _fmt_codigo(r),
        "fecha":    str(r.get("fecha_desembolso", ""))[:10].replace("-", "/"),
        "cliente":  _i(r.get("IdCliente")),
        "zona":     r.get("zona", ""),
        "agencia":  r.get("agencia", ""),
        "asesor":   r.get("usuario", "") or "",
        "producto": "Semanal" if r.get("productoagrupado") == "MICRO SEMANAL" else "Catorcenal",
        "perfil":   r.get("P_Propuesto"),
        "monto":    _f(r.get("montodesembolso")),
        "saldo":    _f(r.get("saldo")),
        "cuota":    _f(r.get("monto_cuota")),
        "ciclo":    _i(r.get("ciclo")),
        "tipo":     r.get("TipoCliente", ""),
        "dias":     _i(r.get("dias_atraso")),
        "rango":    r.get("rango_atraso", ""),
        "critica":  "Sí" if r.get("agencia") in AGENCIAS_CRITICAS else "No",
    }
    for _, r in top200_df.iterrows()
]
print(f"✅ top200: {len(D['top200'])} operaciones con atraso")

# ============================================================================
# INYECTAR D EN LA PLANTILLA Y ESCRIBIR HTML
# ============================================================================
# ── Leer plantilla ────────────────────────────────────────────────────────────
# Busca en este orden:
#   1. Files/reportes_af/plantilla_tablero.html en el Lakehouse (preferida)
#   2. El HTML generado más reciente en el mismo directorio (fallback)
import glob, re

_rutas_candidatas = [
    "/lakehouse/default/Files/reportes_af/plantilla_tablero.html",
]
# Fallback: el HTML más reciente ya generado en el directorio de salida
_htmls_existentes = sorted(glob.glob(f"{OUTPUT_DIR}Tablero_AFGT_*.html"), reverse=True)
if _htmls_existentes:
    _rutas_candidatas.append(_htmls_existentes[0])

plantilla_path = None
for ruta_candidata in _rutas_candidatas:
    if os.path.exists(ruta_candidata):
        plantilla_path = ruta_candidata
        break

if plantilla_path is None:
    raise FileNotFoundError(
        "No se encontró plantilla. "
        "Verifica que 'plantilla_tablero.html' esté en Files/reportes_af/ del Lakehouse."
    )

print(f"  Plantilla: {plantilla_path}")
with open(plantilla_path, "r", encoding="utf-8") as f:
    plantilla = f.read()

# ============================================================================
# BLOQUE evol — Gráficos evolutivos diarios (desembolso acumulado + PAR8d)
# ============================================================================
# Fuente: analisisAF_PAR8d_Diario (query 4 de 00_parametros.py)
# Desembolsos diarios: analisisAF_Cartera_Detalle filtrando maduracion_meses=0
# ─────────────────────────────────────────────────────────────────────────────

par8d_raw = spark.table("LKH_Operaciones.dbo.analisisAF_PAR8d_Diario").toPandas()

# ── Desembolso acumulado diario por producto ──────────────────────────────────
# Mes actual: usar desem que ya está filtrado por periodo y maduracion=0
desem_diario = (desem
    .withColumn("dia_mes", (F.col("fecha_desembolso") % 100).cast("int"))
    .groupBy("dia_mes", "productoagrupado")
    .agg(F.round(F.sum("montodesembolso"), 2).alias("monto"))
    .orderBy("dia_mes")
    .toPandas())

# Mes anterior: mismo filtro sobre stock_prev
desem_diario_prev = (stock_prev
    .filter(F.col("maduracion_meses") == 0)
    .filter(F.col("fecha_desembolso").isNotNull())
    .filter(F.col("montodesembolso").isNotNull())
    .withColumn("dia_mes", (F.col("fecha_desembolso") % 100).cast("int"))
    .groupBy("dia_mes", "productoagrupado")
    .agg(F.round(F.sum("montodesembolso"), 2).alias("monto"))
    .orderBy("dia_mes")
    .toPandas())

def acum_por_dia(df, prod, max_dia=31):
    """Desembolso acumulado día a día para un producto."""
    sub = df[df["productoagrupado"] == prod].copy()
    sub = sub.groupby("dia_mes")["monto"].sum().reset_index()
    # Rellenar días sin desembolso
    dias = list(range(1, max_dia + 1))
    mapa = {k: float(v) for k, v in zip(sub["dia_mes"], sub["monto"])}
    acum, total = [], 0.0
    for d in dias:
        total += mapa.get(d, 0.0)
        acum.append(round(total, 2))
    return acum

import calendar as _cal
from datetime import date as _date

# ── Parámetros del mes ────────────────────────────────────────────────────────
_anio    = int(PERIODO[:4])
_mes     = int(PERIODO[4:])
MAX_DIA  = int(FECHA_CORTE[6:8])          # día del corte (hasta aquí llegan los datos)
DIAS_MES = _cal.monthrange(_anio, _mes)[1] # días totales del mes (28/30/31)
DIAS     = list(range(1, MAX_DIA + 1))     # eje X: días con datos

# ── PPTO hábil: lunes a sábado (domingos no tienen gestión) ──────────────────
_habiles_mes = [d for d in range(1, DIAS_MES + 1)
                if _date(_anio, _mes, d).weekday() != 6]  # 6 = domingo
_n_habiles   = len(_habiles_mes)

def ppto_acum_habil(ppto_total, dias_eje):
    """Curva acumulada de PPTO distribuido en días hábiles (lun-sab)."""
    ppto_dia = ppto_total / _n_habiles if _n_habiles > 0 else 0.0
    acum, habs = [], 0
    for d in dias_eje:
        if _date(_anio, _mes, d).weekday() != 6:
            habs += 1
        acum.append(round(ppto_dia * habs, 2))
    return acum

# Mes anterior: usar todos sus días (mes cerrado → curva completa)
_anio_prev = int(PERIODO_PREV[:4])
_mes_prev  = int(PERIODO_PREV[4:])
MAX_DIA_PREV = _cal.monthrange(_anio_prev, _mes_prev)[1]

evol_sem_act  = acum_por_dia(desem_diario,      "MICRO SEMANAL",    MAX_DIA)[:MAX_DIA]
evol_cat_act  = acum_por_dia(desem_diario,      "MICRO CATORCENAL", MAX_DIA)[:MAX_DIA]
evol_sem_prev = acum_por_dia(desem_diario_prev, "MICRO SEMANAL",    MAX_DIA_PREV)[:MAX_DIA_PREV]
evol_cat_prev = acum_por_dia(desem_diario_prev, "MICRO CATORCENAL", MAX_DIA_PREV)[:MAX_DIA_PREV]
DIAS_PREV     = list(range(1, MAX_DIA_PREV + 1))

# PPTO acumulado hábil (días hábiles del mes completo para proyección)
DIAS_FULL     = list(range(1, DIAS_MES + 1))
ppto_sem_acum = ppto_acum_habil(PPTO_SEM, DIAS_FULL)
ppto_cat_acum = ppto_acum_habil(PPTO_CAT, DIAS_FULL)

# ── PAR8d diario por perfil ───────────────────────────────────────────────────
PERFILES_EVOL = ["P1", "P2", "P3", "P4", "P5"]

def par8d_serie(df, prod, perfiles, max_dia):
    """PAR8d diario por perfil para un producto."""
    prod_upper = prod.upper()
    sub = df[df["Producto"].str.upper() == prod_upper].copy()
    sub["dia"] = sub["Fecha"].astype(str).str[-2:].astype(int)
    sub = sub[sub["dia"] <= max_dia]
    result = {}
    for p in perfiles:
        sp = sub[sub["Perfil"] == p].sort_values("dia")
        mapa = dict(zip(sp["dia"], sp["PAR8d"]))
        serie = [round(float(mapa[d]), 6) if d in mapa and mapa[d] is not None else None
                 for d in range(1, max_dia + 1)]
        result[p] = serie
    return result

par8d_sem = par8d_serie(par8d_raw, "MICRO SEMANAL",    PERFILES_EVOL, MAX_DIA)
par8d_cat = par8d_serie(par8d_raw, "MICRO CATORCENAL", PERFILES_EVOL, MAX_DIA)

D["evol"] = {
    "dias":          DIAS,        # días con datos reales mes actual (1 a MAX_DIA)
    "dias_full":     DIAS_FULL,   # días completos mes actual (1 a DIAS_MES) — para PPTO
    "dias_prev":     DIAS_PREV,   # días completos mes anterior (mes cerrado)
    "ppto_sem":      ppto_sem_acum,
    "ppto_cat":      ppto_cat_acum,
    "sem_act":       evol_sem_act,
    "sem_prev":      evol_sem_prev,
    "cat_act":       evol_cat_act,
    "cat_prev":      evol_cat_prev,
    "par8d_sem":     par8d_sem,
    "par8d_cat":     par8d_cat,
    "label_prev":    f"{PERIODO_PREV[:4]}-{PERIODO_PREV[4:]}",
    "label_act":     f"{PERIODO[:4]}-{PERIODO[4:]}",
}
print(f"✅ evol: {MAX_DIA} días | sem_act acum={evol_sem_act[-1]:,.0f} | cat_act acum={evol_cat_act[-1]:,.0f}")

# ── Limpiar SVGs prerendereados (evita superposición con datos del día anterior)
_SVG_LIMPIAR = ['c-efzona', 'c-eftop', 'c-efzero', 'c-perfil', 'c-topperfil', 'c-topag']
plantilla_limpia = plantilla
for _sid in _SVG_LIMPIAR:
    plantilla_limpia = re.sub(
        rf'(<svg[^>]*\bid="{_sid}"[^>]*>).*?(</svg>)',
        r'\1\2', plantilla_limpia, flags=re.DOTALL
    )
print(f"  SVGs limpiados: {_SVG_LIMPIAR}")

# ── Inyectar D en la plantilla ───────────────────────────────────────────────
data_json = json.dumps(D, ensure_ascii=False, default=float)

# Diagnóstico: ver cómo empieza el bloque D en la plantilla
import re
_muestra = re.search(r'const D.{0,200}', plantilla_limpia)
if _muestra:
    print(f"  Bloque D encontrado en plantilla: '{_muestra.group()[:100]}...'")
else:
    print("  ⚠️ No se encontró 'const D' en la plantilla")

# Intentar varios patrones en orden de especificidad
_patrones = [
    r'const D = \{.*?\};',          # original: exacto con punto y coma
    r'const D=\{.*?\};',            # sin espacios
    r'const D\s*=\s*\{.*?\};',      # con espacios variables
    r'const D\s*=\s*\{.*?\}(?=\s*\n|;|\s*<)', # sin punto y coma obligatorio
]

html_nuevo = None
for patron in _patrones:
    resultado = re.sub(patron, f'const D = {data_json};', plantilla_limpia, flags=re.DOTALL)
    if resultado != plantilla_limpia:      # si hubo cambio, el patrón funcionó
        html_nuevo = resultado
        print(f"  Patrón que funcionó: {patron}")
        break

if html_nuevo is None:
    # Fallback: buscar el índice de "const D" y reemplazar hasta el cierre del objeto
    idx_inicio = plantilla_limpia.find('const D')
    if idx_inicio == -1:
        raise ValueError("No se encontró 'const D' en la plantilla. Verificar el HTML.")
    # Encontrar el cierre del objeto contando llaves
    idx = plantilla.index('{', idx_inicio)
    nivel = 0
    for i, c in enumerate(plantilla[idx:], idx):
        if c == '{': nivel += 1
        elif c == '}':
            nivel -= 1
            if nivel == 0:
                idx_fin = i + 1
                break
    # Incluir el ; si viene después
    if plantilla[idx_fin:idx_fin+1] == ';':
        idx_fin += 1
    html_nuevo = plantilla_limpia[:idx_inicio] + f'const D = {data_json};' + plantilla_limpia[idx_fin:]
    print(f"  Reemplazo via conteo de llaves (fallback)")

print(f"  Tamaño plantilla original: {len(plantilla):,} bytes")
print(f"  Tamaño HTML nuevo:         {len(html_nuevo):,} bytes")

# ── Patch: inyectar gráficos evolutivos en la sección resumen ────────────────
# Se insertan después del último </div> del segundo bloque de KPIs en #resumen
_GRAFICOS_HTML = """
  <div class="subttl" style="margin-top:24px;">Seguimiento diario de desembolso y calidad de cartera</div>
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px;">
    <div class="chartbox">
      <div class="ct">Seguimiento desembolso diario \u00b7 Semanal</div>
      <div class="cs">Monto acumulado en quetzales</div>
      <svg id="ev-sem" viewBox="0 0 480 200" width="100%" style="display:block"></svg>
    </div>
    <div class="chartbox">
      <div class="ct">Seguimiento PAR8d diario \u00b7 Semanal</div>
      <div class="cs">PAR8d sobre cartera vigente por perfil</div>
      <svg id="ev-par-sem" viewBox="0 0 480 200" width="100%" style="display:block"></svg>
    </div>
    <div class="chartbox">
      <div class="ct">Seguimiento desembolso diario \u00b7 Catorcenal</div>
      <div class="cs">Monto acumulado en quetzales</div>
      <svg id="ev-cat" viewBox="0 0 480 200" width="100%" style="display:block"></svg>
    </div>
    <div class="chartbox">
      <div class="ct">Seguimiento PAR8d diario \u00b7 Catorcenal (Recurrentes)</div>
      <div class="cs">PAR8d sobre cartera vigente por perfil</div>
      <svg id="ev-par-cat" viewBox="0 0 480 200" width="100%" style="display:block"></svg>
    </div>
  </div>
  <script>
  window.addEventListener('load', function(){
    const NS='http://www.w3.org/2000/svg';
    function svgEl(tag,attrs){
      const e=document.createElementNS(NS,tag);
      for(const k in attrs) e.setAttribute(k,attrs[k]);
      return e;
    }
    function txt(content,attrs){
      const e=svgEl('text',attrs);
      e.textContent=content;
      return e;
    }

    // Layout constants
    const W=480, H=200, PAD={t:16,r:16,b:40,l:56};
    const IW=W-PAD.l-PAD.r, IH=H-PAD.t-PAD.b;

    // diasX = eje X del mes completo (para escalar correctamente)
    function lineChart(svgId, series, yFmt, yMax){
      const svg=document.getElementById(svgId);
      if(!svg) return;
      const diasX=D.evol.dias_full;   // eje completo del mes (1..31)
      const N=diasX.length;
      if(N===0) return;

      // Compute max sobre todos los datos de todas las series
      let mx = yMax || 0;
      series.forEach(s=>s.data.forEach(v=>{ if(v!=null && v>mx) mx=v; }));
      if(mx===0) mx=1;

      // Grid lines (4 horizontal)
      for(let g=0;g<=4;g++){
        const y=PAD.t+IH*(1-g/4);
        svg.appendChild(svgEl('line',{x1:PAD.l,y1:y,x2:W-PAD.r,y2:y,
          stroke:'#eef2f6','stroke-width':1}));
        svg.appendChild(txt(yFmt(mx*g/4),{x:PAD.l-4,y:y+4,
          'font-size':9,fill:'#7a8b9a','text-anchor':'end'}));
      }

      // X axis
      svg.appendChild(svgEl('line',{x1:PAD.l,y1:PAD.t+IH,x2:W-PAD.r,y2:PAD.t+IH,
        stroke:'#d5dde5','stroke-width':1}));

      // X labels: dias del mes completo, cada 5 días + último
      diasX.forEach((d,i)=>{
        if(d===1 || d%5===0 || d===diasX[diasX.length-1]){
          const x=PAD.l+i*IW/(Math.max(N-1,1));
          svg.appendChild(txt(d,{x,y:H-PAD.b+12,'font-size':9,
            fill:'#7a8b9a','text-anchor':'middle'}));
        }
      });

      // Series: cada serie tiene su propio eje X (dias o dias_full)
      series.forEach(s=>{
        const diasS = s.diasRef || D.evol.dias;  // dias reales o completos
        const nS    = diasS.length;
        const pts=s.data.map((v,i)=>{
          if(v==null || i>=nS) return null;
          // Mapear día real al eje X completo del mes
          const diaReal = diasS[i];
          const xi = diasX.indexOf(diaReal);
          if(xi<0) return null;
          const x=PAD.l+xi*IW/(Math.max(N-1,1));
          const y=PAD.t+IH*(1-v/mx);
          return [x,y];
        }).filter(p=>p!==null);
        if(pts.length<2) return;

        const d=pts.map((p,i)=>(i===0?'M':'L')+p[0].toFixed(1)+','+p[1].toFixed(1)).join(' ');
        const path=svgEl('path',{d,'fill':'none',stroke:s.color,
          'stroke-width':s.width||2,'stroke-dasharray':s.dash||''});
        svg.appendChild(path);
      });

      // Legend
      let lx=PAD.l;
      series.forEach(s=>{
        svg.appendChild(svgEl('line',{x1:lx,y1:H-10,x2:lx+18,y2:H-10,
          stroke:s.color,'stroke-width':2,'stroke-dasharray':s.dash||''}));
        svg.appendChild(txt(s.label,{x:lx+22,y:H-6,'font-size':9,fill:'#333'}));
        lx+=s.label.length*6+30;
      });
    }

    const ev=D.evol;
    const fmtM=v=>v>=1e6?'Q'+(v/1e6).toFixed(1)+'M':v>=1e3?'Q'+(v/1e3).toFixed(0)+'K':'Q'+v.toFixed(0);
    const fmtP=v=>v==null?'':(v*100).toFixed(1)+'%';
    const CP={P1:'#2e7d32',P2:'#558b2f',P3:'#e6ac00',P4:'#e65100',P5:'#b71c1c'};

    // Desembolso Semanal — prev usa mes anterior completo, act hasta corte, PPTO mes completo
    lineChart('ev-sem',[
      {label:ev.label_prev+' acum', data:ev.sem_prev, color:'#e53935', dash:'4,3', diasRef:ev.dias_prev},
      {label:ev.label_act+' acum',  data:ev.sem_act,  color:'#43a047',             diasRef:ev.dias},
      {label:'PPTO Acum',           data:ev.ppto_sem, color:'#1565c0', dash:'6,3', width:1.5, diasRef:ev.dias_full}
    ], fmtM);

    // PAR8d Semanal
    lineChart('ev-par-sem',
      Object.entries(ev.par8d_sem).map(([p,d])=>({label:p,data:d,color:CP[p],diasRef:ev.dias})),
      fmtP, 1.0);

    // Desembolso Catorcenal
    lineChart('ev-cat',[
      {label:ev.label_prev+' acum', data:ev.cat_prev, color:'#e53935', dash:'4,3', diasRef:ev.dias_prev},
      {label:ev.label_act+' acum',  data:ev.cat_act,  color:'#43a047',             diasRef:ev.dias},
      {label:'PPTO Acum',           data:ev.ppto_cat, color:'#1565c0', dash:'6,3', width:1.5, diasRef:ev.dias_full}
    ], fmtM);

    // PAR8d Catorcenal
    lineChart('ev-par-cat',
      Object.entries(ev.par8d_cat).map(([p,d])=>({label:p,data:d,color:CP[p],diasRef:ev.dias})),
      fmtP, 1.0);
  });
  </script>
"""

# Insertar los gráficos al final de la sección #resumen (antes del cierre </section>)
# Insertar gráficos antes de la sección perfiles
# ── Patch tabla nuevos: agregar columna Mensual ──────────────────────────────
# Extiende encabezado, filas y footer de la tabla de nuevos para mostrar mensual.
# Opera sobre html_nuevo ya generado — no toca la plantilla.

# 1. Encabezado: agregar grupo Mensual junto a Catorcenal
html_nuevo = html_nuevo.replace(
    '<th colspan="4" class="grp sep">Catorcenal (meta 2)</th></tr>',
    '<th colspan="4" class="grp sep">Catorcenal (meta 2)</th>'
    '<th colspan="4" class="grp sep">Mensual (meta 2)</th></tr>',
    1
)
html_nuevo = html_nuevo.replace(
    '<th class="srt num sep" data-k="cat_sin">Sin historial</th>'
    '<th class="srt num" data-k="cat_con">Con historial</th>'
    '<th class="srt num" data-k="cat_tot">Total</th>'
    '<th class="srt" data-k="cat_estado">Estado</th></tr>',
    '<th class="srt num sep" data-k="cat_sin">Sin historial</th>'
    '<th class="srt num" data-k="cat_con">Con historial</th>'
    '<th class="srt num" data-k="cat_tot">Total</th>'
    '<th class="srt" data-k="cat_estado">Estado</th>'
    '<th class="srt num sep" data-k="men_sin">Sin historial</th>'
    '<th class="srt num" data-k="men_con">Con historial</th>'
    '<th class="srt num" data-k="men_tot">Total</th>'
    '<th class="srt" data-k="men_estado">Estado</th></tr>',
    1
)

# 2. Función render(): inyectar celdas mensual después de las de catorcenal
# El JS original termina la fila con: ...estadoBadge(n.cat_estado)+'</td></tr>';}
html_nuevo = html_nuevo.replace(
    "estadoBadge(n.cat_estado)+'</td></tr>';}",
    "estadoBadge(n.cat_estado)+'</td>'+\n"
    "    '<td class=\"num sep\" data-l=\"Mensual sin historial\">'+(n.men_sin||0)+'</td>"
    "<td class=\"num\" data-l=\"Mensual con historial\">'+(n.men_con||0)+'</td>"
    "<td class=\"num\" data-l=\"Mensual total\">'+(n.men_tot||0)+'</td>"
    "<td data-l=\"Mensual estado\">'+estadoBadge(n.men_estado||'EN 0')+'</td></tr>';}",
    1
)

# 3. Footer: agregar totales mensual
# El footer original termina con: ...+ct+'</td><td></td></tr>'
html_nuevo = html_nuevo.replace(
    "ct+'</td><td></td></tr>'",
    "ct+'</td><td></td>"
    "<td class=\"num sep\">'+rr.reduce(function(a,n){return a+(n.men_sin||0);},0)+'</td>"
    "<td class=\"num\">'+rr.reduce(function(a,n){return a+(n.men_con||0);},0)+'</td>"
    "<td class=\"num\">'+rr.reduce(function(a,n){return a+(n.men_tot||0);},0)+'</td>"
    "<td></td></tr>'",
    1
)

_patch_men_ok = 'men_sin' in html_nuevo
print(f"  Patch nuevos+mensual: {'✅ OK' if _patch_men_ok else '⚠️ no aplicó'}")

# Insertar gráficos DENTRO de #resumen, justo antes de su </section># Sin limpieza de bloque evolutivo — la plantilla siempre es el HTML original sin gráficos.

# Insertar gráficos DENTRO de #resumen, justo antes de su </section>
# Buscamos el </section> que cierra #resumen (antes de <section id="perfiles">)
_m_resumen = re.search(r'(<section id="resumen".*?)(</section>)(\s*<section id="perfiles")',
                        html_nuevo, re.DOTALL)
if _m_resumen:
    html_nuevo = (html_nuevo[:_m_resumen.start(2)]
                  + _GRAFICOS_HTML
                  + html_nuevo[_m_resumen.start(2):])
    _insertar_ok = True
else:
    _insertar_ok = False
print(f"  Gráficos evolutivos inyectados: {'OK' if _insertar_ok else '⚠️ separador no encontrado'}")

# ── Patch KPIs hardcodeados ──────────────────────────────────────────────────
# Los <div class="val"> del bloque .kpis no tienen IDs — el JS no los actualiza.
# Los sobreescribimos directamente desde D.kpi antes de guardar.
def _fmt_q(v):
    return f"Q{v:,.2f}" if v is not None else "—"
def _fmt_pct(v):
    return f"{v:.2%}" if v is not None else "—"

_kpi = D["kpi"]
_efe = D["efe_resumen"]

_kpi_replacements = [
    # (label exacto en HTML,                    valor nuevo)
    ("Contratos totales",   str(_kpi["contratos"])),
    ("Presupuesto",         _fmt_q(_kpi["presupuesto"])),
    ("Monto desembolsado",  _fmt_q(_kpi["desembolsado"])),
    ("% Avance",            _fmt_pct(_kpi["avance"])),
    ("Avance Semanal",      _fmt_pct(_kpi["avance_sem"])),
    ("Avance Catorcenal",   _fmt_pct(_kpi["avance_cat"])),
    ("Reactivación desertores",
     f"{_efe['recup']:,} / {_efe['base']:,} · {_efe['efect']:.2%}"),
]

for lbl, val in _kpi_replacements:
    # Reemplaza <div class="lbl">LABEL</div><div class="val">CUALQUIER_VALOR</div>
    html_nuevo = re.sub(
        rf'(<div class="lbl">{re.escape(lbl)}</div><div class="val">)[^<]*(</div>)',
        rf'\g<1>{val}\g<2>',
        html_nuevo, count=1
    )

print(f"  KPIs actualizados: contratos={_kpi['contratos']} desembolsado={_fmt_q(_kpi['desembolsado'])} avance={_fmt_pct(_kpi['avance'])}")

# ── Patch fechas hardcodeadas en título y subtítulo ──────────────────────────
_MESES_ES  = ['Ene','Feb','Mar','Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic']
_fc        = str(FECHA_CORTE)
_titulo    = f"{_fc[6:8]}-{_MESES_ES[int(_fc[4:6])-1]}-{_fc[:4]}"  # "09-Jul-2026"
_subtitulo = f"{_fc[6:8]}/{_fc[4:6]}/{_fc[:4]}"                     # "09/07/2026"

# 1. Título: DD-Mes-YYYY (único en el HTML)
html_nuevo = re.sub(r'\d{2}-[A-Za-z]{3}-\d{4}', _titulo, html_nuevo, count=1)

# 2. Subtítulo: "Corte al DD/MM/YYYY" — reemplazar solo ese patrón contextual
html_nuevo = re.sub(r'Corte al \d{2}/\d{2}/\d{4}', f'Corte al {_subtitulo}', html_nuevo, count=1)

print(f"  Título:    {_titulo}")
print(f"  Subtítulo: Corte al {_subtitulo}")
os.makedirs(OUTPUT_DIR, exist_ok=True)
ruta = os.path.join(OUTPUT_DIR, OUTPUT_NAME)
with open(ruta, "w", encoding="utf-8") as f:
    f.write(html_nuevo)
# ============================================================================
# SOLUCIÓN SEGURA: Actualizar valores de KPIs en #efect con regex específico
# ============================================================================
# Reconstruye los 4 valores (Base, Reactivados, Efectividad, Agencias)
# SOLO dentro de <section id="efect"> usando su estructura HTML única
#
# Añade esto al FINAL del notebook 01_generar_html.py (después de escribir HTML)
# ============================================================================

import re, json

print("\n🔧 Solución segura: actualizando valores en #efect...")

# ── Leer HTML generado ────────────────────────────────────────────────
with open(ruta, 'r', encoding='utf-8') as f:
    html_content = f.read()

# ── Extraer D para obtener valores de efectividad ──────────────────────
d_match = re.search(r'const D = ({.*?});', html_content, re.DOTALL)
if not d_match:
    print("  ⚠️  No se encontró const D")
else:
    try:
        D = json.loads(d_match.group(1))
        react = D.get("react", {})
        
        base = react.get("base", 0)
        recup = react.get("recup", 0)
        efect = react.get("efect", 0.0)
        sin_venta = react.get("sin", 0)
        
        print(f"  ✅ Valores de D.react:")
        print(f"     Base: {base}, Recup: {recup}, Efect: {efect:.2%}, Sin: {sin_venta}")
        
        # ── Buscar la sección #efect COMPLETA ────────────────────────────
        # Extrae: <section id="efect">...</section>
        efect_match = re.search(
            r'<section id="efect"[^>]*>.*?</section>',
            html_content, re.DOTALL
        )
        
        if efect_match:
            efect_section = efect_match.group(0)
            efect_section_original = efect_section
            
            # ── Dentro de #efect, reemplazar los 4 valores específicos ──────
            # Patrón 1: Base asignada
            # Busca: <div class="lbl">Base asignada</div><div class="val">NUMERO</div>
            efect_section = re.sub(
                r'(<div class="lbl">Base asignada</div><div class="val">)[^<]*(?=</div>)',
                rf'\g<1>{base:,}',
                efect_section,
                count=1
            )
            
            # Patrón 2: Reactivados (con clase "green")
            # Busca: <div class="lbl">Reactivados</div><div class="val">NUMERO</div>
            efect_section = re.sub(
                r'(<div class="lbl">Reactivados</div><div class="val">)[^<]*(?=</div>)',
                rf'\g<1>{recup}',
                efect_section,
                count=1
            )
            
            # Patrón 3: Efectividad (con clase "red")
            # Busca: <div class="lbl">Efectividad</div><div class="val">PORCENTAJE</div>
            efect_section = re.sub(
                r'(<div class="lbl">Efectividad</div><div class="val">)[^<]*(?=</div>)',
                rf'\g<1>{efect:.2%}',
                efect_section,
                count=1
            )
            
            # Patrón 4: Agencias sin reactivación
            # Busca: <div class="lbl">Agencias sin reactivación</div><div class="val">NUMERO / 31</div>
            efect_section = re.sub(
                r'(<div class="lbl">Agencias sin reactivación</div><div class="val">)[^<]*(?=</div>)',
                rf'\g<1>{sin_venta} / 31',
                efect_section,
                count=1
            )
            
            # ── Reemplazar la sección #efect COMPLETA en html_content ──────
            html_content = html_content.replace(efect_section_original, efect_section, 1)
            
            print("  ✅ Valores reemplazados en #efect:")
            print(f"     - Base asignada: {base:,}")
            print(f"     - Reactivados: {recup}")
            print(f"     - Efectividad: {efect:.2%}")
            print(f"     - Agencias sin reactivación: {sin_venta} / 31")
        else:
            print("  ⚠️  No se encontró <section id='efect'>")
        
        # ── CSS de respaldo (por si acaso) ──────────────────────────────
        css_respaldo = """<style>
#efect .kpi .lbl {
  display: block !important;
  margin-bottom: 16px !important;
}
#efect .kpi .val {
  display: block !important;
  margin-top: 0 !important;
}
</style>"""
        
        if '#efect .kpi .lbl' not in html_content and '</head>' in html_content:
            html_content = html_content.replace('</head>', css_respaldo + '\n</head>', 1)
            print("  ✅ CSS de respaldo inyectado")
        
        # ── Guardar ──────────────────────────────────────────────────────
        with open(ruta, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"  ✅ HTML guardado: {ruta}")
        print(f"     Tamaño: {len(html_content):,} bytes")
        
    except json.JSONDecodeError as e:
        print(f"  ❌ Error parseando D: {e}")

# ============================================================================
# FIN
# ============================================================================

# ============================================================================
# PARCHE: Agregar KPIs de Mensual en la sección "Avance de nuevos"
# ============================================================================
# Añade 3 nuevos cuadros: Mensual · OK, Mensual · Avance, Mensual · En 0
#
# Añade esto al FINAL del notebook 01_generar_html.py (después de SOLUCION_SEGURA_VALORES.py)
# ============================================================================

import re, json

print("\n🔧 Agregando KPIs de Mensual en 'Avance de nuevos'...")

# ── Leer HTML generado ────────────────────────────────────────────────
with open(ruta, 'r', encoding='utf-8') as f:
    html_content = f.read()

# ── Extraer D para obtener conteos de Mensual ────────────────────────
d_match = re.search(r'const D = ({.*?});', html_content, re.DOTALL)
if not d_match:
    print("  ⚠️  No se encontró const D")
else:
    try:
        D = json.loads(d_match.group(1))
        nuevos_resumen = D.get("nuevos_resumen", {})
        
        # Valores para los 3 KPIs de Mensual
        men_ok = nuevos_resumen.get("men_ok", 0)
        men_av = nuevos_resumen.get("men_av", 0)
        men_0 = nuevos_resumen.get("men_0", 0)
        
        print(f"  ✅ Valores de Mensual extraídos:")
        print(f"     Mensual OK: {men_ok}")
        print(f"     Mensual Avance: {men_av}")
        print(f"     Mensual En 0: {men_0}")
        
        # ── Construir los 3 nuevos KPIs HTML ──────────────────────────────
        kpi_mensual_html = f'''<div class="kpi green"><div class="lbl">Mensual · OK</div><div class="val" id="nz-mok">{men_ok}</div></div>
    <div class="kpi"><div class="lbl">Mensual · Avance</div><div class="val" id="nz-mav">{men_av}</div></div>
    <div class="kpi red"><div class="lbl">Mensual · En 0</div><div class="val" id="nz-m0">{men_0}</div></div>'''
        
        # ── Buscar el bloque .kpis dentro de #nuevos ──────────────────────
        # Patrón: <section id="nuevos">...<div class="kpis">...</div>
        pattern = r'(<section[^>]*id="nuevos"[^>]*>.*?<div class="kpis">)(.*?)(</div>\s*<div class="controls">)'
        match = re.search(pattern, html_content, re.DOTALL)
        
        if match:
            # El contenido actual de .kpis está entre match.group(1) y match.group(3)
            kpis_content = match.group(2)
            
            # Verificar que no esté ya (para evitar duplicados)
            if 'nz-mok' not in kpis_content:
                # Agregar los KPIs de Mensual al final
                new_kpis_content = kpis_content + '\n    ' + kpi_mensual_html
                
                # Reemplazar
                html_content = (
                    html_content[:match.start(2)] +
                    new_kpis_content +
                    html_content[match.end(2):]
                )
                print("  ✅ KPIs de Mensual agregados a #nuevos")
            else:
                print("  ⓘ KPIs de Mensual ya existen, omitiendo")
        else:
            print("  ⚠️  No se encontró patrón de .kpis en #nuevos")
        
        # ── Guardar ──────────────────────────────────────────────────────
        with open(ruta, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"  ✅ HTML guardado: {ruta}")
        print(f"     Tamaño: {len(html_content):,} bytes")
        print("\n  📊 KPIs de Mensual finales:")
        print(f"     Mensual · OK: {men_ok}")
        print(f"     Mensual · Avance: {men_av}")
        print(f"     Mensual · En 0: {men_0}")
        
    except json.JSONDecodeError as e:
        print(f"  ❌ Error parseando D: {e}")

# ============================================================================
# FIN
# ============================================================================

# La plantilla NO se sobreescribe — siempre parte del HTML limpio del compañero.

print(f"\n🏁 HTML generado: {ruta}")
print(f"   Tamaño: {len(html_nuevo):,} bytes")
print(f"   Bloques en D: {list(D.keys())}")

# ============================================================================
# CELDA 6 · ENVÍO DE CORREO CON ADJUNTO + CC + CUERPO DINÁMICO
#
# Prerequisito: CELDA 1 (parámetros) y CELDA 5 (D construido, HTML generado)
# ya ejecutadas. Las variables D, FECHA_CORTE, PERIODO, etc. están en memoria.
#
# Cambios requeridos en Power Automate (una sola vez):
#   1. Trigger "manual" → agregar campos 'cc' y 'body' al JSON Schema.
#   2. Acción "Enviar correo V2":
#      - Campo CC  → triggerBody()?['cc']
#      - Campo Cuerpo → triggerBody()?['body']  con "¿Es HTML?" = Sí
# ============================================================================

import base64, requests, json

# ── Leer el HTML generado ────────────────────────────────────────────────────
ruta = os.path.join(OUTPUT_DIR, OUTPUT_NAME)
with open(ruta, "rb") as f:
    contenido_b64 = base64.b64encode(f.read()).decode()

# ── Construir cuerpo analítico dinámico ─────────────────────────────────────
def pct(v):
    if v is None: return "—"
    return f"{v:.1%}"

def q(v):
    if v is None: return "—"
    return f"Q{v:,.0f}"

# Fecha de la data — FECHA_CORTE ya maneja lunes → sábado anterior
_fc = str(FECHA_CORTE)  # YYYYMMDD
_fecha_str = f"{_fc[6:8]}/{_fc[4:6]}/{_fc[:4]}"
_kpi       = D["kpi"]
_tot       = D["totales"]
_efe       = D["efe_resumen"]
_nv        = D["nuevos_resumen"]

# Agencia con mayor desembolso semanal
_ag_top = max(D["agencias"], key=lambda x: x.get("monto_sem", 0))
# Agencia con mayor PAR8d en stock semanal
_ag_par = max(D["agencias"], key=lambda x: x.get("sem_par_stock", 0))

cuerpo_html = f"""
<div style="font-family: Arial, sans-serif; font-size: 14px; color: #222; max-width: 680px;">

  <p style="margin-bottom: 16px; line-height: 1.6;">
    Hola,<br>
    <br>
    Le adjunto el Tablero de Seguimiento de Medidas para la cartera de Entidad Microfinanciera
    al corte de <strong>{_fecha_str}</strong>.
  </p>

  <h2 style="color: #1a3a5c; margin-bottom: 4px;">
    Tablero de Seguimiento · Entidad Microfinanciera
  </h2>
  <p style="color: #666; margin-top: 0;">
    Corte: <strong>{_fecha_str}</strong> &nbsp;|&nbsp; Periodo: <strong>{PERIODO}</strong>
  </p>

  <hr style="border: none; border-top: 2px solid #1a3a5c; margin: 12px 0;">

  <!-- KPI principal -->
  <h3 style="color: #1a3a5c;">📊 Avance de Desembolsos</h3>
  <table style="border-collapse: collapse; width: 100%;">
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;"><strong>Contratos desembolsados</strong></td>
      <td style="padding: 8px 12px; text-align: right;">{_kpi['contratos']:,}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;"><strong>Monto desembolsado</strong></td>
      <td style="padding: 8px 12px; text-align: right;">{q(_kpi['desembolsado'])}</td>
    </tr>
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;"><strong>Presupuesto del mes</strong></td>
      <td style="padding: 8px 12px; text-align: right;">{q(_kpi['presupuesto'])}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;"><strong>Avance total</strong></td>
      <td style="padding: 8px 12px; text-align: right; font-size: 16px;
          color: {'#2e7d32' if (_kpi['avance'] or 0) >= 0.5 else '#c62828'};">
        <strong>{pct(_kpi['avance'])}</strong>
      </td>
    </tr>
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;">Avance Semanal</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_kpi['avance_sem'])}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;">Avance Catorcenal</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_kpi['avance_cat'])}</td>
    </tr>
  </table>

  <!-- Riesgo por segmento -->
  <h3 style="color: #1a3a5c; margin-top: 20px;">⚠️ Calidad de Cartera (PAR8d)</h3>
  <table style="border-collapse: collapse; width: 100%;">
    <tr style="background: #1a3a5c; color: white;">
      <th style="padding: 8px 12px; text-align: left;">Segmento</th>
      <th style="padding: 8px 12px; text-align: right;">PAR8d Stock</th>
      <th style="padding: 8px 12px; text-align: right;">Normalidad Stock</th>
      <th style="padding: 8px 12px; text-align: right;">Cartera</th>
    </tr>
    <tr>
      <td style="padding: 8px 12px;">Semanal Recurrentes</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_tot['sem_rec']['par_stock'])}</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_tot['sem_rec']['norm_stock'])}</td>
      <td style="padding: 8px 12px; text-align: right;">{q(_tot['sem_rec']['cartera'])}</td>
    </tr>
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;">Semanal Nuevos</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_tot['sem_new']['par_stock'])}</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_tot['sem_new']['norm_stock'])}</td>
      <td style="padding: 8px 12px; text-align: right;">{q(_tot['sem_new']['cartera'])}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;">Catorcenal Recurrentes</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_tot['cat_rec']['par_stock'])}</td>
      <td style="padding: 8px 12px; text-align: right;">{pct(_tot['cat_rec']['norm_stock'])}</td>
      <td style="padding: 8px 12px; text-align: right;">{q(_tot['cat_rec']['cartera'])}</td>
    </tr>
  </table>

  <!-- Efectividad desertores -->
  <h3 style="color: #1a3a5c; margin-top: 20px;">🔄 Efectividad de Recuperación</h3>
  <table style="border-collapse: collapse; width: 100%;">
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;"><strong>Base asignada</strong></td>
      <td style="padding: 8px 12px; text-align: right;">{_efe['base']:,}</td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;"><strong>Recuperados</strong></td>
      <td style="padding: 8px 12px; text-align: right;">{_efe['recup']:,}</td>
    </tr>
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;"><strong>Efectividad</strong></td>
      <td style="padding: 8px 12px; text-align: right;"><strong>{pct(_efe['efect'])}</strong></td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;">Agencias sin venta</td>
      <td style="padding: 8px 12px; text-align: right;">{_efe['sin_venta']}</td>
    </tr>
  </table>

  <!-- Clientes nuevos -->
  <h3 style="color: #1a3a5c; margin-top: 20px;">🆕 Clientes Nuevos por Agencia</h3>
  <table style="border-collapse: collapse; width: 100%;">
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;">Semanal — agencias OK (≥{_nv['meta_sem']})</td>
      <td style="padding: 8px 12px; text-align: right; color: #2e7d32;"><strong>{_nv['sem_ok']}</strong></td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;">Semanal — agencias en avance</td>
      <td style="padding: 8px 12px; text-align: right; color: #e65100;"><strong>{_nv['sem_av']}</strong></td>
    </tr>
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;">Semanal — agencias sin venta</td>
      <td style="padding: 8px 12px; text-align: right; color: #c62828;"><strong>{_nv['sem_0']}</strong></td>
    </tr>
    <tr>
      <td style="padding: 8px 12px;">Catorcenal — agencias OK (≥{_nv['meta_cat']})</td>
      <td style="padding: 8px 12px; text-align: right; color: #2e7d32;"><strong>{_nv['cat_ok']}</strong></td>
    </tr>
    <tr style="background: #f0f4f8;">
      <td style="padding: 8px 12px;">Catorcenal — agencias sin venta</td>
      <td style="padding: 8px 12px; text-align: right; color: #c62828;"><strong>{_nv['cat_0']}</strong></td>
    </tr>
  </table>

  <!-- Destacados -->
  <h3 style="color: #1a3a5c; margin-top: 20px;">🏆 Destacados del día</h3>
  <ul style="line-height: 1.8;">
    <li>Mayor desembolso semanal: <strong>{_ag_top['agencia']}</strong>
        ({q(_ag_top.get('monto_sem', 0))})</li>
    <li>Mayor PAR8d en stock semanal: <strong>{_ag_par['agencia']}</strong>
        ({pct(_ag_par.get('sem_par_stock', 0))})</li>
  </ul>

  <hr style="border: none; border-top: 1px solid #ccc; margin: 20px 0;">
  <p style="color: #999; font-size: 12px;">
    Reporte generado automáticamente · Institución Financiera · Entidad Microfinanciera<br>
    El detalle completo se encuentra en el archivo HTML adjunto.
  </p>

</div>
"""

# ── Armar payload ────────────────────────────────────────────────────────────
payload = {
    "filename":      OUTPUT_NAME,
    "contentBase64": contenido_b64,
    "to":            ";".join(DESTINATARIOS_TO),
    "cc":            ";".join(DESTINATARIOS_CC) if DESTINATARIOS_CC else "",
    "subject":       ASUNTO,
    "body":          cuerpo_html,
}

# ── Enviar ───────────────────────────────────────────────────────────────────
print(f"Enviando a:  {payload['to']}")
print(f"CC:          {payload['cc'] or '(ninguno)'}")
print(f"Asunto:      {payload['subject']}")
print(f"Adjunto:     {OUTPUT_NAME}  ({len(contenido_b64) // 1024:,} KB en base64)")

resp = requests.post(FLOW_URL, json=payload, timeout=120)
print(f"\nStatus HTTP: {resp.status_code}")

if resp.status_code in (200, 202):
    print("✅ Correo enviado correctamente")
else:
    print(f"❌ Error en el envío: {resp.text[:300]}")
    resp.raise_for_status()
