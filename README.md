# 📊 Automated Daily Portfolio Report Pipeline

Pipeline end-to-end de reportería automática de cartera crediticia para instituciones de microfinanzas, construido sobre **Microsoft Fabric**, **Spark SQL** y **Python**, con entrega automática vía **Power Automate**.

---
## 🔗 [Ver demo en vivo](https://ronaldo-chiche.github.io/credit-portfolio-automated-reporting/sample_output/sample_report.html)

## 🎯 Problema que resuelve

El seguimiento diario de la cartera de crédito en microfinanzas requería que el equipo analítico generara manualmente reportes en Excel cada mañana — un proceso de **6 a 8 horas semanales** propenso a errores y desactualizado al momento de llegar a los gerentes.

Este pipeline elimina ese proceso completamente: cada día hábil a las 10:34 PM (hora Lima), el sistema extrae los datos, calcula los indicadores, genera el reporte HTML y lo envía por correo de forma completamente autónoma.

---

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│  Fabric Pipeline — Programado: Lun–Vie 22:34 (UTC-5)            │
│                                                                   │
│  └── Notebook (Spark SQL + Python)                               │
│       │                                                           │
│       ├── CELDA 1 · Parámetros centralizados                     │
│       │    └── Auto-resolución de fecha de corte y período       │
│       │        (lunes → sábado anterior; demás días → ayer)      │
│       │                                                           │
│       ├── CELDA 2 · Query cartera detalle                        │
│       │    └── Spark SQL sobre hec.Cartera + dims → tabla temp   │
│       │                                                           │
│       ├── CELDA 3 · PAR8d diario por perfil/segmento            │
│       │                                                           │
│       ├── CELDA 4 · Tasa de recuperación de desertores           │
│       │    └── FULL OUTER JOIN: base asignada vs recuperados     │
│       │                                                           │
│       ├── CELDA 5 · Construcción del objeto D (métricas)         │
│       │    └── Agrega KPIs, totales, rankings en un dict Python  │
│       │                                                           │
│       └── CELDA 6 · Generación HTML + envío                      │
│            ├── HTML dinámico con f-strings (tablero interactivo) │
│            ├── Guarda en Lakehouse (Files/reportes/)             │
│            └── POST a Power Automate → correo con adjunto        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📈 Indicadores monitoreados

| Bloque | Indicadores |
|--------|-------------|
| **Desembolsos** | Contratos desembolsados, monto vs presupuesto, % avance por producto |
| **Calidad de cartera** | PAR8d por segmento (recurrentes/nuevos) y perfil de riesgo |
| **Recuperación** | Tasa de recuperación de desertores por agencia, producto y perfil |
| **Nuevos clientes** | Agencias en meta / en avance / sin venta vs meta diaria |
| **Ranking agencias** | Top por desembolso semanal y catorcenal |

---

## 🛠️ Stack técnico

| Capa | Tecnología |
|------|------------|
| Orquestación | Microsoft Fabric Pipeline (scheduled) |
| Procesamiento | Spark SQL · PySpark · Python (Pandas, json) |
| Almacenamiento | Microsoft Fabric Lakehouse (Delta tables) |
| Reporte | HTML dinámico generado con f-strings |
| Entrega | Power Automate (HTTP trigger → correo con adjunto) |

---

## 📁 Estructura del repositorio

```
credit-portfolio-automated-reporting/
├── README.md
├── notebook/
│   └── daily_report_pipeline.py   # Pipeline completo sanitizado
├── sample_output/
│   └── sample_report.html         # Ejemplo de reporte generado
└── docs/
    └── architecture.md            # Detalle técnico del diseño
```

---

## ⚙️ Configuración

Antes de ejecutar, configurar en `daily_report_pipeline.py`:

```python
# 1. Destinatarios
DESTINATARIOS_TO = ["gerencia@tu-entidad.com"]
DESTINATARIOS_CC = ["equipo@tu-entidad.com"]

# 2. URL del flujo Power Automate
FLOW_URL = "YOUR_POWER_AUTOMATE_FLOW_URL_HERE"

# 3. Empresa y productos
ID_EMPRESA = X          # ID de la entidad en la base de datos
PRODUCTOS  = ["PRODUCTO_A", "PRODUCTO_B"]

# 4. Zona horaria
hoy = datetime.now(ZoneInfo("America/Lima")).date()
```

---

## 🔍 Aspectos técnicos destacados

**Auto-resolución de fecha:** La Celda 1 detecta automáticamente si el día es lunes (→ toma el sábado anterior como corte) o cualquier otro día hábil (→ toma ayer), sin necesidad de configuración manual.

**Parámetro `params` CTE:** Todas las queries Spark SQL reciben los parámetros de período y fecha a través de un CTE inicial, eliminando fechas hardcodeadas y facilitando el backfill de períodos cerrados.

**Objeto D centralizado:** Los resultados de todas las queries se consolidan en un único diccionario Python (`D`) antes de la generación HTML, separando claramente la capa de datos de la capa de presentación.

**Tablero HTML responsivo:** El reporte final incluye navegación por tabs con JavaScript, tablas ordenables, KPI cards con colores semáforo y diseño adaptable a móvil — sin dependencia de librerías externas.

---

## 📬 Contacto

**Ronaldo Chiche Surco**
[LinkedIn](https://linkedin.com/in/ronaldo-chiche) · rchiches@uni.pe
