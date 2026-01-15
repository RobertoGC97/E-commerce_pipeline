# 🛒 Olist Data Pipeline - Data Engineering Project

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-orange.svg)
![Pandas](https://img.shields.io/badge/Pandas-2.1.0-green.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-13+-blue.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

Pipeline completo de ETL (Extract, Transform, Load) para análisis de datos de e-commerce utilizando el dataset público de **Olist** (Brasil). Este proyecto demuestra habilidades en ingeniería de datos, programación orientada a objetos con Python, procesamiento distribuido con Spark y análisis SQL avanzado.

---

## 📋 Tabla de Contenidos

- [Descripción](#-descripción)
- [Stack Tecnológico](#-stack-tecnológico)
- [Arquitectura](#-arquitectura)
- [Estructura del Proyecto](#-estructura-del-proyecto)
- [Instalación](#-instalación)
- [Uso](#-uso)
- [Características Principales](#-características-principales)
- [Queries SQL](#-queries-sql)
- [Resultados](#-resultados)
- [Próximas Mejoras](#-próximas-mejoras)
- [Autor](#-autor)

---

## 🎯 Descripción

Este proyecto implementa un pipeline de datos completo que procesa información de más de 100,000 órdenes del marketplace brasileño Olist. El pipeline extrae datos de múltiples fuentes CSV, aplica transformaciones y limpieza, y carga los resultados en una base de datos PostgreSQL, generando reportes analíticos en el proceso.

**Dataset utilizado:** [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### Objetivos del Proyecto

✅ Demostrar arquitectura ETL escalable con Python  
✅ Aplicar principios de Programación Orientada a Objetos  
✅ Implementar procesamiento distribuido con PySpark  
✅ Ejecutar análisis SQL avanzado (JOINS, agregaciones, CTEs)  
✅ Crear pipeline reproducible con buenas prácticas de código

---

## 🛠 Stack Tecnológico

| Tecnología | Versión | Propósito |
|-----------|---------|-----------|
| **Python** | 3.9+ | Lenguaje principal |
| **Pandas** | 2.1.0 | Transformación de datos |
| **PySpark** | 3.5.0 | Procesamiento distribuido |
| **PostgreSQL** | 13+ | Base de datos relacional |
| **SQLAlchemy** | 2.0.20 | ORM y conexión a DB |
| **PyArrow** | 13.0.0 | Formato Parquet |
| **Git** | 2.x | Control de versiones |

---

## 🏗 Arquitectura

El proyecto sigue una arquitectura modular de tres capas:

```
┌─────────────────────────────────────────────────────────┐
│                    EXTRACCIÓN (Extract)                  │
│  ┌──────────────┐     ┌──────────────┐                 │
│  │   CSV Files  │────▶│DataExtractor │────▶ Parquet    │
│  └──────────────┘     └──────────────┘                 │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                TRANSFORMACIÓN (Transform)                │
│  ┌────────────────┐                                     │
│  │DataTransformer │─▶ Limpieza                         │
│  └────────────────┘   Validación                       │
│                       Joins                             │
│                       Agregaciones                      │
└─────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────┐
│                     CARGA (Load)                        │
│  ┌────────────┐                                         │
│  │DataLoader  │─▶ PostgreSQL                           │
│  └────────────┘   CSV Reports                          │
│                   Excel Reports                         │
│                   Parquet Files                         │
└─────────────────────────────────────────────────────────┘
```

### Componentes Principales

**1. DataExtractor** (`src/extractors/data_extractor.py`)
- Lectura de archivos CSV con Pandas y Spark
- Creación y gestión de Spark Sessions
- Lectura/escritura de archivos Parquet
- Carga masiva de tablas Olist

**2. DataTransformer** (`src/transformers/data_transformer.py`)
- Limpieza de datos (nombres de columnas, duplicados)
- Conversión de tipos de datos
- Manejo de valores nulos (múltiples estrategias)
- Joins entre tablas relacionales
- Creación de columnas derivadas
- Agregaciones con Spark

**3. DataLoader** (`src/loaders/data_loader.py`)
- Conexión a PostgreSQL con SQLAlchemy
- Carga de datos a base de datos
- Exportación a múltiples formatos (CSV, Excel, Parquet)
- Generación de reportes de calidad de datos
- Ejecución de queries SQL

**4. OlistPipeline** (`main.py`)
- Orquestación del flujo ETL completo
- Logging de operaciones
- Manejo de errores y excepciones
- Resumen de ejecución

---

## 📁 Estructura del Proyecto

```
olist-data-pipeline/
├── data/
│   ├── raw/                    # CSVs originales de Olist
│   ├── processed/              # Archivos Parquet procesados
│   └── reports/                # Reportes generados
├── src/
│   ├── __init__.py
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── data_extractor.py  # Clase para extracción
│   ├── transformers/
│   │   ├── __init__.py
│   │   └── data_transformer.py # Clase para transformación
│   └── loaders/
│       ├── __init__.py
│       └── data_loader.py     # Clase para carga
├── sql/
│   └── queries.sql            # 12 queries de análisis
├── notebooks/
│   └── exploratory_analysis.ipynb
├── tests/
│   └── __init__.py
├── .gitignore
├── requirements.txt
├── README.md
└── main.py                    # Orquestador principal
```

---

## 🚀 Instalación

### Prerrequisitos

- Python 3.9 o superior
- PostgreSQL 13+ (opcional, para carga a DB)
- Git

### Pasos de Instalación

1. **Clonar el repositorio**
```bash
git clone https://github.com/tu-usuario/olist-data-pipeline.git
cd olist-data-pipeline
```

2. **Crear entorno virtual**
```bash
python -m venv venv

# Linux/Mac
source venv/bin/activate

# Windows
venv\Scripts\activate
```

3. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

4. **Descargar dataset de Olist**
- Descarga desde [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- Extrae los CSVs en la carpeta `data/raw/`

5. **Configurar PostgreSQL (opcional)**
```bash
# Edita main.py y configura tu connection string
DB_CONNECTION = "postgresql://user:password@localhost:5432/olist_db"
```

---

## 💻 Uso

### Ejecución Básica

```bash
python main.py
```

Esto ejecutará el pipeline completo:
- ✅ Extrae datos de los CSVs
- ✅ Aplica transformaciones y limpieza
- ✅ Genera reportes en CSV
- ✅ Muestra resumen de ejecución

### Configuración Avanzada

Edita las variables en `main.py`:

```python
# Usar Spark para procesamiento
USE_SPARK = True

# Formato de exportación: 'csv', 'excel', 'parquet', 'all'
EXPORT_FORMAT = 'all'

# Cargar a PostgreSQL
LOAD_TO_DB = True
DB_CONNECTION = "postgresql://user:pass@localhost:5432/db"
```

### Uso Modular

Puedes usar cada clase de forma independiente:

```python
from src.extractors.data_extractor import DataExtractor
from src.transformers.data_transformer import DataTransformer

# Extraer datos
extractor = DataExtractor()
tables = extractor.load_all_olist_tables()

# Transformar
transformer = DataTransformer()
orders_clean = transformer.add_derived_columns_orders(tables['orders'])
```

---

## ⚡ Características Principales

### 1. Extracción de Datos
- ✅ Lectura eficiente de múltiples CSVs
- ✅ Soporte para Pandas y PySpark
- ✅ Conversión automática a formato Parquet
- ✅ Manejo de errores y archivos faltantes

### 2. Transformación
- ✅ Limpieza automatizada de datos
- ✅ Manejo inteligente de valores nulos (4 estrategias)
- ✅ Detección y eliminación de duplicados
- ✅ Joins relacionales entre 8+ tablas
- ✅ Creación de métricas derivadas:
  - Tiempo de entrega
  - Retrasos en envíos
  - Análisis temporal
- ✅ Logging completo de transformaciones

### 3. Carga y Exportación
- ✅ Carga masiva a PostgreSQL
- ✅ Exportación multi-formato (CSV, Excel, Parquet)
- ✅ Generación de reportes de calidad de datos
- ✅ Ejecución de queries personalizadas

### 4. Buenas Prácticas
- ✅ Arquitectura POO modular y extensible
- ✅ Type hints en Python
- ✅ Docstrings en todas las funciones
- ✅ Manejo robusto de excepciones
- ✅ Logging de operaciones
- ✅ Código reutilizable y testeable

---

## 📊 Queries SQL

El proyecto incluye **12 queries SQL avanzadas** en `sql/queries.sql`:

| # | Query | Conceptos Demostrados |
|---|-------|----------------------|
| 1 | Ventas por Categoría | JOIN, GROUP BY, agregaciones |
| 2 | Top Vendedores | JOIN múltiple, subqueries |
| 3 | Análisis de Entregas | Date functions, CASE WHEN |
| 4 | Clientes por Estado | JOIN, agregaciones múltiples |
| 5 | Productos Más Vendidos | Window functions, PARTITION BY |
| 6 | Métodos de Pago | GROUP BY, ORDER BY |
| 7 | Ventas Temporales | Date functions, GROUP BY temporal |
| 8 | Análisis de Reviews | JOIN, agregaciones condicionales |
| 9 | Clientes Valiosos | Subqueries, ORDER BY |
| 10 | Costos de Envío | JOIN, agregaciones, HAVING |
| 11 | Cohort Analysis | CTEs, Window functions complejas |
| 12 | Dashboard Summary | Múltiples CTEs, métricas generales |

### Ejemplo de Query

```sql
-- Top 10 Vendedores por Revenue
SELECT 
    s.seller_id,
    s.seller_city,
    COUNT(DISTINCT oi.order_id) AS ordenes,
    ROUND(SUM(oi.price)::NUMERIC, 2) AS revenue_total,
    ROUND(AVG(r.review_score)::NUMERIC, 2) AS rating
FROM sellers s
INNER JOIN order_items oi ON s.seller_id = oi.seller_id
INNER JOIN orders o ON oi.order_id = o.order_id
LEFT JOIN reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_id, s.seller_city
ORDER BY revenue_total DESC
LIMIT 10;
```

---

## 📈 Resultados

### Métricas Procesadas

- **100,000+** órdenes analizadas
- **8 tablas** relacionales procesadas
- **50+ columnas** derivadas creadas
- **12 reportes** analíticos generados

### Insights Obtenidos

1. **Categorías más vendidas**: Electrónicos, muebles, deportes
2. **Tiempo promedio de entrega**: 12-15 días
3. **Tasa de entregas tarde**: ~10%
4. **Rating promedio**: 4.2/5.0
5. **Métodos de pago**: Credit card (76%), Boleto (19%)

---

## 🔮 Próximas Mejoras

- [ ] Implementar tests unitarios con Pytest
- [ ] Agregar pipeline CI/CD con GitHub Actions
- [ ] Dockerizar la aplicación
- [ ] Implementar Apache Airflow para scheduling
- [ ] Agregar dashboard con Streamlit/Dash
- [ ] Integrar con AWS S3 y Redshift
- [ ] Implementar data quality checks automáticos
- [ ] Agregar monitoring con Grafana

---

## 👨‍💻 Autor

**[Roberto Gomez]**

- 📧 Email: roberto.kgc@gmail.com 
- 💼 LinkedIn: www.linkedin.com/in/rkgc0897
- 🐙 GitHub: https://github.com/RobertoGC97 

---

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

---

## 🙏 Agradecimientos

- Dataset proporcionado por [Olist](https://olist.com/) en Kaggle
- Inspiración de proyectos de la comunidad de Data Engineering
- Documentación oficial de PySpark, Pandas y PostgreSQL

---

## 📚 Referencias

- [Dataset Original - Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
