"""
DataExtractor - Clase para extracción de datos desde múltiples fuentes
"""
import pandas as pd
from pyspark.sql import SparkSession
from typing import Dict, List
import os


class DataExtractor:
    """
    Clase para extraer datos desde archivos CSV y Parquet
    utilizando Pandas y PySpark
    """
    
    def __init__(self, raw_data_path: str = "data/raw", 
                 processed_data_path: str = "data/processed"):
        """
        Inicializa el extractor con las rutas de datos
        
        Args:
            raw_data_path: Ruta a los datos crudos (CSV)
            processed_data_path: Ruta a los datos procesados (Parquet)
        """
        self.raw_data_path = raw_data_path
        self.processed_data_path = processed_data_path
        self.spark = None
        
    def initialize_spark(self, app_name: str = "OlistPipeline") -> SparkSession:
        """
        Crea y retorna una Spark Session
        
        Args:
            app_name: Nombre de la aplicación Spark
            
        Returns:
            SparkSession configurada
        """
        if self.spark is None:
            self.spark = SparkSession.builder \
                .appName(app_name) \
                .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
            
            print(f"✓ Spark Session creada: {app_name}")
        
        return self.spark
    
    def read_csv_pandas(self, filename: str, **kwargs) -> pd.DataFrame:
        """
        Lee un archivo CSV usando Pandas
        
        Args:
            filename: Nombre del archivo CSV
            **kwargs: Argumentos adicionales para pd.read_csv
            
        Returns:
            DataFrame de Pandas
        """
        filepath = os.path.join(self.raw_data_path, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
        
        df = pd.read_csv(filepath, **kwargs)
        print(f"✓ CSV cargado con Pandas: {filename} ({len(df):,} filas)")
        
        return df
    
    def read_csv_spark(self, filename: str, **kwargs):
        """
        Lee un archivo CSV usando Spark
        
        Args:
            filename: Nombre del archivo CSV
            **kwargs: Argumentos adicionales para spark.read.csv
            
        Returns:
            Spark DataFrame
        """
        if self.spark is None:
            self.initialize_spark()
        
        filepath = os.path.join(self.raw_data_path, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
        
        df = self.spark.read.csv(
            filepath,
            header=True,
            inferSchema=True,
            **kwargs
        )
        
        print(f"✓ CSV cargado con Spark: {filename} ({df.count():,} filas)")
        
        return df
    
    def read_parquet_spark(self, filename: str):
        """
        Lee un archivo Parquet usando Spark
        
        Args:
            filename: Nombre del archivo Parquet
            
        Returns:
            Spark DataFrame
        """
        if self.spark is None:
            self.initialize_spark()
        
        filepath = os.path.join(self.processed_data_path, filename)
        
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Archivo no encontrado: {filepath}")
        
        df = self.spark.read.parquet(filepath)
        print(f"✓ Parquet cargado con Spark: {filename} ({df.count():,} filas)")
        
        return df
    
    def write_parquet_spark(self, df, filename: str, mode: str = "overwrite"):
        """
        Escribe un Spark DataFrame a formato Parquet
        
        Args:
            df: Spark DataFrame a escribir
            filename: Nombre del archivo de salida
            mode: Modo de escritura (overwrite, append)
        """
        if self.spark is None:
            self.initialize_spark()
        
        filepath = os.path.join(self.processed_data_path, filename)
        
        # Crear directorio si no existe
        os.makedirs(self.processed_data_path, exist_ok=True)
        
        df.write.parquet(filepath, mode=mode)
        print(f"✓ Parquet guardado: {filename}")
    
    def load_all_olist_tables(self) -> Dict[str, pd.DataFrame]:
        """
        Carga todas las tablas de Olist usando Pandas
        
        Returns:
            Diccionario con nombre de tabla como key y DataFrame como value
        """
        tables = {
            'customers': 'olist_customers_dataset.csv',
            'orders': 'olist_orders_dataset.csv',
            'order_items': 'olist_order_items_dataset.csv',
            'products': 'olist_products_dataset.csv',
            'sellers': 'olist_sellers_dataset.csv',
            'payments': 'olist_order_payments_dataset.csv',
            'reviews': 'olist_order_reviews_dataset.csv',
            'geolocation': 'olist_geolocation_dataset.csv',
            'category_translation': 'product_category_name_translation.csv'
        }
        
        dataframes = {}
        
        print("\n📥 Cargando tablas de Olist con Pandas...")
        for table_name, filename in tables.items():
            try:
                dataframes[table_name] = self.read_csv_pandas(filename)
            except FileNotFoundError:
                print(f"⚠️  Tabla no encontrada: {filename}")
        
        print(f"\n✅ {len(dataframes)} tablas cargadas exitosamente\n")
        
        return dataframes
    
    def get_table_info(self, df: pd.DataFrame, table_name: str = ""):
        """
        Imprime información resumida de una tabla
        
        Args:
            df: DataFrame de Pandas
            table_name: Nombre de la tabla (opcional)
        """
        print(f"\n{'='*60}")
        print(f"Información de tabla: {table_name}")
        print(f"{'='*60}")
        print(f"Filas: {len(df):,}")
        print(f"Columnas: {len(df.columns)}")
        print(f"\nPrimeras columnas:")
        print(df.columns.tolist()[:10])
        print(f"\nTipos de datos:")
        print(df.dtypes)
        print(f"\nValores nulos:")
        print(df.isnull().sum())
        print(f"{'='*60}\n")
    
    def stop_spark(self):
        """Detiene la Spark Session"""
        if self.spark is not None:
            self.spark.stop()
            print("✓ Spark Session detenida")
            self.spark = None


# Ejemplo de uso
if __name__ == "__main__":
    # Crear instancia del extractor
    extractor = DataExtractor()
    
    # Cargar todas las tablas con Pandas
    tables = extractor.load_all_olist_tables()
    
    # Mostrar info de la tabla de orders
    if 'orders' in tables:
        extractor.get_table_info(tables['orders'], 'orders')
    
    # Ejemplo con Spark: leer CSV y guardar como Parquet
    extractor.initialize_spark()
    
    if 'orders' in tables:
        orders_spark = extractor.read_csv_spark('olist_orders_dataset.csv')
        extractor.write_parquet_spark(orders_spark, 'orders.parquet')
    
    # Leer el Parquet guardado
    orders_parquet = extractor.read_parquet_spark('orders.parquet')
    orders_parquet.show(5)
    
    # Detener Spark
    extractor.stop_spark()