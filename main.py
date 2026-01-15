#!/usr/bin/env python3
"""
Pipeline Principal de ETL para Datos de Olist
Orquesta la extracción, transformación y carga de datos
"""

import sys
import os
from datetime import datetime

# Agregar src al path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from extractors.data_extractor import DataExtractor
from transformers.data_transformer import DataTransformer
from loaders.data_loader import DataLoader


class OlistPipeline:
    """
    Clase principal que orquesta todo el pipeline ETL
    """
    
    def __init__(self, db_connection: str = None):
        """
        Inicializa el pipeline con sus componentes
        
        Args:
            db_connection: String de conexión a PostgreSQL (opcional)
        """
        print("\n" + "="*70)
        print("OLIST DATA PIPELINE - INICIANDO")
        print("="*70 + "\n")
        
        self.extractor = DataExtractor()
        self.transformer = DataTransformer()
        self.loader = DataLoader(db_connection)
        
        self.start_time = datetime.now()
        self.tables_raw = {}
        self.tables_transformed = {}
        
        print("✓ Pipeline inicializado correctamente\n")
    
    def run_extraction(self, use_spark: bool = False):
        """
        Ejecuta la fase de extracción
        
        Args:
            use_spark: Si usar Spark para extracción (default: Pandas)
        """
        print("\n" + "="*70)
        print("FASE 1: EXTRACCIÓN DE DATOS")
        print("="*70 + "\n")
        
        if use_spark:
            print("Modo: Spark")
            self.extractor.initialize_spark()
            
            # Cargar con Spark y convertir a Parquet
            csv_files = [
                'olist_orders_dataset.csv',
                'olist_order_items_dataset.csv',
                'olist_products_dataset.csv',
                'olist_customers_dataset.csv',
                'olist_sellers_dataset.csv',
                'olist_order_payments_dataset.csv'
            ]
            
            for csv_file in csv_files:
                try:
                    df_spark = self.extractor.read_csv_spark(csv_file)
                    parquet_name = csv_file.replace('.csv', '.parquet')
                    self.extractor.write_parquet_spark(df_spark, parquet_name)
                except Exception as e:
                    print(f"⚠️  Error con {csv_file}: {e}")
        else:
            print("Modo: Pandas")
            self.tables_raw = self.extractor.load_all_olist_tables()
        
        print("\n✅ Extracción completada\n")
    
    def run_transformation(self):
        """
        Ejecuta la fase de transformación
        """
        print("\n" + "="*70)
        print("FASE 2: TRANSFORMACIÓN DE DATOS")
        print("="*70 + "\n")
        
        if not self.tables_raw:
            print("⚠️  No hay datos cargados. Ejecuta run_extraction() primero.")
            return
        
        # Transformar tabla de orders
        if 'orders' in self.tables_raw:
            print("Transformando tabla: orders")
            orders = self.tables_raw['orders'].copy()
            orders = self.transformer.clean_column_names(orders)
            orders = self.transformer.add_derived_columns_orders(orders)
            orders = self.transformer.remove_duplicates(orders, subset=['order_id'])
            self.tables_transformed['orders'] = orders
        
        # Transformar tabla de customers
        if 'customers' in self.tables_raw:
            print("\nTransformando tabla: customers")
            customers = self.tables_raw['customers'].copy()
            customers = self.transformer.clean_column_names(customers)
            customers = self.transformer.remove_duplicates(customers, subset=['customer_id'])
            self.tables_transformed['customers'] = customers
        
        # Transformar tabla de products
        if 'products' in self.tables_raw:
            print("\nTransformando tabla: products")
            products = self.tables_raw['products'].copy()
            products = self.transformer.clean_column_names(products)
            products = self.transformer.remove_duplicates(products, subset=['product_id'])
            
            # Traducir categorías si existe la tabla de traducción
            if 'category_translation' in self.tables_raw:
                translation = self.tables_raw['category_translation'].copy()
                translation = self.transformer.clean_column_names(translation)
                products = products.merge(
                    translation,
                    on='product_category_name',
                    how='left'
                )
                print("  ✓ Categorías traducidas al inglés")
            
            self.tables_transformed['products'] = products
        
        # Transformar order_items
        if 'order_items' in self.tables_raw:
            print("\nTransformando tabla: order_items")
            items = self.tables_raw['order_items'].copy()
            items = self.transformer.clean_column_names(items)
            items = self.transformer.remove_duplicates(items)
            self.tables_transformed['order_items'] = items
        
        # Transformar payments
        if 'payments' in self.tables_raw:
            print("\nTransformando tabla: payments")
            payments = self.tables_raw['payments'].copy()
            payments = self.transformer.clean_column_names(payments)
            self.tables_transformed['payments'] = payments
        
        # Crear tabla de resumen de ventas (JOIN múltiple)
        print("\nCreando tabla resumen de ventas...")
        if all(k in self.tables_transformed for k in ['orders', 'order_items', 'payments']):
            sales_summary = self.transformer.create_sales_summary(
                self.tables_transformed['orders'],
                self.tables_transformed['order_items'],
                self.tables_transformed['payments']
            )
            self.tables_transformed['sales_summary'] = sales_summary
        
        print("\n✅ Transformación completada\n")
        self.transformer.print_log()
    
    def run_loading(self, export_format: str = 'csv', load_to_db: bool = False):
        """
        Ejecuta la fase de carga
        
        Args:
            export_format: 'csv', 'excel', 'parquet', 'all'
            load_to_db: Si cargar a PostgreSQL
        """
        print("\n" + "="*70)
        print("FASE 3: CARGA DE DATOS")
        print("="*70 + "\n")
        
        if not self.tables_transformed:
            print("⚠️  No hay datos transformados. Ejecuta run_transformation() primero.")
            return
        
        # Exportar reportes
        print(f"Exportando reportes (formato: {export_format})...\n")
        
        if export_format in ['csv', 'all']:
            for table_name, df in self.tables_transformed.items():
                self.loader.export_to_csv(df, f"{table_name}.csv")
        
        if export_format in ['parquet', 'all']:
            for table_name, df in self.tables_transformed.items():
                self.loader.export_to_parquet(df, f"{table_name}.parquet")
        
        if export_format in ['excel', 'all']:
            # Exportar las tablas principales en un solo Excel
            main_tables = {
                k: v for k, v in self.tables_transformed.items()
                if k in ['orders', 'sales_summary', 'products', 'customers']
            }
            if main_tables:
                self.loader.export_to_excel(main_tables, 'olist_report.xlsx')
        
        # Crear y exportar reportes resumen
        print("\nCreando reportes resumen...\n")
        if 'sales_summary' in self.tables_transformed:
            summary_report = self.loader.create_summary_report(
                self.tables_transformed['sales_summary'],
                'sales_summary'
            )
            self.loader.export_to_csv(summary_report, 'data_quality_report.csv')
        
        # Cargar a PostgreSQL si está configurado
        if load_to_db and self.loader.engine:
            print("\nCargando a PostgreSQL...\n")
            results = self.loader.load_multiple_tables(self.tables_transformed)
            
            # Mostrar resultados
            print("\nResultados de carga a DB:")
            for table, success in results.items():
                status = "✓" if success else "✗"
                print(f"  {status} {table}")
        
        print("\n✅ Carga completada\n")
        self.loader.print_log()
    
    def run_full_pipeline(self, use_spark: bool = False, 
                         export_format: str = 'csv',
                         load_to_db: bool = False):
        """
        Ejecuta el pipeline completo (Extract -> Transform -> Load)
        
        Args:
            use_spark: Si usar Spark para procesamiento
            export_format: Formato de exportación
            load_to_db: Si cargar a base de datos
        """
        try:
            # Extracción
            self.run_extraction(use_spark=use_spark)
            
            # Transformación
            self.run_transformation()
            
            # Carga
            self.run_loading(export_format=export_format, load_to_db=load_to_db)
            
            # Resumen final
            self.print_summary()
            
        except Exception as e:
            print(f"\n❌ Error en el pipeline: {e}")
            raise
        finally:
            self.cleanup()
    
    def print_summary(self):
        """Imprime resumen de ejecución del pipeline"""
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        print("\n" + "="*70)
        print("RESUMEN DE EJECUCIÓN")
        print("="*70)
        print(f"Inicio:          {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Fin:             {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Duración:        {duration:.2f} segundos")
        print(f"Tablas cargadas: {len(self.tables_raw)}")
        print(f"Tablas procesadas: {len(self.tables_transformed)}")
        
        if self.tables_transformed:
            print(f"\nTablas generadas:")
            for table_name, df in self.tables_transformed.items():
                print(f"  • {table_name}: {len(df):,} filas")
        
        print("="*70 + "\n")
    
    def cleanup(self):
        """Limpia recursos (Spark, DB connections)"""
        if self.extractor.spark:
            self.extractor.stop_spark()
        
        if self.loader.engine:
            self.loader.close_connection()
        
        print("✓ Recursos liberados\n")


def main():
    """Función principal"""
    
    # Configuración
    DB_CONNECTION = None  # "postgresql://user:password@localhost:5432/olist_db"
    USE_SPARK = False
    EXPORT_FORMAT = 'csv'  # 'csv', 'excel', 'parquet', 'all'
    LOAD_TO_DB = False
    
    # Crear y ejecutar pipeline
    pipeline = OlistPipeline(db_connection=DB_CONNECTION)
    
    pipeline.run_full_pipeline(
        use_spark=USE_SPARK,
        export_format=EXPORT_FORMAT,
        load_to_db=LOAD_TO_DB
    )
    
    print("🎉 Pipeline ejecutado exitosamente!")


if __name__ == "__main__":
    main()