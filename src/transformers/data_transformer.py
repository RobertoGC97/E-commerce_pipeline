"""
DataTransformer - Clase para transformación y limpieza de datos
"""
import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Tuple
from datetime import datetime


class DataTransformer:
    """
    Clase para aplicar transformaciones, limpieza y joins
    a los datos de Olist
    """
    
    def __init__(self):
        """Inicializa el transformador"""
        self.transformations_log = []
    
    def log_transformation(self, message: str):
        """
        Registra una transformación aplicada
        
        Args:
            message: Descripción de la transformación
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.transformations_log.append(log_entry)
        print(f"✓ {message}")
    
    # ==================== LIMPIEZA DE DATOS ====================
    
    def clean_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Limpia nombres de columnas (lowercase, sin espacios)
        
        Args:
            df: DataFrame de Pandas
            
        Returns:
            DataFrame con columnas limpias
        """
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')
        self.log_transformation(f"Nombres de columnas limpiados")
        return df
    
    def convert_dates(self, df: pd.DataFrame, date_columns: List[str]) -> pd.DataFrame:
        """
        Convierte columnas a tipo datetime
        
        Args:
            df: DataFrame de Pandas
            date_columns: Lista de nombres de columnas de fecha
            
        Returns:
            DataFrame con fechas convertidas
        """
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                self.log_transformation(f"Columna '{col}' convertida a datetime")
        
        return df
    
    def handle_missing_values(self, df: pd.DataFrame, 
                            strategy: str = 'drop',
                            columns: List[str] = None) -> pd.DataFrame:
        """
        Maneja valores faltantes
        
        Args:
            df: DataFrame de Pandas
            strategy: 'drop', 'fill_zero', 'fill_mean', 'fill_mode'
            columns: Columnas específicas a procesar (None = todas)
            
        Returns:
            DataFrame procesado
        """
        if columns is None:
            columns = df.columns.tolist()
        
        if strategy == 'drop':
            initial_rows = len(df)
            df = df.dropna(subset=columns)
            dropped = initial_rows - len(df)
            self.log_transformation(f"Eliminadas {dropped} filas con valores nulos")
        
        elif strategy == 'fill_zero':
            df[columns] = df[columns].fillna(0)
            self.log_transformation(f"Valores nulos rellenados con 0")
        
        elif strategy == 'fill_mean':
            for col in columns:
                if df[col].dtype in ['float64', 'int64']:
                    df[col] = df[col].fillna(df[col].mean())
            self.log_transformation(f"Valores nulos rellenados con media")
        
        elif strategy == 'fill_mode':
            for col in columns:
                df[col] = df[col].fillna(df[col].mode()[0] if not df[col].mode().empty else None)
            self.log_transformation(f"Valores nulos rellenados con moda")
        
        return df
    
    def remove_duplicates(self, df: pd.DataFrame, 
                         subset: List[str] = None) -> pd.DataFrame:
        """
        Elimina filas duplicadas
        
        Args:
            df: DataFrame de Pandas
            subset: Columnas a considerar para duplicados
            
        Returns:
            DataFrame sin duplicados
        """
        initial_rows = len(df)
        df = df.drop_duplicates(subset=subset)
        removed = initial_rows - len(df)
        self.log_transformation(f"Eliminados {removed} registros duplicados")
        
        return df
    
    # ==================== TRANSFORMACIONES CON PANDAS ====================
    
    def add_derived_columns_orders(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Agrega columnas derivadas a la tabla de orders
        
        Args:
            df: DataFrame de orders
            
        Returns:
            DataFrame con columnas adicionales
        """
        # Asegurar que las fechas están en formato datetime
        date_cols = ['order_purchase_timestamp', 'order_approved_at', 
                     'order_delivered_carrier_date', 'order_delivered_customer_date',
                     'order_estimated_delivery_date']
        
        df = self.convert_dates(df, date_cols)
        
        # Tiempo de entrega en días
        if 'order_delivered_customer_date' in df.columns and 'order_purchase_timestamp' in df.columns:
            df['delivery_time_days'] = (
                df['order_delivered_customer_date'] - df['order_purchase_timestamp']
            ).dt.days
            self.log_transformation("Columna 'delivery_time_days' agregada")
        
        # Diferencia entre entrega estimada y real
        if 'order_estimated_delivery_date' in df.columns and 'order_delivered_customer_date' in df.columns:
            df['delivery_delay_days'] = (
                df['order_delivered_customer_date'] - df['order_estimated_delivery_date']
            ).dt.days
            self.log_transformation("Columna 'delivery_delay_days' agregada")
        
        # Año y mes de compra
        if 'order_purchase_timestamp' in df.columns:
            df['purchase_year'] = df['order_purchase_timestamp'].dt.year
            df['purchase_month'] = df['order_purchase_timestamp'].dt.month
            df['purchase_day_of_week'] = df['order_purchase_timestamp'].dt.dayofweek
            self.log_transformation("Columnas de fecha derivadas agregadas")
        
        return df
    
    def join_orders_with_items(self, orders: pd.DataFrame, 
                               order_items: pd.DataFrame) -> pd.DataFrame:
        """
        Join entre orders y order_items
        
        Args:
            orders: DataFrame de orders
            order_items: DataFrame de order_items
            
        Returns:
            DataFrame combinado
        """
        merged = orders.merge(
            order_items,
            on='order_id',
            how='inner'
        )
        
        self.log_transformation(
            f"Join orders-items: {len(merged):,} registros resultantes"
        )
        
        return merged
    
    def create_sales_summary(self, orders: pd.DataFrame,
                           order_items: pd.DataFrame,
                           payments: pd.DataFrame) -> pd.DataFrame:
        """
        Crea resumen de ventas por orden
        
        Args:
            orders: DataFrame de orders
            order_items: DataFrame de order_items
            payments: DataFrame de payments
            
        Returns:
            DataFrame con resumen de ventas
        """
        # Agrupar items por orden
        items_agg = order_items.groupby('order_id').agg({
            'order_item_id': 'count',
            'price': 'sum',
            'freight_value': 'sum'
        }).reset_index()
        
        items_agg.columns = ['order_id', 'total_items', 'total_price', 'total_freight']
        
        # Agrupar pagos por orden
        payments_agg = payments.groupby('order_id').agg({
            'payment_value': 'sum'
        }).reset_index()
        
        payments_agg.columns = ['order_id', 'total_payment']
        
        # Join todo
        summary = orders.merge(items_agg, on='order_id', how='left')
        summary = summary.merge(payments_agg, on='order_id', how='left')
        
        self.log_transformation(
            f"Resumen de ventas creado: {len(summary):,} órdenes"
        )
        
        return summary
    
    # ==================== TRANSFORMACIONES CON SPARK ====================
    
    def spark_clean_and_transform(self, df: SparkDataFrame, 
                                 table_name: str) -> SparkDataFrame:
        """
        Limpieza y transformación usando Spark
        
        Args:
            df: Spark DataFrame
            table_name: Nombre de la tabla
            
        Returns:
            Spark DataFrame transformado
        """
        # Eliminar duplicados
        initial_count = df.count()
        df = df.dropDuplicates()
        final_count = df.count()
        
        self.log_transformation(
            f"[Spark] {table_name}: {initial_count - final_count} duplicados eliminados"
        )
        
        return df
    
    def spark_join_orders_items_products(self, orders_df: SparkDataFrame,
                                        items_df: SparkDataFrame,
                                        products_df: SparkDataFrame) -> SparkDataFrame:
        """
        Join completo entre orders, items y products usando Spark
        
        Args:
            orders_df: Spark DataFrame de orders
            items_df: Spark DataFrame de items
            products_df: Spark DataFrame de products
            
        Returns:
            Spark DataFrame con join completo
        """
        # Join orders con items
        result = orders_df.join(items_df, on='order_id', how='inner')
        
        # Join con products
        result = result.join(products_df, on='product_id', how='left')
        
        count = result.count()
        self.log_transformation(
            f"[Spark] Join orders-items-products: {count:,} registros"
        )
        
        return result
    
    def spark_aggregate_by_category(self, df: SparkDataFrame) -> SparkDataFrame:
        """
        Agregación de ventas por categoría usando Spark
        
        Args:
            df: Spark DataFrame con datos de ventas
            
        Returns:
            Spark DataFrame agregado
        """
        result = df.groupBy('product_category_name').agg(
            F.count('order_id').alias('total_orders'),
            F.sum('price').alias('total_revenue'),
            F.avg('price').alias('avg_price'),
            F.sum('freight_value').alias('total_freight')
        ).orderBy(F.desc('total_revenue'))
        
        self.log_transformation(
            "[Spark] Agregación por categoría completada"
        )
        
        return result
    
    # ==================== UTILIDADES ====================
    
    def get_transformations_log(self) -> List[str]:
        """
        Retorna el log de transformaciones aplicadas
        
        Returns:
            Lista de transformaciones
        """
        return self.transformations_log
    
    def print_log(self):
        """Imprime el log de transformaciones"""
        print("\n" + "="*60)
        print("LOG DE TRANSFORMACIONES")
        print("="*60)
        for entry in self.transformations_log:
            print(entry)
        print("="*60 + "\n")


# Ejemplo de uso
if __name__ == "__main__":
    # Simulación con datos de ejemplo
    print("Ejemplo de uso de DataTransformer\n")
    
    # Crear datos de ejemplo
    orders_example = pd.DataFrame({
        'order_id': ['1', '2', '3'],
        'customer_id': ['c1', 'c2', 'c3'],
        'order_status': ['delivered', 'delivered', 'canceled'],
        'order_purchase_timestamp': ['2023-01-01', '2023-01-02', '2023-01-03'],
        'order_delivered_customer_date': ['2023-01-05', '2023-01-06', None]
    })
    
    # Crear transformer
    transformer = DataTransformer()
    
    # Aplicar transformaciones
    orders_clean = transformer.clean_column_names(orders_example)
    orders_clean = transformer.add_derived_columns_orders(orders_clean)
    
    print("\nDataFrame transformado:")
    print(orders_clean)
    
    # Mostrar log
    transformer.print_log()