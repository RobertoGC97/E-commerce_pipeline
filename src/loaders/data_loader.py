"""
DataLoader - Clase para cargar datos a bases de datos y exportar reportes
"""
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Dict, List, Optional
import os
from datetime import datetime


class DataLoader:
    """
    Clase para cargar datos procesados a PostgreSQL
    y exportar reportes en diferentes formatos
    """
    
    def __init__(self, connection_string: Optional[str] = None):
        """
        Inicializa el loader con conexión a base de datos
        
        Args:
            connection_string: String de conexión SQLAlchemy
                              Ej: 'postgresql://user:password@localhost:5432/dbname'
        """
        self.connection_string = connection_string
        self.engine = None
        self.load_log = []
        
        if connection_string:
            self._create_engine()
    
    def _create_engine(self):
        """Crea el engine de SQLAlchemy"""
        try:
            self.engine = create_engine(self.connection_string)
            print("✓ Conexión a base de datos establecida")
        except Exception as e:
            print(f"✗ Error al conectar a base de datos: {e}")
            self.engine = None
    
    def set_connection(self, connection_string: str):
        """
        Establece una nueva conexión
        
        Args:
            connection_string: String de conexión SQLAlchemy
        """
        self.connection_string = connection_string
        self._create_engine()
    
    def log_load(self, message: str):
        """
        Registra una operación de carga
        
        Args:
            message: Descripción de la operación
        """
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        self.load_log.append(log_entry)
        print(f"✓ {message}")
    
    # ==================== CARGA A POSTGRESQL ====================
    
    def load_to_postgres(self, df: pd.DataFrame, 
                        table_name: str,
                        if_exists: str = 'replace',
                        index: bool = False) -> bool:
        """
        Carga un DataFrame a PostgreSQL
        
        Args:
            df: DataFrame de Pandas
            table_name: Nombre de la tabla destino
            if_exists: 'fail', 'replace', 'append'
            index: Si incluir el índice como columna
            
        Returns:
            True si fue exitoso, False en caso contrario
        """
        if self.engine is None:
            print("✗ No hay conexión a base de datos")
            return False
        
        try:
            rows_loaded = len(df)
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists=if_exists,
                index=index,
                method='multi',
                chunksize=1000
            )
            
            self.log_load(
                f"Tabla '{table_name}' cargada: {rows_loaded:,} filas ({if_exists})"
            )
            return True
            
        except Exception as e:
            print(f"✗ Error al cargar tabla '{table_name}': {e}")
            return False
    
    def load_multiple_tables(self, tables_dict: Dict[str, pd.DataFrame],
                           if_exists: str = 'replace') -> Dict[str, bool]:
        """
        Carga múltiples DataFrames a PostgreSQL
        
        Args:
            tables_dict: Diccionario {nombre_tabla: DataFrame}
            if_exists: 'fail', 'replace', 'append'
            
        Returns:
            Diccionario con resultados {tabla: éxito}
        """
        results = {}
        
        print(f"\n📤 Cargando {len(tables_dict)} tablas a PostgreSQL...\n")
        
        for table_name, df in tables_dict.items():
            success = self.load_to_postgres(df, table_name, if_exists)
            results[table_name] = success
        
        successful = sum(results.values())
        print(f"\n✅ {successful}/{len(tables_dict)} tablas cargadas exitosamente\n")
        
        return results
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Ejecuta una query SQL y retorna resultados
        
        Args:
            query: Query SQL a ejecutar
            
        Returns:
            DataFrame con resultados
        """
        if self.engine is None:
            print("✗ No hay conexión a base de datos")
            return pd.DataFrame()
        
        try:
            df = pd.read_sql(query, self.engine)
            self.log_load(f"Query ejecutada: {len(df):,} filas retornadas")
            return df
            
        except Exception as e:
            print(f"✗ Error al ejecutar query: {e}")
            return pd.DataFrame()
    
    def table_exists(self, table_name: str) -> bool:
        """
        Verifica si una tabla existe en la base de datos
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            True si existe, False en caso contrario
        """
        if self.engine is None:
            return False
        
        query = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{table_name}'
        );
        """
        
        try:
            result = self.execute_query(query)
            return result.iloc[0, 0] if not result.empty else False
        except:
            return False
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Obtiene el número de filas de una tabla
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            Número de filas
        """
        query = f"SELECT COUNT(*) FROM {table_name};"
        result = self.execute_query(query)
        return int(result.iloc[0, 0]) if not result.empty else 0
    
    # ==================== EXPORTACIÓN DE REPORTES ====================
    
    def export_to_csv(self, df: pd.DataFrame, 
                     filename: str,
                     output_dir: str = "data/reports") -> bool:
        """
        Exporta DataFrame a CSV
        
        Args:
            df: DataFrame a exportar
            filename: Nombre del archivo
            output_dir: Directorio de salida
            
        Returns:
            True si fue exitoso
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, filename)
            
            df.to_csv(filepath, index=False, encoding='utf-8')
            self.log_load(f"CSV exportado: {filepath} ({len(df):,} filas)")
            return True
            
        except Exception as e:
            print(f"✗ Error al exportar CSV: {e}")
            return False
    
    def export_to_excel(self, dfs_dict: Dict[str, pd.DataFrame],
                       filename: str,
                       output_dir: str = "data/reports") -> bool:
        """
        Exporta múltiples DataFrames a Excel (múltiples hojas)
        
        Args:
            dfs_dict: Diccionario {nombre_hoja: DataFrame}
            filename: Nombre del archivo
            output_dir: Directorio de salida
            
        Returns:
            True si fue exitoso
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, filename)
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                for sheet_name, df in dfs_dict.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            self.log_load(
                f"Excel exportado: {filepath} ({len(dfs_dict)} hojas)"
            )
            return True
            
        except Exception as e:
            print(f"✗ Error al exportar Excel: {e}")
            return False
    
    def export_to_parquet(self, df: pd.DataFrame,
                         filename: str,
                         output_dir: str = "data/reports") -> bool:
        """
        Exporta DataFrame a Parquet
        
        Args:
            df: DataFrame a exportar
            filename: Nombre del archivo
            output_dir: Directorio de salida
            
        Returns:
            True si fue exitoso
        """
        try:
            os.makedirs(output_dir, exist_ok=True)
            filepath = os.path.join(output_dir, filename)
            
            df.to_parquet(filepath, index=False, engine='pyarrow')
            self.log_load(f"Parquet exportado: {filepath} ({len(df):,} filas)")
            return True
            
        except Exception as e:
            print(f"✗ Error al exportar Parquet: {e}")
            return False
    
    def create_summary_report(self, df: pd.DataFrame, 
                            report_name: str = "summary") -> pd.DataFrame:
        """
        Crea un reporte resumen de un DataFrame
        
        Args:
            df: DataFrame a analizar
            report_name: Nombre del reporte
            
        Returns:
            DataFrame con estadísticas resumen
        """
        summary = {
            'Columna': df.columns.tolist(),
            'Tipo': df.dtypes.tolist(),
            'No_Nulos': df.count().tolist(),
            'Nulos': df.isnull().sum().tolist(),
            'Porcentaje_Nulos': (df.isnull().sum() / len(df) * 100).round(2).tolist(),
            'Valores_Unicos': [df[col].nunique() for col in df.columns]
        }
        
        summary_df = pd.DataFrame(summary)
        self.log_load(f"Reporte resumen '{report_name}' creado")
        
        return summary_df
    
    # ==================== UTILIDADES ====================
    
    def get_load_log(self) -> List[str]:
        """
        Retorna el log de operaciones de carga
        
        Returns:
            Lista de operaciones
        """
        return self.load_log
    
    def print_log(self):
        """Imprime el log de operaciones"""
        print("\n" + "="*60)
        print("LOG DE OPERACIONES DE CARGA")
        print("="*60)
        for entry in self.load_log:
            print(entry)
        print("="*60 + "\n")
    
    def close_connection(self):
        """Cierra la conexión a la base de datos"""
        if self.engine:
            self.engine.dispose()
            print("✓ Conexión cerrada")
            self.engine = None


# Ejemplo de uso
if __name__ == "__main__":
    print("Ejemplo de uso de DataLoader\n")
    
    # Crear datos de ejemplo
    sample_data = pd.DataFrame({
        'order_id': ['1', '2', '3'],
        'customer_id': ['c1', 'c2', 'c3'],
        'total_value': [100.50, 200.75, 150.00],
        'status': ['delivered', 'delivered', 'canceled']
    })
    
    # Crear loader (sin conexión DB para el ejemplo)
    loader = DataLoader()
    
    # Exportar a CSV
    loader.export_to_csv(sample_data, 'sales_report.csv')
    
    # Crear reporte resumen
    summary = loader.create_summary_report(sample_data, 'sales')
    print("\nReporte Resumen:")
    print(summary)
    
    # Exportar a Excel
    reports = {
        'Ventas': sample_data,
        'Resumen': summary
    }
    loader.export_to_excel(reports, 'sales_analysis.xlsx')
    
    # Mostrar log
    loader.print_log()
    
    # Ejemplo de conexión a PostgreSQL (comentado)
    # connection_string = "postgresql://user:password@localhost:5432/olist_db"
    # loader.set_connection(connection_string)
    # loader.load_to_postgres(sample_data, 'sales_table')
    # loader.close_connection()