"""
Funciones auxiliares para el procesamiento de datos y operaciones comunes
"""

import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
import re
import logging
from datetime import datetime

def clean_value(value: Any) -> str:
    """
    Limpia valores nulos y NaN, retornando string vacío en su lugar
    
    Args:
        value: Valor a limpiar
        
    Returns:
        str: Valor limpio o string vacío
    """
    if value is None or pd.isna(value) or value == 'nan' or value == 'NaN' or not str(value).strip():
        return ""
    return str(value).strip()

def is_variant_reference(reference: str) -> bool:
    """
    Determina si una referencia corresponde a una variante
    
    Args:
        reference (str): Referencia a verificar
        
    Returns:
        bool: True si es una referencia de variante
    """
    return '/' in reference

def get_base_reference(reference: str) -> str:
    """
    Obtiene la referencia base sin el número de talla
    
    Args:
        reference (str): Referencia completa
        
    Returns:
        str: Referencia base
    """
    return reference.split('/')[0] if '/' in reference else reference

def get_variant_size(reference: str) -> Optional[str]:
    """
    Extrae la talla de una referencia de variante
    
    Args:
        reference (str): Referencia completa
        
    Returns:
        Optional[str]: Talla extraída o None si no es una variante
    """
    return reference.split('/')[1] if '/' in reference else None

def format_price(price: Any) -> float:
    """
    Formatea un precio asegurando que sea un float válido
    
    Args:
        price: Precio en cualquier formato
        
    Returns:
        float: Precio formateado
    """
    try:
        if isinstance(price, str):
            # Eliminar caracteres no numéricos excepto punto y coma
            price = re.sub(r'[^\d.,]', '', price)
            # Reemplazar coma por punto
            price = price.replace(',', '.')
        return float(price)
    except (ValueError, TypeError):
        logging.warning(f"Error converting price: {price}")
        return 0.0

def validate_product_data(data: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Valida que un producto tenga todos los campos requeridos
    
    Args:
        data: Diccionario con datos del producto
        
    Returns:
        Tuple[bool, List[str]]: (es_válido, lista_de_errores)
    """
    required_fields = {
        'REFERENCIA': 'referencia',
        'DESCRIPCION': 'descripción',
        'PRECIO': 'precio',
        'TIPO': 'tipo'
    }
    
    missing_fields = []
    
    for field, name in required_fields.items():
        if field not in data or pd.isna(data[field]) or str(data[field]).strip() == '':
            missing_fields.append(name)
    
    return len(missing_fields) == 0, missing_fields

def group_variants(df: pd.DataFrame) -> Dict[str, Dict]:
    """
    Agrupa los productos y sus variantes por referencia base
    
    Args:
        df: DataFrame con los productos
        
    Returns:
        Dict[str, Dict]: Diccionario con productos agrupados
    """
    products = {}
    
    for _, row in df.iterrows():
        reference = clean_value(row['REFERENCIA'])
        base_reference = get_base_reference(reference)
        
        if base_reference not in products:
            products[base_reference] = {
                'is_variant_product': False,
                'base_data': row,
                'variants': []
            }
        
        if is_variant_reference(reference):
            products[base_reference]['is_variant_product'] = True
            products[base_reference]['variants'].append(row)
        elif len(products[base_reference]['variants']) == 0:
            products[base_reference]['variants'].append(row)
    
    return products

def format_title(reference: str, title: str) -> str:
    """
    Formatea el título del producto incluyendo la referencia base
    
    Args:
        reference (str): Referencia del producto
        title (str): Título original
        
    Returns:
        str: Título formateado
    """
    base_reference = get_base_reference(reference)
    if not isinstance(title, str):
        return base_reference
    
    formatted_title = re.sub(r'^(18K|9k)\s*', '', title)
    formatted_title = formatted_title.capitalize()
    
    return f"{base_reference} - {formatted_title}"

def process_tags(category: str, subcategory: str, tipo: str) -> str:
    """
    Procesa y combina las etiquetas del producto
    
    Args:
        category (str): Categoría del producto
        subcategory (str): Subcategoría del producto
        tipo (str): Tipo de producto
        
    Returns:
        str: Etiquetas combinadas
    """
    tags = []
    
    for value in [clean_value(v) for v in [category, subcategory]]:
        if value:
            tags.append(value)

    tipo_clean = clean_value(tipo)
    if tipo_clean:
        tipo_norm = tipo_clean.strip().capitalize()
        if tipo_norm in ["Solitario", "Alianza", "Sello"]:
            tags.append(f"{tipo_norm}s")
    
    return ", ".join(filter(None, tags))

def log_processing_stats(start_time: datetime, processed: int, failed: int):
    """
    Registra estadísticas del procesamiento
    
    Args:
        start_time (datetime): Tiempo de inicio
        processed (int): Productos procesados
        failed (int): Productos fallidos
    """
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logging.info("="*40)
    logging.info("RESUMEN DE PROCESAMIENTO")
    logging.info("="*40)
    logging.info(f"Total productos procesados: {processed + failed}")
    logging.info(f"Productos exitosos: {processed}")
    logging.info(f"Productos con errores: {failed}")
    logging.info(f"Tiempo total: {duration:.2f} segundos")
    if processed > 0:
        logging.info(f"Tiempo promedio por producto: {(duration/processed):.2f} segundos")
    logging.info("="*40)

def format_log_message(product_ref: str, message: str, error: bool = False) -> str:
    """
    Formatea un mensaje de log
    
    Args:
        product_ref (str): Referencia del producto
        message (str): Mensaje
        error (bool): Si es un mensaje de error
        
    Returns:
        str: Mensaje formateado
    """
    prefix = "ERROR" if error else "INFO"
    return f"[{prefix}] [{product_ref}] {message}"