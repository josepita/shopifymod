#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script principal de sincronización de productos con Shopify
Soporta productos simples y con variantes de talla
Maneja múltiples formatos de archivo (XLS, XLSX, CSV)
"""

import pandas as pd
import sys
import os
from typing import Dict, List, Optional, Tuple
import shopify
import time
from datetime import datetime
import logging
from pathlib import Path

# Importaciones locales
from config.settings import MYSQL_CONFIG, SHOPIFY_ACCESS_TOKEN, SHOPIFY_API_VERSION, SHOPIFY_SHOP_URL
from db.product_mapper import ProductMapper
from utils.helpers import (
    clean_value, format_price, validate_product_data, group_variants,
    format_title, process_tags, log_processing_stats, format_log_message,
    get_variant_size  # Añadimos esta importación
)

def load_data(input_file: str) -> Optional[pd.DataFrame]:
    """
    Carga los datos desde un archivo Excel, HTML o CSV
    
    Args:
        input_file: Ruta del archivo a cargar
        
    Returns:
        Optional[pd.DataFrame]: DataFrame con los datos o None si hay error
    """
    print(f"\nIntentando cargar archivo: {input_file}")
    
    # Intentar como CSV primero
    try:
        # Probar diferentes encodings comunes
        encodings = ['utf-8', 'latin1', 'iso-8859-1']
        df = None
        
        for encoding in encodings:
            try:
                # Intentar con diferentes separadores comunes
                for separator in [',', ';', '\t']:
                    try:
                        df = pd.read_csv(input_file, encoding=encoding, sep=separator)
                        if len(df.columns) > 1:  # Verificar que se separó correctamente
                            logging.info(f"Archivo cargado como CSV (encoding: {encoding}, separador: {separator})")
                            df.columns = df.columns.str.strip()
                            logging.info(f"Columnas encontradas: {df.columns.tolist()}")
                            return df
                    except:
                        continue
            except:
                continue
                
        if df is None:
            logging.warning("No es un archivo CSV válido o el formato no es reconocido")
    except Exception as e:
        logging.error(f"Error al intentar leer como CSV: {str(e)}")

    # Intentar como Excel xlsx
    try:
        df = pd.read_excel(input_file, engine='openpyxl')
        logging.info("Archivo cargado como Excel XLSX")
        df.columns = df.columns.str.strip()
        logging.info(f"Columnas encontradas: {df.columns.tolist()}")
        return df
    except Exception as e:
        logging.warning(f"No es un archivo XLSX válido: {str(e)}")

    # Intentar como Excel xls
    try:
        df = pd.read_excel(input_file, engine='xlrd')
        logging.info("Archivo cargado como Excel XLS")
        df.columns = df.columns.str.strip()
        logging.info(f"Columnas encontradas: {df.columns.tolist()}")
        return df
    except Exception as e:
        logging.warning(f"No es un archivo XLS válido: {str(e)}")

    # Si llegamos aquí, no pudimos cargar el archivo
    logging.error(f"No se pudo cargar el archivo {input_file} en ningún formato soportado")
    return None

###########################################
# CONFIGURACIÓN DE SHOPIFY
###########################################

def setup_shopify_api() -> bool:
    """
    Configura la conexión con la API de Shopify
    
    Returns:
        bool: True si la conexión fue exitosa
    """
    try:
        logging.info("Iniciando configuración de API Shopify...")
        shop_url = SHOPIFY_SHOP_URL.replace('https://', '').replace('http://', '')
        api_url = f"https://{shop_url}/admin/api/{SHOPIFY_API_VERSION}"
        
        shopify.ShopifyResource.set_site(api_url)
        shopify.ShopifyResource.set_headers({
            'X-Shopify-Access-Token': SHOPIFY_ACCESS_TOKEN
        })
        
        shop = shopify.Shop.current()
        logging.info(f"Conexión exitosa con la tienda: {shop.name}")
        return True
        
    except Exception as e:
        logging.error(f"Error de configuración Shopify: {e}")
        return False

def get_location_id() -> str:
    """
    Obtiene el ID de la ubicación principal de Shopify
    
    Returns:
        str: ID de la ubicación
    """
    locations = shopify.Location.find()
    if not locations:
        raise Exception("No se encontró ubicación para el inventario")
    return locations[0].id

###########################################
# FUNCIONES DE CREACIÓN DE PRODUCTOS
###########################################

def create_simple_product(
    product_data: Dict, 
    product_mapper: ProductMapper,
    location_id: str
) -> bool:
    """
    Crea un producto simple (sin variantes) en Shopify
    
    Args:
        product_data: Datos del producto
        product_mapper: Instancia del mapper
        location_id: ID de la ubicación de inventario
        
    Returns:
        bool: True si se creó correctamente
    """
    try:
        new_product = shopify.Product()
        new_product.title = product_data['title']
        new_product.body_html = product_data['body_html']
        new_product.vendor = product_data['vendor']
        new_product.product_type = product_data['product_type']
        new_product.tags = product_data['tags']
        new_product.published = True
        
        # Crear única variante
        variant = shopify.Variant({
            'price': product_data['price'],
            'sku': product_data['sku'],
            'inventory_management': 'shopify',
            'inventory_policy': 'deny',
            'grams': int(float(product_data.get('weight', 0))),
            'cost': product_data.get('cost', 0)
        })
        
        new_product.variants = [variant]
        
        if new_product.save():
            # Guardar mapeo del producto
            success = product_mapper.save_product_mapping(
                internal_reference=product_data['sku'],
                shopify_product=new_product
            )
            
            if not success:
                raise Exception("Error guardando mapeo del producto")
            
            # Configurar inventario
            shopify.InventoryLevel.set(
                location_id=location_id,
                inventory_item_id=new_product.variants[0].inventory_item_id,
                available=product_data['stock']
            )
            
            # Crear metafields si existen
            if product_data.get('metafields'):
                create_product_metafields(new_product.id, product_data['metafields'])
            
            # Configurar imágenes
            if product_data.get('images'):
                setup_product_images(new_product.id, product_data['images'])
            
            logging.info(f"Producto simple creado correctamente: {product_data['sku']}")
            return True
        else:
            logging.error(f"Error al crear producto simple: {new_product.errors.full_messages()}")
            return False
            
    except Exception as e:
        logging.error(f"Error creando producto simple: {str(e)}")
        return False

def create_variant_product(
    product_data: Dict, 
    variants_data: List[Dict], 
    product_mapper: ProductMapper,
    location_id: str
) -> bool:
    """
    Crea un producto con variantes en Shopify
    
    Args:
        product_data: Datos del producto base
        variants_data: Lista de datos de las variantes
        product_mapper: Instancia del mapper
        location_id: ID de la ubicación de inventario
        
    Returns:
        bool: True si se creó correctamente
    """
    try:
        new_product = shopify.Product()
        new_product.title = product_data['title']
        new_product.body_html = product_data['body_html']
        new_product.vendor = product_data['vendor']
        new_product.product_type = product_data['product_type']
        new_product.tags = product_data['tags']
        new_product.published = True
        
        # Configurar opción de talla
        tallas = [v['size'] for v in variants_data]
        new_product.options = [{'name': 'Talla', 'values': sorted(list(set(tallas)))}]
        
        # Crear variantes
        variants = []
        for var_data in variants_data:
            variant = shopify.Variant({
                'option1': var_data['size'],
                'price': var_data['price'],
                'sku': var_data['sku'],
                'inventory_management': 'shopify',
                'inventory_policy': 'deny',
                'grams': int(float(var_data.get('weight', 0))),
                'cost': var_data.get('cost', 0)
            })
            variants.append(variant)
            
        new_product.variants = variants
        
        if new_product.save():
            # Guardar mapeo del producto
            success = product_mapper.save_product_mapping(
                internal_reference=product_data['sku'],
                shopify_product=new_product
            )
            
            if not success:
                raise Exception("Error guardando mapeo del producto")
            
            # Guardar mapeos de variantes y configurar inventario
            for variant, var_data in zip(new_product.variants, variants_data):
                # Guardar mapeo de variante
                success = product_mapper.save_variant_mapping(
                    internal_sku=var_data['sku'],
                    variant=variant,
                    parent_reference=product_data['sku'],
                    size=var_data['size'],
                    price=var_data['price']
                )
                
                if not success:
                    raise Exception(f"Error guardando mapeo de variante {var_data['sku']}")
                
                # Configurar inventario de la variante
                shopify.InventoryLevel.set(
                    location_id=location_id,
                    inventory_item_id=variant.inventory_item_id,
                    available=var_data['stock']
                )
            
            # Crear metafields si existen
            if product_data.get('metafields'):
                create_product_metafields(new_product.id, product_data['metafields'])
            
            # Configurar imágenes
            if product_data.get('images'):
                setup_product_images(new_product.id, product_data['images'])
            
            logging.info(f"Producto con variantes creado correctamente: {product_data['sku']}")
            return True
        else:
            logging.error(f"Error al crear producto con variantes: {new_product.errors.full_messages()}")
            return False
            
    except Exception as e:
        logging.error(f"Error creando producto con variantes: {str(e)}")
        return False
    
    ###########################################
# FUNCIONES DE METAFIELDS E IMÁGENES
###########################################

def create_product_metafields(product_id: int, metafields_data: Dict[str, str]) -> None:
    """
    Crea los metafields para un producto
    
    Args:
        product_id: ID del producto en Shopify
        metafields_data: Diccionario con los metafields a crear
    """
    # Mapeo de nombres internos a nombres de Shopify y sus tipos
    field_mapping = {
        'alto': {'key': 'alto', 'type': 'number_decimal'},
        'ancho': {'key': 'ancho', 'type': 'number_decimal'},
        'grosor': {'key': 'grosor', 'type': 'number_decimal'},
        'medidas': {'key': 'medidas', 'type': 'single_line_text_field'},
        'largo': {'key': 'largo', 'type': 'number_decimal'},
        'tipo_piedra': {'key': 'tipo_de_piedra', 'type': 'single_line_text_field'},
        'forma_piedra': {'key': 'forma_piedra', 'type': 'single_line_text_field'},
        'calidad_piedra': {'key': 'calidad_de_la_piedra', 'type': 'single_line_text_field'},
        'color_piedra': {'key': 'color_piedra', 'type': 'single_line_text_field'},
        'disposicion_piedras': {'key': 'disposicion_de_la_piedra', 'type': 'single_line_text_field'},
        'acabado': {'key': 'acabado', 'type': 'single_line_text_field'},
        'estructura': {'key': 'estructura', 'type': 'single_line_text_field'},
        'material': {'key': 'material', 'type': 'single_line_text_field'},
        'destinatario': {'key': 'destinatario', 'type': 'single_line_text_field'},
        'cierre': {'key': 'cierre', 'type': 'single_line_text_field'},
        'color_del_oro': {'key': 'color_del_oro', 'type': 'single_line_text_field'},
        'calidad_diamante': {'key': 'calidad_diamante', 'type': 'single_line_text_field'},
        'quilates': {'key': 'quilates', 'type': 'number_decimal'}
    }

    for internal_key, value in metafields_data.items():
        if value and str(value).strip():
            try:
                field_config = field_mapping.get(internal_key)
                if not field_config:
                    continue
                
                shopify_key = field_config['key']
                field_type = field_config['type']
                
                # Formatear el valor según el tipo
                formatted_value = value
                if field_type == 'number_decimal':
                    formatted_value = str(float(str(value).replace(',', '.')))
                
                metafield = shopify.Metafield({
                    'namespace': 'custom',
                    'key': shopify_key,
                    'value': formatted_value,
                    'type': field_type,
                    'owner_id': product_id,
                    'owner_resource': 'product'
                })
                
                if metafield.save():
                    logging.info(f"Metafield creado: {shopify_key} = {formatted_value}")
                else:
                    logging.error(f"Error al crear metafield {shopify_key}: {metafield.errors.full_messages()}")
                    
            except Exception as e:
                logging.error(f"Error creando metafield {internal_key}: {str(e)}")

def setup_product_images(product_id: int, image_data: List[Dict]) -> None:
    """
    Configura las imágenes del producto
    
    Args:
        product_id: ID del producto en Shopify
        image_data: Lista de diccionarios con datos de imágenes
    """
    for img_data in image_data:
        if img_data.get('src'):
            try:
                image = shopify.Image({
                    'product_id': product_id,
                    'src': img_data['src'],
                    'position': img_data['position'],
                    'alt': img_data.get('alt', '')
                })
                image.save()
            except Exception as e:
                logging.error(f"Error configurando imagen: {str(e)}")

###########################################
# FUNCIÓN PRINCIPAL DE PROCESAMIENTO
###########################################

def process_products(df: pd.DataFrame, display_mode: bool = False) -> None:
    """
    Procesa los productos del DataFrame
    
    Args:
        df: DataFrame con los productos a procesar
        display_mode: Si es True, solo muestra información sin crear productos
    """
    # Inicializar contador y mapper
    products_processed = 0
    products_failed = 0
    product_mapper = ProductMapper(MYSQL_CONFIG)
    start_time = datetime.now()
    
    try:
        # Obtener location_id si no estamos en modo visualización
        location_id = None
        if not display_mode:
            location_id = get_location_id()
        
        # Agrupar productos y variantes
        grouped_products = group_variants(df)
        total_products = len(grouped_products)
        
        logging.info(f"\nTotal de productos a procesar: {total_products}")
        
        for i, (base_reference, product_info) in enumerate(grouped_products.items(), 1):
            try:
                base_row = product_info['base_data']
                descripcion = clean_value(base_row['DESCRIPCION'])
                
                logging.info(f"\n[{i}/{total_products}] Procesando: {base_reference}")
                logging.info(f"Descripción: {descripcion}")
                
                # Validar datos del producto
                is_valid, missing_fields = validate_product_data(base_row)
                if not is_valid:
                    logging.error(f"Campos faltantes: {', '.join(missing_fields)}")
                    products_failed += 1
                    continue
                
                # Preparar datos comunes del producto
                product_data = prepare_product_data(base_row, base_reference)
                
                if product_info['is_variant_product']:
                    if not display_mode:
                        variants_data = prepare_variants_data(product_info['variants'])
                        if create_variant_product(product_data, variants_data, product_mapper, location_id):
                            products_processed += 1
                        else:
                            products_failed += 1
                else:
                    if not display_mode:
                        if create_simple_product(product_data, product_mapper, location_id):
                            products_processed += 1
                        else:
                            products_failed += 1
                
                # Esperar entre productos para evitar límites de API
                if not display_mode:
                    time.sleep(1)
                    
            except Exception as e:
                logging.error(f"Error procesando producto {base_reference}: {str(e)}")
                products_failed += 1
                continue
        
        # Registrar estadísticas
        log_processing_stats(start_time, products_processed, products_failed)
        
    finally:
        product_mapper.close()

###########################################
# FUNCIONES DE PREPARACIÓN DE DATOS
###########################################

def prepare_product_data(base_row: pd.Series, base_reference: str) -> Dict:
    """
    Prepara los datos comunes del producto
    
    Args:
        base_row: Fila del DataFrame con datos base
        base_reference: Referencia base del producto
        
    Returns:
        Dict: Datos del producto preparados
    """
    return {
        'title': format_title(base_reference, base_row['DESCRIPCION']),
        'body_html': clean_value(base_row['DESCRIPCION']),
        'vendor': "Joyas Armaan",
        'product_type': clean_value(base_row['TIPO']).capitalize(),
        'tags': process_tags(
            base_row.get('CATEGORIA', ''),
            base_row.get('SUBCATEGORIA', ''),
            base_row.get('TIPO', '')
        ),
        'sku': base_reference,
        'price': round(float(base_row['PRECIO']) * 2.2, 2),
        'stock': int(base_row['STOCK']),
        'weight': clean_value(base_row.get('PESO G.', 0)),
        'cost': clean_value(base_row['PRECIO']),
        'metafields': {
            'destinatario': clean_value(base_row.get('GENERO', '')).capitalize(),
            'cierre': clean_value(base_row.get('CIERRE', '')).capitalize(),
            'material': get_material(base_row['DESCRIPCION']),
            'color_del_oro': clean_value(base_row.get('COLOR ORO', '')).capitalize()
        },
        'images': prepare_images_data(base_row)
    }

def prepare_variants_data(variants_rows: List[pd.Series]) -> List[Dict]:
    """
    Prepara los datos de las variantes
    
    Args:
        variants_rows: Lista de filas del DataFrame con datos de variantes
        
    Returns:
        List[Dict]: Lista de datos de variantes preparados
    """
    variants_data = []
    for row in variants_rows:
        variant_reference = clean_value(row['REFERENCIA'])
        size = get_variant_size(variant_reference)
        if size:
            variants_data.append({
                'size': size,
                'price': round(float(row['PRECIO']) * 2.2, 2),
                'sku': variant_reference,
                'stock': int(row['STOCK']),
                'weight': clean_value(row.get('PESO G.', 0)),
                'cost': clean_value(row['PRECIO'])
            })
    return variants_data

def prepare_images_data(row: pd.Series) -> List[Dict]:
    """
    Prepara los datos de las imágenes
    
    Args:
        row: Fila del DataFrame con datos de imágenes
        
    Returns:
        List[Dict]: Lista de datos de imágenes preparados
    """
    images = []
    for idx, img_col in enumerate(['IMAGEN 1', 'IMAGEN 2', 'IMAGEN 3'], 1):
        img_src = clean_value(row.get(img_col, ''))
        if img_src:
            if not img_src.startswith(('http://', 'https://')):
                img_src = f"https://{img_src}"
            images.append({
                'src': img_src,
                'position': idx,
                'alt': f"{row.get('DESCRIPCION', '')} - Imagen {idx}"
            })
    return images

def get_material(description: str) -> str:
    """
    Determina el material basado en la descripción
    
    Args:
        description: Descripción del producto
        
    Returns:
        str: Material determinado
    """
    if isinstance(description, str):
        description = description.upper()
        if description.startswith("18K"):
            return "Oro 18 kilates"
        elif description.startswith("9K"):
            return "Oro 9 kilates"
    return ""

###########################################
# FUNCIÓN MAIN
###########################################

def main():
    """Función principal del script"""
    if len(sys.argv) != 3:
        print("""
Uso: python main.py <input_file> <mode>

Argumentos:
  input_file    - Archivo de entrada (Excel XLS/XLSX o CSV)
  mode          - Modo de ejecución:
                  screen-N: Muestra resumen en pantalla de las primeras N líneas
                  api-N: Procesa las primeras N líneas en la API de Shopify

Ejemplos:
  python main.py productos.xlsx screen-10
  python main.py productos.xlsx api-50
        """)
        sys.exit(1)

    input_file = sys.argv[1]
    mode = sys.argv[2]

    # Validar modo de ejecución
    if not (mode.startswith('screen-') or mode.startswith('api-')):
        logging.error("Error: El modo debe comenzar con 'screen-' o 'api-' seguido de un número")
        sys.exit(1)

    try:
        mode_type, num_lines = mode.split('-')
        num_lines = int(num_lines)
        if num_lines <= 0:
            logging.error("Error: El número de líneas debe ser mayor que 0")
            sys.exit(1)
    except ValueError:
        logging.error("Error: Formato de modo inválido")
        sys.exit(1)

    if not os.path.exists(input_file):
        logging.error(f"Error: El archivo {input_file} no existe")
        sys.exit(1)

    try:
        # Cargar datos
        df = load_data(input_file)
        if df is None:
            logging.error("Error: No se pudo cargar el archivo")
            sys.exit(1)

        # Limitar registros según el número especificado
        if num_lines:
            df = df.head(num_lines)

        # Configurar API si no estamos en modo visualización
        if mode_type == 'api':
            if not setup_shopify_api():
                logging.error("Error: No se pudo establecer conexión con Shopify")
                sys.exit(1)

        # Procesar productos
        process_products(
            df=df,
            display_mode=(mode_type == 'screen')
        )

    except Exception as e:
        logging.error(f"Error en la ejecución: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
