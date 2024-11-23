"""
Clase para manejar el mapeo entre productos locales y Shopify
"""
from typing import Dict, Optional, List, Any
from datetime import datetime
import logging
from .mysql_connector import MySQLConnector
import shopify

class ProductMapper(MySQLConnector):
    """
    Clase para manejar el mapeo entre productos locales y Shopify.
    Hereda de MySQLConnector para la funcionalidad de base de datos.
    """

    def save_product_mapping(self, internal_reference: str, shopify_product) -> bool:
        """
        Guarda o actualiza el mapeo de un producto
        
        Args:
            internal_reference (str): Referencia interna del producto
            shopify_product: Objeto producto de Shopify
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        try:
            query = """
                INSERT INTO product_mappings 
                (internal_reference, shopify_product_id, shopify_handle, title) 
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    shopify_product_id = VALUES(shopify_product_id),
                    shopify_handle = VALUES(shopify_handle),
                    title = VALUES(title),
                    last_updated_at = CURRENT_TIMESTAMP
            """
            
            self.execute_query(query, (
                internal_reference,
                shopify_product.id,
                shopify_product.handle,
                shopify_product.title
            ))
            
            self._log_sync(
                internal_reference=internal_reference,
                action='create_product',
                status='success',
                message=f'Product mapped successfully. Shopify ID: {shopify_product.id}'
            )
            
            return True
            
        except Exception as e:
            self._log_sync(
                internal_reference=internal_reference,
                action='create_product',
                status='error',
                message=str(e)
            )
            logging.error(f"Error saving product mapping: {e}")
            return False

    def save_variant_mapping(self, internal_sku: str, variant, parent_reference: str, 
                           size: str = None, price: float = None) -> bool:
        """
        Guarda o actualiza el mapeo de una variante
        
        Args:
            internal_sku (str): SKU interno de la variante
            variant: Objeto variante de Shopify
            parent_reference (str): Referencia del producto padre
            size (str, optional): Talla de la variante
            price (float, optional): Precio de la variante
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        try:
            query = """
                INSERT INTO variant_mappings 
                (internal_sku, shopify_variant_id, shopify_product_id, parent_reference, size, price) 
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    shopify_variant_id = VALUES(shopify_variant_id),
                    shopify_product_id = VALUES(shopify_product_id),
                    size = VALUES(size),
                    price = VALUES(price),
                    last_updated_at = CURRENT_TIMESTAMP
            """
            
            self.execute_query(query, (
                internal_sku,
                variant.id,
                variant.product_id,
                parent_reference,
                size,
                price or variant.price
            ))
            
            self._log_sync(
                internal_reference=internal_sku,
                action='create_variant',
                status='success',
                message=f'Variant mapped successfully. Shopify ID: {variant.id}'
            )
            
            return True
            
        except Exception as e:
            self._log_sync(
                internal_reference=internal_sku,
                action='create_variant',
                status='error',
                message=str(e)
            )
            logging.error(f"Error saving variant mapping: {e}")
            return False

    def get_product_mapping(self, internal_reference: str) -> Optional[Dict]:
        """
        Obtiene el mapeo completo de un producto y sus variantes
        
        Args:
            internal_reference (str): Referencia interna del producto
            
        Returns:
            Optional[Dict]: Diccionario con la información del producto y sus variantes,
                          o None si no se encuentra
        """
        try:
            # Obtener producto
            product_query = "SELECT * FROM product_mappings WHERE internal_reference = %s"
            products = self.execute_query(product_query, (internal_reference,), fetch=True)
            
            if not products:
                return None
                
            product = products[0]
            
            # Obtener variantes
            variants_query = "SELECT * FROM variant_mappings WHERE parent_reference = %s"
            variants = self.execute_query(variants_query, (internal_reference,), fetch=True)
            
            return {
                'product': product,
                'variants': variants or []
            }
            
        except Exception as e:
            logging.error(f"Error getting product mapping: {e}")
            return None

    def get_variant_mapping(self, internal_sku: str) -> Optional[Dict]:
        """
        Obtiene el mapeo de una variante específica
        
        Args:
            internal_sku (str): SKU interno de la variante
            
        Returns:
            Optional[Dict]: Información de la variante o None si no se encuentra
        """
        try:
            query = "SELECT * FROM variant_mappings WHERE internal_sku = %s"
            variants = self.execute_query(query, (internal_sku,), fetch=True)
            return variants[0] if variants else None
        except Exception as e:
            logging.error(f"Error getting variant mapping: {e}")
            return None

    def delete_product_mapping(self, internal_reference: str) -> bool:
        """
        Elimina un producto y sus variantes del mapeo
        
        Args:
            internal_reference (str): Referencia interna del producto
            
        Returns:
            bool: True si se eliminó correctamente, False en caso contrario
        """
        try:
            # La eliminación en cascada manejará las variantes
            query = "DELETE FROM product_mappings WHERE internal_reference = %s"
            self.execute_query(query, (internal_reference,))
            
            self._log_sync(
                internal_reference=internal_reference,
                action='delete_product',
                status='success',
                message='Product mapping deleted successfully'
            )
            
            return True
        except Exception as e:
            self._log_sync(
                internal_reference=internal_reference,
                action='delete_product',
                status='error',
                message=str(e)
            )
            logging.error(f"Error deleting product mapping: {e}")
            return False

    def _log_sync(self, internal_reference: str, action: str, status: str, message: str) -> None:
        """
        Registra una acción de sincronización en el log
        
        Args:
            internal_reference (str): Referencia del producto/variante
            action (str): Tipo de acción realizada
            status (str): Estado de la acción (success/error)
            message (str): Mensaje descriptivo
        """
        query = """
            INSERT INTO sync_log 
            (internal_reference, action, status, message) 
            VALUES (%s, %s, %s, %s)
        """
        try:
            self.execute_query(query, (internal_reference, action, status, message))
        except Exception as e:
            logging.error(f"Error logging sync action: {e}")

    def get_sync_history(self, internal_reference: str, limit: int = 10) -> List[Dict]:
        """
        Obtiene el historial de sincronización de un producto
        
        Args:
            internal_reference (str): Referencia del producto
            limit (int): Número máximo de registros a retornar
            
        Returns:
            List[Dict]: Lista de registros de sincronización
        """
        query = """
            SELECT * FROM sync_log 
            WHERE internal_reference = %s 
            ORDER BY created_at DESC 
            LIMIT %s
        """
        try:
            return self.execute_query(query, (internal_reference, limit), fetch=True) or []
        except Exception as e:
            logging.error(f"Error getting sync history: {e}")
            return []   