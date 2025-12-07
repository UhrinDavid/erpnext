"""
SAX-based XML Parser with Redis Queue for ERPNext XML Importer
Memory-efficient streaming XML parser that queues individual items for processing

This replaces the ElementTree DOM parser to handle large XML files without memory overload.
Individual XML items are parsed as they're encountered and queued in Redis for background processing.

Author: Herbatica
License: MIT
"""

import xml.sax
import redis
import json
import frappe
from typing import Dict, Any, Optional, Callable
import uuid
from datetime import datetime


class SAXItemHandler(xml.sax.ContentHandler):
    """
    SAX Content Handler for parsing XML items and queuing them in Redis

    Streams through XML file and extracts individual SHOPITEM elements,
    converting them to dictionaries and queuing for background processing.
    """

    def __init__(self, redis_client, queue_name: str, item_processor: Optional[Callable] = None):
        """
        Initialize SAX handler

        Args:
            redis_client: Redis connection
            queue_name: Redis queue name for items
            item_processor: Optional function to call immediately for each item
        """
        super().__init__()
        self.redis_client = redis_client
        self.queue_name = queue_name
        self.item_processor = item_processor

        # Parser state
        self.current_element = None
        self.current_data = ""
        self.current_item = {}
        self.element_stack = []
        self.in_shopitem = False
        self.in_categories = False
        self.in_images = False
        self.in_stock = False
        self.in_logistic = False
        self.in_text_properties = False
        self.in_related_products = False
        self.current_category = {}
        self.current_image = {}
        self.current_supplier = {}
        self.current_text_property = {}

        # Counters
        self.items_queued = 0
        self.items_processed = 0
        self.parse_errors = 0

        # Progress tracking
        self.last_progress_update = datetime.now()

    def startElement(self, name: str, attrs):
        """Handle start of XML element"""
        self.element_stack.append(name)
        self.current_element = name
        self.current_data = ""

        if name == "SHOPITEM":
            self.in_shopitem = True
            self.current_item = {
                "external_id": attrs.get("id", ""),
                "import_code": attrs.get("import-code", ""),
                "product_categories": [],
                "product_images": [],
                "custom_attributes": [],
                "related_product_codes": []
            }

        elif self.in_shopitem:
            if name == "CATEGORIES":
                self.in_categories = True
            elif name == "CATEGORY" and self.in_categories:
                self.current_category = {
                    "category_id": attrs.get("id", ""),
                    "category_name": ""
                }
            elif name == "DEFAULT_CATEGORY" and self.in_categories:
                # Store DEFAULT_CATEGORY id for reference
                self.current_item["default_category_id"] = attrs.get("id", "")
            elif name == "IMAGES":
                self.in_images = True
            elif name == "IMAGE" and self.in_images:
                self.current_image = {
                    "image_url": "",
                    "image_description": attrs.get("description", "")
                }
            elif name == "STOCK":
                self.in_stock = True
            elif name == "LOGISTIC":
                self.in_logistic = True
            elif name == "TEXT_PROPERTIES":
                self.in_text_properties = True
            elif name == "TEXT_PROPERTY" and self.in_text_properties:
                self.current_text_property = {
                    "attribute_name": "",
                    "attribute_value": "",
                    "attribute_description": ""
                }
            elif name == "RELATED_PRODUCTS":
                self.in_related_products = True
            elif name == "SUPPLIER":
                self.current_supplier = {
                    "supplier_id": attrs.get("id", ""),
                    "supplier_name": ""
                }

    def characters(self, content: str):
        """Handle character data between XML tags"""
        self.current_data += content.strip()

    def endElement(self, name: str):
        """Handle end of XML element"""
        if name == "SHOPITEM" and self.in_shopitem:
            # Post-process item data before queuing
            self.finalize_item_data()

            # Complete item parsed - queue it for processing
            self.queue_item(self.current_item)
            self.current_item = {}
            self.in_shopitem = False
            # Reset all state variables
            self.in_categories = False
            self.in_images = False
            self.in_stock = False
            self.in_logistic = False
            self.in_text_properties = False
            self.in_related_products = False
            self.items_queued += 1

            # Update progress periodically
            self.update_progress()

        elif self.in_shopitem:
            # Handle nested structure closings first
            if name == "CATEGORIES":
                self.in_categories = False
            elif name == "IMAGES":
                self.in_images = False
            elif name == "STOCK":
                self.in_stock = False
            elif name == "LOGISTIC":
                self.in_logistic = False
            elif name == "TEXT_PROPERTIES":
                self.in_text_properties = False
            elif name == "RELATED_PRODUCTS":
                self.in_related_products = False

            # Handle stock fields
            elif name == "AMOUNT" and self.in_stock:
                self.current_item["current_stock"] = self.current_data
            elif name == "MINIMAL_AMOUNT" and self.in_stock:
                self.current_item["minimum_stock"] = self.current_data
            elif name == "MAXIMAL_AMOUNT" and self.in_stock:
                self.current_item["maximum_stock"] = self.current_data

            # Handle logistic fields
            elif name == "WEIGHT" and self.in_logistic:
                self.current_item["weight_kg"] = self.current_data

            # Handle text properties (custom attributes)
            elif name == "NAME" and self.in_text_properties:
                if not hasattr(self, 'current_text_property'):
                    self.current_text_property = {}
                self.current_text_property["attribute_name"] = self.current_data
            elif name == "VALUE" and self.in_text_properties:
                if not hasattr(self, 'current_text_property'):
                    self.current_text_property = {}
                self.current_text_property["attribute_value"] = self.current_data
            elif name == "DESCRIPTION" and self.in_text_properties:
                if not hasattr(self, 'current_text_property'):
                    self.current_text_property = {}
                self.current_text_property["attribute_description"] = self.current_data
            elif name == "TEXT_PROPERTY" and self.in_text_properties:
                if hasattr(self, 'current_text_property') and (self.current_text_property.get("attribute_name") or self.current_text_property.get("attribute_value")):
                    self.current_item["custom_attributes"].append(self.current_text_property.copy())
                self.current_text_property = {}

            # Handle related products
            elif name == "CODE" and self.in_related_products:
                if self.current_data:
                    self.current_item["related_product_codes"].append(self.current_data)

            # Handle category completion
            elif name == "CATEGORY" and self.in_categories:
                self.current_category["category_name"] = self.current_data
                self.current_item["product_categories"].append(self.current_category.copy())
                self.current_category = {}

            # Handle image completion
            elif name == "IMAGE" and self.in_images:
                if self.current_data:
                    self.current_image["image_url"] = self.current_data
                self.current_item["product_images"].append(self.current_image.copy())
                self.current_image = {}

            # Handle structured supplier completion
            elif name == "SUPPLIER" and hasattr(self, 'current_supplier') and self.current_supplier.get("supplier_id"):
                self.current_supplier["supplier_name"] = self.current_data
                if "suppliers" not in self.current_item:
                    self.current_item["suppliers"] = []
                self.current_item["suppliers"].append(self.current_supplier.copy())
                self.current_supplier = {}

            # Handle top-level item fields (only if not in nested contexts)
            elif name == "NAME" and not self.in_text_properties:
                self.current_item["item_name"] = self.current_data
            elif name == "CODE" and not self.in_related_products:
                self.current_item["item_code"] = self.current_data
            elif name == "DESCRIPTION" and not self.in_text_properties:
                self.current_item["description"] = self.current_data
            elif name == "GUID":
                self.current_item["guid"] = self.current_data
            elif name == "SHORT_DESCRIPTION":
                self.current_item["short_description"] = self.current_data
            elif name == "MANUFACTURER":
                self.current_item["manufacturer_name"] = self.current_data
            elif name == "SUPPLIER" and not self.current_supplier.get("supplier_id"):
                # Simple supplier name (not structured)
                self.current_item["supplier_name"] = self.current_data
            elif name == "CURRENCY":
                self.current_item["currency_code"] = self.current_data
            elif name == "PRICE_VAT":
                self.current_item["selling_price_with_tax"] = self.current_data
            elif name == "PURCHASE_PRICE":
                self.current_item["purchase_price"] = self.current_data
            elif name == "VAT":
                self.current_item["tax_rate"] = self.current_data
            elif name == "EAN":
                self.current_item["barcode"] = self.current_data
            elif name == "UNIT":
                self.current_item["unit_of_measure"] = self.current_data
            elif name == "VISIBLE":
                self.current_item["is_published"] = self.current_data
            elif name == "ITEM_TYPE":
                self.current_item["product_type"] = self.current_data
            elif name == "DEFAULT_CATEGORY":
                self.current_item["default_category"] = self.current_data
            elif name == "SEO_TITLE":
                self.current_item["seo_page_title"] = self.current_data
            elif name == "META_DESCRIPTION":
                self.current_item["seo_meta_description"] = self.current_data
            # Legacy fields for backward compatibility
            elif name == "PRODUCT":
                self.current_item["product_name"] = self.current_data
            elif name == "PRODUCTNAME":
                self.current_item["product_name"] = self.current_data
            elif name == "URL":
                self.current_item["url"] = self.current_data
            elif name == "IMGURL":
                self.current_item["image_url"] = self.current_data
            elif name == "PRICE":
                self.current_item["price"] = self.current_data
            elif name == "PRODUCTNO":
                self.current_item["product_code"] = self.current_data
            elif name == "AVAILABILITY":
                self.current_item["availability"] = self.current_data
            elif name == "STOCK_QUANTITY":
                self.current_item["stock_quantity"] = self.current_data
            elif name == "WEIGHT" and not self.in_logistic:
                # Only handle WEIGHT if not inside LOGISTIC (to avoid duplication)
                self.current_item["weight_kg"] = self.current_data
            else:
                # Store any other unmapped data for debugging
                if self.current_data and name not in ["SHOPITEM", "CATEGORIES", "IMAGES", "STOCK", "LOGISTIC", "TEXT_PROPERTIES", "RELATED_PRODUCTS"]:
                    if "additional_data" not in self.current_item:
                        self.current_item["additional_data"] = {}
                    self.current_item["additional_data"][name.lower()] = self.current_data

        # Pop from stack
        if self.element_stack:
            self.element_stack.pop()

        # Reset current data
        self.current_data = ""
        self.current_element = self.element_stack[-1] if self.element_stack else None

    def finalize_item_data(self):
        """Post-process item data to ensure required fields and fallbacks"""
        try:
            # Ensure item_code exists - fallback to external_id if CODE is empty
            if not self.current_item.get("item_code") or not self.current_item.get("item_code").strip():
                self.current_item["item_code"] = self.current_item.get("external_id", "")

            # Ensure item_name exists - clean up if needed
            if not self.current_item.get("item_name") or not self.current_item.get("item_name").strip():
                # Fallback to product_name or item_code
                self.current_item["item_name"] = (
                    self.current_item.get("product_name") or
                    self.current_item.get("item_code") or
                    "Unknown Item"
                )

            # Clean item_name - remove quotes and extra whitespace
            if self.current_item.get("item_name"):
                item_name = self.current_item["item_name"].strip()
                # Remove surrounding quotes
                if item_name.startswith('"') and item_name.endswith('"'):
                    item_name = item_name[1:-1]
                self.current_item["item_name"] = item_name.strip()

            # Debug logging
            frappe.logger().debug(
                f"Finalized item: ID={self.current_item.get('external_id')}, "
                f"Code={self.current_item.get('item_code')}, "
                f"Name={self.current_item.get('item_name')}, "
                f"Default Category={self.current_item.get('default_category')}, "
                f"Categories={len(self.current_item.get('product_categories', []))}"
            )

        except Exception as e:
            frappe.logger().error(f"Error finalizing item data: {str(e)}")

    def queue_item(self, item_data: Dict[str, Any]):
        """Queue item for background processing"""
        try:
            # Generate unique ID for this queue entry
            queue_id = str(uuid.uuid4())

            # Add metadata
            queue_entry = {
                "id": queue_id,
                "timestamp": datetime.now().isoformat(),
                "item_data": item_data,
                "status": "queued"
            }

            # Push to Redis queue
            self.redis_client.lpush(
                self.queue_name,
                json.dumps(queue_entry, ensure_ascii=False)
            )

            # If immediate processor provided, call it
            if self.item_processor:
                try:
                    self.item_processor(item_data)
                    self.items_processed += 1
                except Exception as e:
                    frappe.log_error(
                        f"Immediate processing error for item {item_data.get('item_id', 'Unknown')}: {str(e)}",
                        "SAX Item Processing Error"
                    )
                    self.parse_errors += 1

        except Exception as e:
            frappe.log_error(
                f"Failed to queue item {item_data.get('item_id', 'Unknown')}: {str(e)}",
                "SAX Queue Error"
            )
            self.parse_errors += 1

    def update_progress(self):
        """Update progress in realtime for UI feedback"""
        now = datetime.now()
        if (now - self.last_progress_update).seconds >= 2:  # Update every 2 seconds
            frappe.publish_realtime(
                "sax_parse_progress",
                {
                    "items_queued": self.items_queued,
                    "items_processed": self.items_processed,
                    "parse_errors": self.parse_errors,
                    "message": f"Parsed {self.items_queued} items, processed {self.items_processed}"
                },
                user=frappe.session.user
            )
            self.last_progress_update = now

    def get_stats(self) -> Dict[str, int]:
        """Get parsing statistics"""
        return {
            "items_queued": self.items_queued,
            "items_processed": self.items_processed,
            "parse_errors": self.parse_errors
        }


class SAXOrderHandler(xml.sax.ContentHandler):
    """
    SAX Content Handler for parsing XML orders and queuing them in Redis

    Similar to SAXItemHandler but for ORDER elements
    """

    def __init__(self, redis_client, queue_name: str, order_processor: Optional[Callable] = None):
        """Initialize SAX order handler"""
        super().__init__()
        self.redis_client = redis_client
        self.queue_name = queue_name
        self.order_processor = order_processor

        # Parser state
        self.current_element = None
        self.current_data = ""
        self.current_order = {}
        self.current_item = {}
        self.element_stack = []
        self.in_order = False
        self.in_order_item = False

        # Counters
        self.orders_queued = 0
        self.orders_processed = 0
        self.parse_errors = 0

        # Progress tracking
        self.last_progress_update = datetime.now()

    def startElement(self, name: str, attrs):
        """Handle start of XML element"""
        self.element_stack.append(name)
        self.current_element = name
        self.current_data = ""

        if name == "ORDER":
            self.in_order = True
            self.current_order = {
                "order_id": attrs.get("id", ""),
                "items": [],
                "customer_data": {},
                "additional_data": {}
            }

        elif name == "ORDERITEM" and self.in_order:
            self.in_order_item = True
            self.current_item = {
                "item_id": attrs.get("id", ""),
                "quantity": attrs.get("quantity", "1")
            }

    def characters(self, content: str):
        """Handle character data between XML tags"""
        self.current_data += content.strip()

    def endElement(self, name: str):
        """Handle end of XML element"""
        if name == "ORDER" and self.in_order:
            # Complete order parsed - queue it
            self.queue_order(self.current_order)
            self.current_order = {}
            self.in_order = False
            self.orders_queued += 1
            self.update_progress()

        elif name == "ORDERITEM" and self.in_order_item:
            # Complete order item
            self.current_order["items"].append(self.current_item.copy())
            self.current_item = {}
            self.in_order_item = False

        elif self.in_order and not self.in_order_item:
            # Handle order-level fields
            if name == "ORDER_NUMBER":
                self.current_order["order_number"] = self.current_data
            elif name == "ORDER_DATE":
                self.current_order["order_date"] = self.current_data
            elif name == "CUSTOMER_NAME":
                self.current_order["customer_data"]["customer_name"] = self.current_data
            elif name == "CUSTOMER_EMAIL":
                self.current_order["customer_data"]["email"] = self.current_data
            elif name == "DELIVERY_ADDRESS":
                self.current_order["customer_data"]["delivery_address"] = self.current_data
            else:
                # Store other data
                if self.current_data:
                    self.current_order["additional_data"][name.lower()] = self.current_data

        elif self.in_order_item:
            # Handle order item fields
            if name == "PRODUCT_NAME":
                self.current_item["product_name"] = self.current_data
            elif name == "UNIT_PRICE":
                self.current_item["unit_price"] = self.current_data
            elif name == "TOTAL_PRICE":
                self.current_item["total_price"] = self.current_data
            else:
                if self.current_data:
                    if "additional_data" not in self.current_item:
                        self.current_item["additional_data"] = {}
                    self.current_item["additional_data"][name.lower()] = self.current_data

        # Pop from stack
        if self.element_stack:
            self.element_stack.pop()

        # Reset current data
        self.current_data = ""
        self.current_element = self.element_stack[-1] if self.element_stack else None

    def queue_order(self, order_data: Dict[str, Any]):
        """Queue order for background processing"""
        try:
            queue_id = str(uuid.uuid4())

            queue_entry = {
                "id": queue_id,
                "timestamp": datetime.now().isoformat(),
                "order_data": order_data,
                "status": "queued"
            }

            self.redis_client.lpush(
                self.queue_name,
                json.dumps(queue_entry, ensure_ascii=False)
            )

            if self.order_processor:
                try:
                    self.order_processor(order_data)
                    self.orders_processed += 1
                except Exception as e:
                    frappe.log_error(
                        f"Immediate processing error for order {order_data.get('order_id', 'Unknown')}: {str(e)}",
                        "SAX Order Processing Error"
                    )
                    self.parse_errors += 1

        except Exception as e:
            frappe.log_error(
                f"Failed to queue order {order_data.get('order_id', 'Unknown')}: {str(e)}",
                "SAX Order Queue Error"
            )
            self.parse_errors += 1

    def update_progress(self):
        """Update progress in realtime"""
        now = datetime.now()
        if (now - self.last_progress_update).seconds >= 2:
            frappe.publish_realtime(
                "sax_parse_progress",
                {
                    "orders_queued": self.orders_queued,
                    "orders_processed": self.orders_processed,
                    "parse_errors": self.parse_errors,
                    "message": f"Parsed {self.orders_queued} orders, processed {self.orders_processed}"
                },
                user=frappe.session.user
            )
            self.last_progress_update = now

    def get_stats(self) -> Dict[str, int]:
        """Get parsing statistics"""
        return {
            "orders_queued": self.orders_queued,
            "orders_processed": self.orders_processed,
            "parse_errors": self.parse_errors
        }


def get_redis_client() -> redis.Redis:
    """Get Redis client connection"""
    try:
        # Get Redis configuration from Frappe
        redis_config = frappe.conf.get("redis_queue") or frappe.conf.get("redis_cache")

        if isinstance(redis_config, str):
            # Simple URL format
            return redis.from_url(redis_config)
        elif isinstance(redis_config, dict):
            # Dictionary format
            return redis.Redis(**redis_config)
        else:
            # Default localhost connection
            return redis.Redis(host='localhost', port=6379, db=0)

    except Exception as e:
        frappe.log_error(f"Redis connection error: {str(e)}")
        # Fallback to default connection
        return redis.Redis(host='localhost', port=6379, db=0)


def parse_xml_with_sax(xml_content: str, content_type: str = "items",
                      immediate_processor: Optional[Callable] = None) -> Dict[str, Any]:
    """
    Parse XML content using SAX parser with Redis queue

    Args:
        xml_content: Raw XML content
        content_type: "items" or "orders"
        immediate_processor: Optional function to process items/orders immediately

    Returns:
        Dict with parsing statistics
    """
    try:
        # Get Redis connection
        redis_client = get_redis_client()

        # Generate unique queue name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        queue_name = f"xml_import_{content_type}_{timestamp}"

        # Create appropriate handler
        if content_type == "items":
            handler = SAXItemHandler(redis_client, queue_name, immediate_processor)
        elif content_type == "orders":
            handler = SAXOrderHandler(redis_client, queue_name, immediate_processor)
        else:
            raise ValueError(f"Invalid content_type: {content_type}")

        # Create parser and parse
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        # Parse from string
        from io import StringIO
        parser.parse(StringIO(xml_content))

        # Return statistics
        stats = handler.get_stats()
        stats["queue_name"] = queue_name
        stats["success"] = True

        return stats

    except Exception as e:
        frappe.log_error(f"SAX parsing error: {str(e)}", "SAX Parser Error")
        return {
            "success": False,
            "error": str(e),
            "items_queued": 0,
            "items_processed": 0,
            "parse_errors": 1
        }
