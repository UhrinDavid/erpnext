"""
Memory-Optimized XML Item Importer using SAX Parser with Redis Queue
Replaces DOM-based ElementTree parsing with streaming SAX parser

This version processes large XML files without loading entire content into memory,
using SAX parser to stream through XML and queue items in Redis for background processing.

Author: Herbatica
License: MIT
"""

import frappe
import requests
from frappe.utils import now, cstr, flt, cint, strip_html_tags
from typing import Dict, List, Optional, Any
import json
from xml_importer.xml_importer.sax_parser import parse_xml_with_sax, get_redis_client
from xml_importer.xml_importer.queue_processor import RedisQueueProcessor
from xml_importer.xml_importer.item_importer import XMLItemImporter as OriginalXMLItemImporter


class SAXXMLItemImporter(OriginalXMLItemImporter):
    """
    Memory-optimized XML Item Importer using SAX parser with Redis queue

    Inherits all the item processing methods from the original XMLItemImporter
    but replaces the XML parsing approach to use streaming SAX parser instead of DOM.
    """

    def __init__(self, xml_source: str = None, company: str = None, config=None,
                 queue_processor_timeout: int = 600):
        """
        Initialize SAX-based XML Item Importer with Redis queue

        Args:
            xml_source: URL or file path to XML feed
            company: Company name in ERPNext
            config: XML Import Configuration document
            queue_processor_timeout: Timeout for queue processing in seconds
        """
        # Initialize parent class with all existing functionality
        super().__init__(xml_source, company, config)

        # SAX-specific configuration - always use Redis queue
        self.use_queue = True
        self.queue_processor_timeout = queue_processor_timeout
        self.redis_client = get_redis_client()
        self.queue_processor = RedisQueueProcessor(self.redis_client)

        # Statistics
        self.sax_stats = {
            "items_queued": 0,
            "items_processed": 0,
            "parse_errors": 0
        }

    def import_from_xml(self) -> Dict[str, Any]:
        """
        Main import function using SAX parser with Redis queue

        This replaces the original DOM-based approach with memory-efficient streaming.
        """
        try:
            frappe.logger().info(f"Starting SAX-based XML import from: {self.xml_source}")

            # Fetch XML content (same as original)
            xml_content = self.fetch_xml_content()

            # Always use Redis queue for processing
            return self._import_with_queue(xml_content)

        except Exception as e:
            error_msg = f"SAX XML import failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported": self.imported_count,
                "updated": self.updated_count,
                "errors": self.error_count
            }

    def _import_with_queue(self, xml_content: str) -> Dict[str, Any]:
        """Import using SAX parser with Redis queue for background processing"""
        try:
            # Phase 1: Parse XML and queue items using SAX
            frappe.publish_realtime(
                "sax_import_progress",
                {"phase": "parsing", "message": "Parsing XML and queueing items..."},
                user=frappe.session.user
            )

            sax_result = parse_xml_with_sax(
                xml_content=xml_content,
                content_type="items",
                immediate_processor=None  # No immediate processing, use queue only
            )

            if not sax_result.get("success", False):
                return {
                    "success": False,
                    "error": f"SAX parsing failed: {sax_result.get('error', 'Unknown error')}",
                    "imported": 0,
                    "updated": 0,
                    "errors": 1
                }

            self.sax_stats.update(sax_result)
            queue_name = sax_result["queue_name"]

            frappe.logger().info(f"SAX parsing completed: {sax_result['items_queued']} items queued in {queue_name}")

            # Phase 2: Process queued items
            frappe.publish_realtime(
                "sax_import_progress",
                {
                    "phase": "processing",
                    "message": f"Processing {sax_result['items_queued']} queued items...",
                    "items_queued": sax_result['items_queued']
                },
                user=frappe.session.user
            )

            # Process the queue
            process_result = self.queue_processor.process_item_queue(
                queue_name=queue_name,
                company=self.company,
                timeout=self.queue_processor_timeout
            )

            if not process_result.get("success", False):
                return {
                    "success": False,
                    "error": f"Queue processing failed: {process_result.get('error', 'Unknown error')}",
                    "imported": process_result.get("processed", 0),
                    "updated": 0,
                    "errors": process_result.get("errors", 1),
                    "queue_name": queue_name
                }

            # Update counters from queue processor results
            self.imported_count = process_result["processed"]
            self.error_count = process_result["errors"]

            # Phase 3: Complete
            frappe.publish_realtime(
                "sax_import_progress",
                {
                    "phase": "complete",
                    "message": "Import completed successfully",
                    "completed": True
                },
                user=frappe.session.user
            )

            # Return comprehensive summary
            summary = {
                "success": True,
                "imported": self.imported_count,
                "updated": 0,  # SAX approach doesn't distinguish updates vs creates in current version
                "errors": self.error_count,
                "error_messages": process_result.get("error_details", []),
                "total_processed": sax_result["items_queued"],
                "queue_name": queue_name,
                "sax_stats": self.sax_stats,
                "processing_time": process_result.get("processing_time", 0),
                "method": "sax_with_queue"
            }

            frappe.logger().info(f"SAX import completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"SAX import with queue failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported": self.imported_count,
                "updated": 0,
                "errors": self.error_count + 1
            }

    def _import_with_immediate_processing(self, xml_content: str) -> Dict[str, Any]:
        """Import using SAX parser with immediate processing (no queue)"""
        try:
            frappe.logger().info("Starting SAX import with immediate processing")

            # Define immediate processor function
            def immediate_item_processor(item_data: Dict[str, Any]) -> bool:
                """Process item immediately during SAX parsing"""
                try:
                    success = self.create_or_update_item(item_data)
                    if success:
                        self.imported_count += 1
                    else:
                        self.error_count += 1
                    return success
                except Exception as e:
                    error_msg = f"Error processing item {item_data.get('item_id', 'Unknown')}: {str(e)}"
                    self.add_error(error_msg)
                    self.error_count += 1
                    return False

            # Parse XML with immediate processing
            sax_result = parse_xml_with_sax(
                xml_content=xml_content,
                content_type="items",
                immediate_processor=immediate_item_processor
            )

            if not sax_result.get("success", False):
                return {
                    "success": False,
                    "error": f"SAX parsing failed: {sax_result.get('error', 'Unknown error')}",
                    "imported": self.imported_count,
                    "updated": 0,
                    "errors": self.error_count
                }

            self.sax_stats.update(sax_result)

            # Send completion message
            frappe.publish_realtime(
                "sax_import_progress",
                {
                    "phase": "complete",
                    "message": "Import completed successfully",
                    "completed": True,
                    "items_processed": self.imported_count,
                    "errors": self.error_count
                },
                user=frappe.session.user
            )

            # Return summary
            summary = {
                "success": True,
                "imported": self.imported_count,
                "updated": 0,
                "errors": self.error_count,
                "error_messages": self.errors[:10],  # First 10 errors
                "total_processed": sax_result["items_queued"],
                "sax_stats": self.sax_stats,
                "method": "sax_immediate"
            }

            frappe.logger().info(f"SAX immediate import completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"SAX immediate import failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported": self.imported_count,
                "updated": 0,
                "errors": self.error_count + 1
            }

    def convert_sax_item_to_legacy_format(self, sax_item_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert SAX-parsed item data to legacy format expected by create_or_update_item

        This ensures compatibility with existing item processing methods.
        """
        try:
            # Handle old SAX format (with item_id, categories, etc.)
            if "item_id" in sax_item_data:
                return self.convert_old_sax_format(sax_item_data)

            # Map new SAX format to the exact format expected by the original parser
            legacy_item_data = {
                # Basic information
                "external_id": sax_item_data.get("external_id", ""),
                "import_code": sax_item_data.get("import_code", ""),
                "item_name": sax_item_data.get("item_name", ""),
                "guid": sax_item_data.get("guid", ""),
                "item_code": sax_item_data.get("item_code", sax_item_data.get("external_id", "")),
                "barcode": sax_item_data.get("barcode", ""),

                # Descriptions
                "description": sax_item_data.get("description", ""),
                "short_description": sax_item_data.get("short_description", ""),

                # Supplier and manufacturer
                "manufacturer_name": sax_item_data.get("manufacturer_name", ""),
                "supplier_name": sax_item_data.get("supplier_name", ""),

                # Pricing information
                "currency_code": sax_item_data.get("currency_code", ""),
                "selling_price_with_tax": sax_item_data.get("selling_price_with_tax", ""),
                "purchase_price": sax_item_data.get("purchase_price", ""),
                "tax_rate": sax_item_data.get("tax_rate", ""),

                # Stock and logistics
                "current_stock": sax_item_data.get("current_stock", ""),
                "minimum_stock": sax_item_data.get("minimum_stock", ""),
                "maximum_stock": sax_item_data.get("maximum_stock", ""),
                "weight_kg": sax_item_data.get("weight_kg", ""),
                "unit_of_measure": sax_item_data.get("unit_of_measure", "Nos"),

                # Visibility and classification
                "is_published": sax_item_data.get("is_published", ""),
                "product_type": sax_item_data.get("product_type", ""),
                "default_category": sax_item_data.get("default_category", ""),

                # Collections
                "product_categories": sax_item_data.get("product_categories", []),
                "product_images": sax_item_data.get("product_images", []),
                "custom_attributes": sax_item_data.get("custom_attributes", []),
                "related_product_codes": sax_item_data.get("related_product_codes", []),

                # SEO metadata
                "seo_page_title": sax_item_data.get("seo_page_title", ""),
                "seo_meta_description": sax_item_data.get("seo_meta_description", ""),
            }

            # If item_code is empty, use external_id as fallback
            if not legacy_item_data.get("item_code") or not legacy_item_data.get("item_code").strip():
                legacy_item_data["item_code"] = legacy_item_data.get("external_id", "")

            # Ensure we have item_name - use product_name as fallback if needed
            if not legacy_item_data.get("item_name") or not legacy_item_data.get("item_name").strip():
                legacy_item_data["item_name"] = legacy_item_data.get("item_code", "")

            return legacy_item_data

        except Exception as e:
            frappe.log_error(f"Error converting SAX item data: {str(e)}")
            return sax_item_data  # Return as-is if conversion fails

    def convert_old_sax_format(self, old_sax_data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert old SAX format to new format"""
        legacy_item_data = {
            # Map old SAX fields to expected format
            "external_id": old_sax_data.get("item_id", ""),
            "item_code": old_sax_data.get("item_code", old_sax_data.get("item_id", "")),
            "item_name": old_sax_data.get("product_name", old_sax_data.get("item_code", old_sax_data.get("item_id", ""))),
            "description": old_sax_data.get("description", ""),
            "manufacturer_name": old_sax_data.get("manufacturer", ""),
            "barcode": old_sax_data.get("ean", ""),
            "unit_of_measure": old_sax_data.get("unit", "Nos"),
            "selling_price_with_tax": old_sax_data.get("price_vat", ""),
            "tax_rate": old_sax_data.get("vat_rate", ""),
            "currency_code": old_sax_data.get("currency", ""),
            "weight_kg": old_sax_data.get("weight", ""),
            "product_categories": old_sax_data.get("categories", []),
            "product_images": old_sax_data.get("images", []),
            "suppliers": old_sax_data.get("suppliers", []),
            "additional_data": old_sax_data.get("additional_data", {})
        }

        return legacy_item_data

    def create_or_update_item(self, item_data: Dict[str, Any]) -> bool:
        """
        Override to handle SAX format conversion if needed

        This ensures SAX-parsed data works with existing item processing logic.
        """
        try:
            # Debug logging for item data
            item_identifier = item_data.get('item_code') or item_data.get('external_id') or item_data.get('item_id') or 'Unknown'
            frappe.logger().debug(f"SAX Importer processing item: {item_identifier}")
            frappe.logger().debug(f"Item data keys: {list(item_data.keys())}")

            # Check if this is OLD SAX format data that needs conversion
            # Only convert if it's actually the old format (with item_id and product_name)
            is_old_sax_format = (
                "item_id" in item_data and "product_name" in item_data  # Old SAX has item_id AND product_name
            )

            # Check if this is NEW SAX format that should be converted to legacy
            is_new_sax_format = (
                "external_id" in item_data and
                "item_name" in item_data and
                "product_categories" in item_data  # New SAX has these structured fields
            )

            if is_old_sax_format:
                # This is old SAX format with item_id and product_name, convert using old method
                frappe.logger().debug(f"Converting OLD SAX format data for item: {item_identifier}")
                original_data = item_data.copy()
                item_data = self.convert_old_sax_format(item_data)
                frappe.logger().debug(f"OLD SAX conversion results - Item name: {item_data.get('item_name')}")

            elif is_new_sax_format:
                # This is new SAX format with proper structure, convert to legacy format
                frappe.logger().debug(f"Converting NEW SAX format data for item: {item_identifier}")
                original_data = item_data.copy()
                item_data = self.convert_sax_item_to_legacy_format(item_data)
                frappe.logger().debug(f"NEW SAX conversion results - Item name: {item_data.get('item_name')}")

            # Call parent class method with converted data
            return super().create_or_update_item(item_data)

        except Exception as e:
            item_identifier = item_data.get('item_code') or item_data.get('external_id') or item_data.get('item_id') or 'Unknown'
            error_msg = f"SAX Importer failed for item {item_identifier}: {str(e)}"
            frappe.log_error(error_msg, "SAX Item Import Error")
            frappe.logger().error(error_msg)
            import traceback
            frappe.logger().error(f"SAX Importer traceback: {traceback.format_exc()}")
            return False

    def get_import_statistics(self) -> Dict[str, Any]:
        """Get comprehensive import statistics including SAX-specific metrics"""
        return {
            "imported_count": self.imported_count,
            "updated_count": self.updated_count,
            "error_count": self.error_count,
            "errors": self.errors,
            "sax_stats": self.sax_stats,
            "use_queue": self.use_queue,
            "queue_processor_timeout": self.queue_processor_timeout
        }


# Public API functions (replacements for original functions)
@frappe.whitelist()
def import_xml_items_sax(xml_source: str, company: str = None,
                        use_queue: bool = True) -> Dict[str, Any]:
    """
    Import items from XML feed using SAX parser with Redis queue

    This function always uses Redis queue for memory-optimized background processing.
    The use_queue parameter is kept for backward compatibility.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        use_queue: Kept for backward compatibility (always True)

    Returns:
        Dict with import results
    """
    importer = SAXXMLItemImporter(xml_source, company)
    return importer.import_from_xml()


@frappe.whitelist()
def compare_import_methods(xml_source: str, company: str = None,
                          sample_size: int = 100) -> Dict[str, Any]:
    """
    Compare DOM vs SAX import methods for performance testing

    Args:
        xml_source: URL or file path to XML feed
        company: Company name
        sample_size: Number of items to process for comparison

    Returns:
        Dict with performance comparison results
    """
    import time
    import psutil
    import os

    results = {
        "dom_method": {},
        "sax_method": {},
        "comparison": {}
    }

    try:
        # Test DOM method (original)
        frappe.logger().info("Testing DOM method...")
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB

        dom_importer = OriginalXMLItemImporter(xml_source, company)
        # Note: This would need modification to limit to sample_size
        dom_result = dom_importer.import_from_xml()

        dom_time = time.time() - start_time
        dom_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024 - start_memory

        results["dom_method"] = {
            "processing_time": dom_time,
            "memory_usage_mb": dom_memory,
            "result": dom_result
        }

        # Test SAX method
        frappe.logger().info("Testing SAX method...")
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        sax_importer = SAXXMLItemImporter(xml_source, company, use_queue=False)
        sax_result = sax_importer.import_from_xml()

        sax_time = time.time() - start_time
        sax_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024 - start_memory

        results["sax_method"] = {
            "processing_time": sax_time,
            "memory_usage_mb": sax_memory,
            "result": sax_result
        }

        # Calculate comparison
        results["comparison"] = {
            "time_improvement": ((dom_time - sax_time) / dom_time * 100) if dom_time > 0 else 0,
            "memory_improvement": ((dom_memory - sax_memory) / dom_memory * 100) if dom_memory > 0 else 0,
            "success": True
        }

        frappe.logger().info(f"Performance comparison completed: {results['comparison']}")

    except Exception as e:
        results["comparison"] = {
            "error": str(e),
            "success": False
        }
        frappe.log_error(f"Performance comparison failed: {str(e)}")

    return results


# Backward compatibility function - now always uses SAX
@frappe.whitelist()
def import_xml_items(xml_source: str, company: str = None,
                    use_sax: bool = True, use_queue: bool = True) -> Dict[str, Any]:
    """
    Import items using SAX parser with Redis queue

    This function now always uses the memory-optimized SAX parser.
    Parameters use_sax and use_queue are kept for backward compatibility.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        use_sax: Kept for backward compatibility (always True)
        use_queue: Kept for backward compatibility (always True)

    Returns:
        Dict with import results
    """
    # Always use SAX parser with Redis queue for memory efficiency
    return import_xml_items_sax(xml_source, company, use_queue=True)
