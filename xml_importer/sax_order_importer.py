"""
Memory-Optimized XML Order Importer using SAX Parser with Redis Queue
Replaces DOM-based ElementTree parsing with streaming SAX parser for orders

This version processes large XML order files without loading entire content into memory,
using SAX parser to stream through XML and queue orders in Redis for background processing.

Author: Herbatica
License: MIT
"""

import frappe
import requests
from frappe.utils import now, cstr, flt, cint, strip_html_tags, get_datetime
from typing import Dict, List, Optional, Any
import json
from xml_importer.xml_importer.sax_parser import parse_xml_with_sax, get_redis_client
from xml_importer.xml_importer.queue_processor import RedisQueueProcessor
from xml_importer.xml_importer.order_importer import XMLOrderImporter as OriginalXMLOrderImporter


class SAXXMLOrderImporter(OriginalXMLOrderImporter):
    """
    Memory-optimized XML Order Importer using SAX parser with Redis queue

    Inherits all the order processing methods from the original XMLOrderImporter
    but replaces the XML parsing approach to use streaming SAX parser instead of DOM.
    """

    def __init__(self, xml_source: str = None, company: str = None, config=None,
                 queue_processor_timeout: int = 600):
        """
        Initialize SAX-based XML Order Importer with Redis queue

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
            "orders_queued": 0,
            "orders_processed": 0,
            "parse_errors": 0
        }

    def import_from_xml(self) -> Dict[str, Any]:
        """
        Main import function using SAX parser with Redis queue

        This replaces the original DOM-based approach with memory-efficient streaming.
        """
        try:
            frappe.logger().info(f"Starting SAX-based XML order import from: {self.xml_source}")

            # Fetch XML content (same as original)
            xml_content = self.fetch_xml_content()

            # Always use Redis queue for processing
            return self._import_with_queue(xml_content)

        except Exception as e:
            error_msg = f"SAX XML order import failed: {str(e)}"
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
            # Phase 1: Parse XML and queue orders using SAX
            frappe.publish_realtime(
                "sax_order_import_progress",
                {"phase": "parsing", "message": "Parsing XML and queueing orders..."},
                user=frappe.session.user
            )

            sax_result = parse_xml_with_sax(
                xml_content=xml_content,
                content_type="orders",
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

            frappe.logger().info(f"SAX parsing completed: {sax_result['orders_queued']} orders queued in {queue_name}")

            # Phase 2: Process queued orders
            frappe.publish_realtime(
                "sax_order_import_progress",
                {
                    "phase": "processing",
                    "message": f"Processing {sax_result['orders_queued']} queued orders...",
                    "orders_queued": sax_result['orders_queued']
                },
                user=frappe.session.user
            )

            # Process the queue
            process_result = self.queue_processor.process_order_queue(
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
                "sax_order_import_progress",
                {
                    "phase": "complete",
                    "message": "Order import completed successfully",
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
                "total_processed": sax_result["orders_queued"],
                "queue_name": queue_name,
                "sax_stats": self.sax_stats,
                "processing_time": process_result.get("processing_time", 0),
                "method": "sax_with_queue"
            }

            frappe.logger().info(f"SAX order import completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"SAX order import with queue failed: {str(e)}"
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
            frappe.logger().info("Starting SAX order import with immediate processing")

            # Define immediate processor function
            def immediate_order_processor(order_data: Dict[str, Any]) -> bool:
                """Process order immediately during SAX parsing"""
                try:
                    success = self.create_or_update_order(order_data)
                    if success:
                        self.imported_count += 1
                    else:
                        self.error_count += 1
                    return success
                except Exception as e:
                    error_msg = f"Error processing order {order_data.get('order_id', 'Unknown')}: {str(e)}"
                    self.add_error(error_msg)
                    self.error_count += 1
                    return False

            # Parse XML with immediate processing
            sax_result = parse_xml_with_sax(
                xml_content=xml_content,
                content_type="orders",
                immediate_processor=immediate_order_processor
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
                "sax_order_import_progress",
                {
                    "phase": "complete",
                    "message": "Order import completed successfully",
                    "completed": True,
                    "orders_processed": self.imported_count,
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
                "total_processed": sax_result["orders_queued"],
                "sax_stats": self.sax_stats,
                "method": "sax_immediate"
            }

            frappe.logger().info(f"SAX immediate order import completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"SAX immediate order import failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported": self.imported_count,
                "updated": 0,
                "errors": self.error_count + 1
            }

    def convert_sax_order_to_legacy_format(self, sax_order_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert SAX-parsed order data to legacy format expected by create_or_update_order

        This ensures compatibility with existing order processing methods.
        """
        try:
            # Map SAX format to legacy format
            legacy_order_data = {
                "order_id": sax_order_data.get("order_id", ""),
                "order_number": sax_order_data.get("order_number", ""),
                "order_date": sax_order_data.get("order_date", ""),
                "customer_data": sax_order_data.get("customer_data", {}),
                "order_items": sax_order_data.get("items", []),  # Map 'items' to 'order_items'
                "additional_data": sax_order_data.get("additional_data", {})
            }

            # Ensure customer_data has required fields
            if not legacy_order_data["customer_data"]:
                legacy_order_data["customer_data"] = {}

            # Map additional order-level fields from additional_data
            additional = sax_order_data.get("additional_data", {})
            for key, value in additional.items():
                if key not in legacy_order_data and value:
                    legacy_order_data[key] = value

            return legacy_order_data

        except Exception as e:
            frappe.log_error(f"Error converting SAX order data: {str(e)}")
            return sax_order_data  # Return as-is if conversion fails

    def create_or_update_order(self, order_data: Dict[str, Any]) -> bool:
        """
        Override to handle SAX format conversion if needed

        This ensures SAX-parsed data works with existing order processing logic.
        """
        # Check if this is SAX format data that needs conversion
        if "order_id" in order_data and "customer_data" in order_data and "items" in order_data:
            # This looks like SAX format, convert to legacy format if needed
            order_data = self.convert_sax_order_to_legacy_format(order_data)

        # Call parent class method with converted data
        return super().create_or_update_order(order_data)

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
def import_xml_orders_sax(xml_source: str, company: str = None,
                         use_queue: bool = True) -> Dict[str, Any]:
    """
    Import orders from XML feed using SAX parser with Redis queue

    This function always uses Redis queue for memory-optimized background processing.
    The use_queue parameter is kept for backward compatibility.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        use_queue: Kept for backward compatibility (always True)

    Returns:
        Dict with import results
    """
    importer = SAXXMLOrderImporter(xml_source, company)
    return importer.import_from_xml()


@frappe.whitelist()
def compare_order_import_methods(xml_source: str, company: str = None,
                                sample_size: int = 50) -> Dict[str, Any]:
    """
    Compare DOM vs SAX order import methods for performance testing

    Args:
        xml_source: URL or file path to XML feed
        company: Company name
        sample_size: Number of orders to process for comparison

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
        frappe.logger().info("Testing DOM method for orders...")
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024  # MB

        dom_importer = OriginalXMLOrderImporter(xml_source, company)
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
        frappe.logger().info("Testing SAX method for orders...")
        start_time = time.time()
        start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024

        sax_importer = SAXXMLOrderImporter(xml_source, company, use_queue=False)
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

        frappe.logger().info(f"Order performance comparison completed: {results['comparison']}")

    except Exception as e:
        results["comparison"] = {
            "error": str(e),
            "success": False
        }
        frappe.log_error(f"Order performance comparison failed: {str(e)}")

    return results


# Backward compatibility function - now always uses SAX
@frappe.whitelist()
def import_xml_orders(xml_source: str, company: str = None,
                     use_sax: bool = True, use_queue: bool = True) -> Dict[str, Any]:
    """
    Import orders using SAX parser with Redis queue

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
    return import_xml_orders_sax(xml_source, company, use_queue=True)


# Scheduled function for background order processing
def scheduled_sax_order_import():
    """
    Scheduled function to import XML orders using SAX parser based on configured frequency
    This replaces the original scheduled_xml_import for orders with SAX-based processing
    """
    # Get all enabled XML Import Configurations for orders
    configs = frappe.get_all(
        "XML Import Configuration",
        filters={"enabled": 1, "import_type": "Orders"},
        fields=["name", "xml_feed_url", "company", "import_frequency", "last_import"]
    )

    if not configs:
        frappe.logger().debug("No enabled XML Order Import Configurations found")
        return

    from datetime import datetime, timedelta
    from frappe.utils import now_datetime

    for config in configs:
        try:
            # Check if it's time to import based on frequency
            # This would need the same should_run_import logic from the original

            frappe.logger().info(f"Running scheduled SAX order import for {config.name}")

            result = import_xml_orders_sax(config.xml_feed_url, config.company, use_queue=True)

            # Create import log
            from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_order_import_log
            create_order_import_log(
                xml_source=config.xml_feed_url,
                status="Success" if result.get("success") else "Failed",
                imported=result.get("imported", 0),
                errors=result.get("errors", 0),
                error_details="\n".join(result.get("error_messages", [])),
                summary=result
            )

            # Update last import time
            frappe.db.set_value("XML Import Configuration", config.name, {
                "last_import": now_datetime(),
                "last_import_status": "Success" if result.get("success") else "Failed"
            })
            frappe.db.commit()

        except Exception as e:
            frappe.log_error(f"Scheduled SAX XML order import error for {config.name}: {str(e)}")
            frappe.db.set_value("XML Import Configuration", config.name, {
                "last_import": now_datetime(),
                "last_import_status": "Failed"
            })
            frappe.db.commit()
