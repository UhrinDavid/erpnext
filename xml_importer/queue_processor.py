"""
Redis Queue Processor for XML Import Background Jobs
Processes queued XML items and orders from Redis queues

This module handles the background processing of XML items and orders
that have been queued by the SAX parser for memory-efficient processing.

Author: Herbatica
License: MIT
"""

import frappe
import redis
import json
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import traceback


class RedisQueueProcessor:
    """
    Processes items from Redis queues for XML import
    Handles both item and order queues with retry logic and error handling
    """

    def __init__(self, redis_client=None):
        """Initialize queue processor"""
        self.redis_client = redis_client or self._get_redis_client()
        self.processed_count = 0
        self.error_count = 0
        self.errors = []

    def _get_redis_client(self) -> redis.Redis:
        """Get Redis client connection"""
        try:
            # Get Redis configuration from Frappe
            redis_config = frappe.conf.get("redis_queue") or frappe.conf.get("redis_cache")

            if isinstance(redis_config, str):
                return redis.from_url(redis_config)
            elif isinstance(redis_config, dict):
                return redis.Redis(**redis_config)
            else:
                return redis.Redis(host='localhost', port=6379, db=0)

        except Exception as e:
            frappe.log_error(f"Redis connection error: {str(e)}")
            return redis.Redis(host='localhost', port=6379, db=0)

    def process_item_queue(self, queue_name: str, company: str = None,
                          max_items: int = None, timeout: int = 300) -> Dict[str, Any]:
        """
        Process items from Redis queue

        Args:
            queue_name: Name of Redis queue containing items
            company: ERPNext company name
            max_items: Maximum number of items to process (None for all)
            timeout: Maximum processing time in seconds

        Returns:
            Dict with processing statistics
        """
        from xml_importer.xml_importer.item_importer import XMLItemImporter

        start_time = time.time()
        processed = 0
        errors = 0
        error_details = []

        try:
            # Create item importer instance
            importer = XMLItemImporter(company=company)

            frappe.logger().info(f"Starting to process item queue: {queue_name}")

            while True:
                # Check timeout
                if time.time() - start_time > timeout:
                    frappe.logger().warning(f"Queue processing timeout after {timeout} seconds")
                    break

                # Check max items limit
                if max_items and processed >= max_items:
                    frappe.logger().info(f"Reached max items limit: {max_items}")
                    break

                # Get next item from queue (blocking pop with 1 second timeout)
                queue_data = self.redis_client.brpop(queue_name, timeout=1)

                if not queue_data:
                    # No more items in queue
                    frappe.logger().info(f"No more items in queue: {queue_name}")
                    break

                try:
                    # Parse queue entry
                    queue_entry = json.loads(queue_data[1])
                    item_data = queue_entry["item_data"]

                    # Log item data for debugging
                    item_id = item_data.get('item_code') or item_data.get('external_id') or item_data.get('item_id', 'Unknown')
                    frappe.logger().debug(f"Processing item: {item_id}")

                    # Process the item
                    success = importer.create_or_update_item(item_data)

                    if success:
                        processed += 1
                        self.processed_count += 1
                        frappe.logger().info(f"Successfully processed item: {item_id}")
                    else:
                        errors += 1
                        self.error_count += 1
                        # Get more detailed error from importer
                        last_errors = getattr(importer, 'errors', [])
                        detailed_error = last_errors[-1] if last_errors else f"Unknown error processing item: {item_id}"
                        error_msg = f"Failed to process item {item_id}: {detailed_error}"
                        error_details.append(error_msg)
                        frappe.log_error(error_msg, "XML Import Queue Processing")
                        frappe.logger().error(f"Item processing failed for {item_id}: {detailed_error}")

                    # Update progress every 10 items
                    if processed % 10 == 0:
                        self._update_progress("items", processed, errors, queue_name)

                except json.JSONDecodeError as e:
                    errors += 1
                    error_msg = f"Invalid JSON in queue: {str(e)}"
                    error_details.append(error_msg)
                    frappe.log_error(error_msg)

                except Exception as e:
                    errors += 1
                    error_msg = f"Error processing queue item: {str(e)}"
                    error_details.append(error_msg)
                    frappe.log_error(error_msg + "\n" + traceback.format_exc())

                # Commit after each item to avoid long transactions
                frappe.db.commit()

            # Final progress update
            self._update_progress("items", processed, errors, queue_name, completed=True)

            result = {
                "success": True,
                "queue_name": queue_name,
                "processed": processed,
                "errors": errors,
                "error_details": error_details[:10],  # First 10 errors
                "processing_time": time.time() - start_time
            }

            frappe.logger().info(f"Queue processing completed: {result}")
            return result

        except Exception as e:
            error_msg = f"Queue processing failed: {str(e)}"
            frappe.log_error(error_msg + "\n" + traceback.format_exc())
            return {
                "success": False,
                "error": error_msg,
                "processed": processed,
                "errors": errors
            }

    def process_order_queue(self, queue_name: str, company: str = None,
                           max_orders: int = None, timeout: int = 300) -> Dict[str, Any]:
        """
        Process orders from Redis queue

        Args:
            queue_name: Name of Redis queue containing orders
            company: ERPNext company name
            max_orders: Maximum number of orders to process (None for all)
            timeout: Maximum processing time in seconds

        Returns:
            Dict with processing statistics
        """
        from xml_importer.xml_importer.order_importer import XMLOrderImporter

        start_time = time.time()
        processed = 0
        errors = 0
        error_details = []

        try:
            # Create order importer instance
            importer = XMLOrderImporter(company=company)

            frappe.logger().info(f"Starting to process order queue: {queue_name}")

            while True:
                # Check timeout
                if time.time() - start_time > timeout:
                    frappe.logger().warning(f"Queue processing timeout after {timeout} seconds")
                    break

                # Check max orders limit
                if max_orders and processed >= max_orders:
                    frappe.logger().info(f"Reached max orders limit: {max_orders}")
                    break

                # Get next order from queue
                queue_data = self.redis_client.brpop(queue_name, timeout=1)

                if not queue_data:
                    frappe.logger().info(f"No more orders in queue: {queue_name}")
                    break

                try:
                    # Parse queue entry
                    queue_entry = json.loads(queue_data[1])
                    order_data = queue_entry["order_data"]

                    # Process the order
                    success = importer.create_or_update_order(order_data)

                    if success:
                        processed += 1
                        self.processed_count += 1
                    else:
                        errors += 1
                        self.error_count += 1
                        error_msg = f"Failed to process order: {order_data.get('order_id', 'Unknown')}"
                        error_details.append(error_msg)
                        frappe.log_error(error_msg)

                    # Update progress every 5 orders
                    if processed % 5 == 0:
                        self._update_progress("orders", processed, errors, queue_name)

                except json.JSONDecodeError as e:
                    errors += 1
                    error_msg = f"Invalid JSON in queue: {str(e)}"
                    error_details.append(error_msg)
                    frappe.log_error(error_msg)

                except Exception as e:
                    errors += 1
                    error_msg = f"Error processing queue order: {str(e)}"
                    error_details.append(error_msg)
                    frappe.log_error(error_msg + "\n" + traceback.format_exc())

                # Commit after each order
                frappe.db.commit()

            # Final progress update
            self._update_progress("orders", processed, errors, queue_name, completed=True)

            result = {
                "success": True,
                "queue_name": queue_name,
                "processed": processed,
                "errors": errors,
                "error_details": error_details[:10],
                "processing_time": time.time() - start_time
            }

            frappe.logger().info(f"Order queue processing completed: {result}")
            return result

        except Exception as e:
            error_msg = f"Order queue processing failed: {str(e)}"
            frappe.log_error(error_msg + "\n" + traceback.format_exc())
            return {
                "success": False,
                "error": error_msg,
                "processed": processed,
                "errors": errors
            }

    def _update_progress(self, content_type: str, processed: int, errors: int,
                        queue_name: str, completed: bool = False):
        """Update progress in realtime"""
        frappe.publish_realtime(
            "queue_process_progress",
            {
                "content_type": content_type,
                "queue_name": queue_name,
                "processed": processed,
                "errors": errors,
                "completed": completed,
                "message": f"Processed {processed} {content_type}, {errors} errors"
            },
            user=frappe.session.user
        )

    def get_queue_stats(self, queue_name: str) -> Dict[str, Any]:
        """Get statistics about a Redis queue"""
        try:
            queue_length = self.redis_client.llen(queue_name)

            # Try to peek at first item without removing it
            first_item = None
            if queue_length > 0:
                items = self.redis_client.lrange(queue_name, 0, 0)
                if items:
                    try:
                        first_item = json.loads(items[0])
                    except:
                        first_item = {"parse_error": True}

            return {
                "queue_name": queue_name,
                "length": queue_length,
                "first_item_preview": first_item,
                "exists": queue_length > 0
            }

        except Exception as e:
            frappe.log_error(f"Error getting queue stats: {str(e)}")
            return {
                "queue_name": queue_name,
                "error": str(e),
                "exists": False
            }

    def clear_queue(self, queue_name: str) -> int:
        """Clear all items from a queue and return number of items removed"""
        try:
            removed_count = self.redis_client.delete(queue_name)
            frappe.logger().info(f"Cleared queue {queue_name}: {removed_count} items removed")
            return removed_count
        except Exception as e:
            frappe.log_error(f"Error clearing queue {queue_name}: {str(e)}")
            return 0

    def list_active_queues(self, pattern: str = "xml_import_*") -> List[str]:
        """List all active XML import queues"""
        try:
            return [key.decode() for key in self.redis_client.keys(pattern)]
        except Exception as e:
            frappe.log_error(f"Error listing queues: {str(e)}")
            return []


@frappe.whitelist()
def process_redis_queue(queue_name: str, content_type: str = "items",
                       company: str = None, max_items: int = None) -> Dict[str, Any]:
    """
    Public API to process a Redis queue

    Args:
        queue_name: Name of Redis queue to process
        content_type: "items" or "orders"
        company: ERPNext company name
        max_items: Maximum number of items to process

    Returns:
        Dict with processing results
    """
    processor = RedisQueueProcessor()

    if content_type == "items":
        return processor.process_item_queue(queue_name, company, max_items)
    elif content_type == "orders":
        return processor.process_order_queue(queue_name, company, max_items)
    else:
        return {
            "success": False,
            "error": f"Invalid content_type: {content_type}"
        }


@frappe.whitelist()
def get_queue_info(queue_name: str = None) -> Dict[str, Any]:
    """
    Get information about Redis queues

    Args:
        queue_name: Specific queue name (None to list all active queues)

    Returns:
        Dict with queue information
    """
    processor = RedisQueueProcessor()

    if queue_name:
        return processor.get_queue_stats(queue_name)
    else:
        active_queues = processor.list_active_queues()
        queue_stats = []

        for queue in active_queues:
            stats = processor.get_queue_stats(queue)
            queue_stats.append(stats)

        return {
            "active_queues": queue_stats,
            "total_queues": len(active_queues)
        }


@frappe.whitelist()
def clear_redis_queue(queue_name: str) -> Dict[str, Any]:
    """
    Clear a Redis queue

    Args:
        queue_name: Name of queue to clear

    Returns:
        Dict with clearing results
    """
    processor = RedisQueueProcessor()
    removed_count = processor.clear_queue(queue_name)

    return {
        "success": True,
        "queue_name": queue_name,
        "items_removed": removed_count
    }


def cleanup_old_queues(days_old: int = 7):
    """
    Cleanup old Redis queues based on naming pattern
    Called by scheduler to prevent Redis memory buildup

    Args:
        days_old: Remove queues older than this many days
    """
    try:
        processor = RedisQueueProcessor()
        active_queues = processor.list_active_queues()

        cutoff_date = datetime.now() - timedelta(days=days_old)
        cutoff_str = cutoff_date.strftime("%Y%m%d")

        cleaned = 0
        for queue in active_queues:
            # Extract date from queue name (format: xml_import_items_YYYYMMDD_HHMMSS)
            try:
                parts = queue.split('_')
                if len(parts) >= 4:
                    date_part = parts[3]  # YYYYMMDD
                    if date_part < cutoff_str:
                        removed = processor.clear_queue(queue)
                        if removed > 0:
                            cleaned += 1
                            frappe.logger().info(f"Cleaned old queue: {queue} ({removed} items)")
            except:
                # Skip queues that don't match expected format
                continue

        if cleaned > 0:
            frappe.logger().info(f"Cleaned up {cleaned} old Redis queues")

    except Exception as e:
        frappe.log_error(f"Error cleaning up old queues: {str(e)}")


# Background job functions for scheduler
def scheduled_queue_cleanup():
    """Scheduled function to cleanup old Redis queues"""
    cleanup_old_queues(days_old=7)


@frappe.whitelist()
def enqueue_queue_processing(queue_name: str, content_type: str = "items",
                            company: str = None) -> str:
    """
    Enqueue queue processing as a background job

    Args:
        queue_name: Redis queue name to process
        content_type: "items" or "orders"
        company: ERPNext company name

    Returns:
        Job ID for tracking
    """
    job_id = frappe.enqueue(
        "xml_importer.xml_importer.queue_processor.process_redis_queue",
        queue="long",
        timeout=1800,  # 30 minutes
        queue_name=queue_name,
        content_type=content_type,
        company=company
    )

    frappe.logger().info(f"Enqueued queue processing job: {job_id} for queue: {queue_name}")
    return job_id
