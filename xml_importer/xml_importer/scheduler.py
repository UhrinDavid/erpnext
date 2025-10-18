"""
Scheduled XML Import Functions
These functions are called by Frappe's scheduler hooks
"""

import frappe
from .item_importer import XMLItemImporter
from frappe.utils import now_datetime, add_to_date, cint
import requests

def scheduled_xml_import():
    """
    Main scheduled function called by Frappe scheduler
    Checks for XML feed updates and imports items
    """
    try:
        # Get XML import settings
        settings = get_xml_import_settings()

        if not settings.get('enabled', False):
            frappe.logger().info("XML import is disabled in settings")
            return

        xml_url = settings.get('xml_feed_url')
        if not xml_url:
            frappe.logger().error("No XML feed URL configured")
            return

        # Check if feed has changed (if checking is enabled)
        if settings.get('check_feed_changes', True):
            if not has_feed_changed(xml_url, settings):
                frappe.logger().info("XML feed has not changed, skipping import")
                return

        # Run the import
        company = settings.get('company') or frappe.defaults.get_global_default("company")
        importer = XMLItemImporter(xml_url, company)
        result = importer.import_from_xml()

        # Log the import result
        log_import_result(result, xml_url)

        # Update last import timestamp
        update_last_import_time()

        frappe.logger().info(f"Scheduled XML import completed: {result}")

    except Exception as e:
        error_msg = f"Scheduled XML import failed: {str(e)}"
        frappe.log_error(error_msg)

        # Send notification if configured
        send_error_notification(error_msg)

def get_xml_import_settings():
    """Get XML import settings from Site Config or default values"""
    try:
        settings = frappe.get_single("XML Item Import Settings")
        return {
            'enabled': cint(settings.enabled),
            'xml_feed_url': settings.xml_feed_url,
            'company': settings.company,
            'check_feed_changes': cint(settings.check_feed_changes),
            'import_frequency': settings.import_frequency or 'Daily',
            'last_import': settings.last_import,
            'last_etag': settings.last_etag,
            'last_modified': settings.last_modified
        }
    except:
        # Fallback to site config if Settings doctype doesn't exist
        return frappe.local.conf.get('xml_import_settings', {
            'enabled': False,
            'xml_feed_url': '',
            'company': frappe.defaults.get_global_default("company"),
            'check_feed_changes': True,
            'import_frequency': 'Daily'
        })

def has_feed_changed(xml_url, settings):
    """
    Check if XML feed has changed since last import
    Uses HTTP headers (ETag, Last-Modified) and content hash
    """
    try:
        # Make HEAD request to check headers
        response = requests.head(xml_url, timeout=30)
        response.raise_for_status()

        # Check ETag
        current_etag = response.headers.get('ETag')
        last_etag = settings.get('last_etag')

        if current_etag and last_etag:
            if current_etag == last_etag:
                return False

        # Check Last-Modified
        current_modified = response.headers.get('Last-Modified')
        last_modified = settings.get('last_modified')

        if current_modified and last_modified:
            if current_modified == last_modified:
                return False

        # If no reliable headers, check content size
        content_length = response.headers.get('Content-Length')
        if content_length:
            last_size = frappe.db.get_single_value("XML Item Import Settings", "last_content_size")
            if last_size and int(content_length) == last_size:
                frappe.logger().info("Content size unchanged, checking timestamp")

                # Only import if enough time has passed
                last_import = settings.get('last_import')
                if last_import:
                    hours_since_import = (now_datetime() - last_import).total_seconds() / 3600
                    if hours_since_import < 1:  # Less than 1 hour
                        return False

        # Update stored values for next check
        update_feed_metadata(current_etag, current_modified, content_length)

        return True

    except Exception as e:
        frappe.log_error(f"Error checking feed changes: {str(e)}")
        # On error, assume feed changed to be safe
        return True

def update_feed_metadata(etag, last_modified, content_length):
    """Update stored feed metadata"""
    try:
        settings = frappe.get_single("XML Import Settings")
        if etag:
            settings.last_etag = etag
        if last_modified:
            settings.last_modified = last_modified
        if content_length:
            settings.last_content_size = int(content_length)
        settings.save(ignore_permissions=True)
        frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"Failed to update feed metadata: {str(e)}")

def log_import_result(result, xml_url):
    """Log import result to XML Import Log using unified system"""
    try:
        from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_import_log

        create_import_log(
            import_type="Items",
            xml_source=xml_url,
            status="Success" if result.get("success") else "Failed",
            imported=result.get("imported", 0),
            updated=result.get("updated", 0),
            errors=result.get("errors", 0),
            error_details="\n".join(result.get("error_messages", [])),
            summary=frappe.as_json(result)
        )
        frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"Failed to log import result: {str(e)}")

def update_last_import_time():
    """Update last import timestamp"""
    try:
        settings = frappe.get_single("XML Import Settings")
        settings.last_import = now_datetime()
        settings.save(ignore_permissions=True)
        frappe.db.commit()
    except Exception as e:
        frappe.log_error(f"Failed to update last import time: {str(e)}")

def send_error_notification(error_msg):
    """Send error notification to configured users"""
    try:
        settings = frappe.get_single("XML Import Settings")
        notification_emails = settings.get('notification_emails', '')

        if not notification_emails:
            return

        # Send email notification
        frappe.sendmail(
            recipients=notification_emails.split(','),
            subject="XML Import Error",
            message=f"""
            <p>An error occurred during the scheduled XML import:</p>
            <p><strong>Error:</strong> {error_msg}</p>
            <p><strong>Time:</strong> {now_datetime()}</p>
            <p>Please check the Error Log for more details.</p>
            """,
            now=True
        )
    except Exception as e:
        frappe.log_error(f"Failed to send error notification: {str(e)}")

def manual_xml_import(xml_source=None, company=None):
    """
    Manually trigger XML import (can be called from UI)

    Args:
        xml_source: Override XML source URL
        company: Override company

    Returns:
        dict: Import result
    """
    try:
        settings = get_xml_import_settings()

        xml_url = xml_source or settings.get('xml_feed_url')
        company = company or settings.get('company') or frappe.defaults.get_global_default("company")

        if not xml_url:
            return {"success": False, "error": "No XML feed URL provided"}

        # Run the import
        importer = XMLItemImporter(xml_url, company)
        result = importer.import_from_xml()

        # Log the result
        log_import_result(result, xml_url)

        if result.get('success'):
            update_last_import_time()

        return result

    except Exception as e:
        error_msg = f"Manual XML import failed: {str(e)}"
        frappe.log_error(error_msg)
        return {"success": False, "error": error_msg}

def validate_xml_feed(xml_url):
    """
    Validate XML feed format and structure

    Args:
        xml_url: URL to validate

    Returns:
        dict: Validation result
    """
    try:
        importer = XMLItemImporter(xml_url)
        xml_content = importer.fetch_xml_content()
        root = importer.parse_xml(xml_content)

        # Count elements
        shopitems = root.findall('.//SHOPITEM')

        # Sample first item for structure validation
        sample_data = None
        if shopitems:
            sample_data = importer.parse_shop_item(shopitems[0])

        return {
            "success": True,
            "total_items": len(shopitems),
            "root_element": root.tag,
            "sample_item": sample_data,
            "valid": True
        }

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "valid": False
        }

def get_import_status():
    """Get status of XML imports"""
    try:
        settings = get_xml_import_settings()

        # Get last few import logs
        recent_logs = frappe.get_all(
            "XML Import Log",
            fields=["import_datetime", "status", "records_imported", "records_updated", "error_count"],
            order_by="import_datetime desc",
            limit=10
        )

        # Get next scheduled run time
        next_run = None
        if settings.get('enabled') and settings.get('last_import'):
            frequency = settings.get('import_frequency', 'Daily')
            if frequency == 'Hourly':
                next_run = add_to_date(settings['last_import'], hours=1)
            elif frequency == 'Daily':
                next_run = add_to_date(settings['last_import'], days=1)
            elif frequency == 'Weekly':
                next_run = add_to_date(settings['last_import'], weeks=1)

        return {
            "enabled": settings.get('enabled', False),
            "xml_feed_url": settings.get('xml_feed_url', ''),
            "last_import": settings.get('last_import'),
            "next_scheduled": next_run,
            "recent_imports": recent_logs,
            "import_frequency": settings.get('import_frequency', 'Daily')
        }

    except Exception as e:
        frappe.log_error(f"Failed to get import status: {str(e)}")
        return {"error": str(e)}
