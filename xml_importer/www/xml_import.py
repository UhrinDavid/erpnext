import frappe

def get_context(context):
    """Get context for XML Import page"""
    context.no_cache = 1

    # Check permissions
    if not frappe.has_permission("XML Import Configuration", "read"):
        frappe.throw("Not permitted", frappe.PermissionError)

    # Get current configurations
    try:
        configurations = frappe.get_list("XML Import Configuration",
            fields=["name", "import_type", "enabled", "xml_feed_url", "last_import", "last_import_status"],
            order_by="modified desc"
        )
        context.configurations = configurations
    except:
        context.configurations = []

    # Get recent import logs
    try:
        logs = frappe.get_list("XML Import Log",
            fields=["name", "import_datetime", "import_type", "status", "records_imported", "records_updated", "error_count"],
            order_by="import_datetime desc",
            limit=20
        )
        context.recent_logs = logs
    except:
        context.recent_logs = []

    context.title = "XML Importer Dashboard"
