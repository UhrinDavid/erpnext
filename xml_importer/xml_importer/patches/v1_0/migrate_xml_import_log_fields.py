import frappe
from frappe.model.utils.rename_field import rename_field

def execute():
    """
    Migrate XML Import Log fields after app rename from xml_item_importer to xml_importer
    """
    if not frappe.db.exists("DocType", "XML Import Log"):
        return

    # Add import_type field with default value for existing records
    if not frappe.db.has_column("XML Import Log", "import_type"):
        frappe.reload_doc("XML Importer", "doctype", "XML Import Log")

        # Set default import_type for existing records
        frappe.db.sql("""
            UPDATE `tabXML Import Log`
            SET import_type = 'Items'
            WHERE import_type IS NULL OR import_type = ''
        """)

    # Rename field names if they exist with old names
    if frappe.db.has_column("XML Import Log", "items_imported") and not frappe.db.has_column("XML Import Log", "records_imported"):
        rename_field("XML Import Log", "items_imported", "records_imported")

    if frappe.db.has_column("XML Import Log", "items_updated") and not frappe.db.has_column("XML Import Log", "records_updated"):
        rename_field("XML Import Log", "items_updated", "records_updated")

    # Update import_date to import_datetime if needed
    if frappe.db.has_column("XML Import Log", "import_date") and not frappe.db.has_column("XML Import Log", "import_datetime"):
        rename_field("XML Import Log", "import_date", "import_datetime")

    frappe.db.commit()
