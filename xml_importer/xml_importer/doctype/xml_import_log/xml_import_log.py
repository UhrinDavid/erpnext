# Copyright (c) 2024, Herbatica and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import now


class XMLImportLog(Document):
	def validate(self):
		"""Validate XML Import Log"""
		pass

	def on_update(self):
		"""Actions after updating log"""
		# Auto-delete old logs (keep last 100)
		if frappe.db.count("XML Import Log") > 100:
			logs_to_delete = frappe.get_all(
				"XML Import Log",
				fields=["name"],
				order_by="import_datetime asc",
				limit=frappe.db.count("XML Import Log") - 100
			)
			for log in logs_to_delete:
				frappe.delete_doc("XML Import Log", log.name, ignore_permissions=True)


@frappe.whitelist()
def create_import_log(import_type, xml_source, status, imported=0, updated=0, errors=0, error_details="", summary=None):
	"""
	Create a new unified XML Import Log entry

	Args:
		import_type: Type of import (Items, Orders, Customers, Product Updates)
		xml_source: URL or path of the XML source
		status: Import status (Success, Failed, Partial)
		imported: Number of records imported (default: 0)
		updated: Number of records updated (default: 0)
		errors: Number of errors encountered (default: 0)
		error_details: Detailed error messages (default: "")
		summary: JSON summary of the import (optional)

	Returns:
		dict: Created log document
	"""
	try:
		# Validate import_type
		valid_types = ["Items", "Orders", "Customers", "Product Updates"]
		if import_type not in valid_types:
			frappe.throw(f"Invalid import type: {import_type}. Must be one of: {', '.join(valid_types)}")

		log_doc = frappe.get_doc({
			"doctype": "XML Import Log",
			"import_datetime": now(),
			"import_type": import_type,
			"xml_source": xml_source,
			"status": status,
			"records_imported": imported,
			"records_updated": updated,
			"error_count": errors,
			"total_processed": imported + updated,
			"error_message": error_details,
			"summary": frappe.as_json(summary) if summary else "{}"
		})
		log_doc.insert(ignore_permissions=True)
		return log_doc.as_dict()
	except Exception as e:
		frappe.log_error(f"Failed to create import log: {str(e)}")
		return None


def create_item_import_log(xml_source, status, imported=0, updated=0, errors=0, error_details="", summary=None):
	"""Create log specifically for item imports"""
	return create_import_log("Items", xml_source, status, imported, updated, errors, error_details, summary)


def create_order_import_log(xml_source, status, imported=0, updated=0, errors=0, error_details="", summary=None):
	"""Create log specifically for order imports"""
	return create_import_log("Orders", xml_source, status, imported, updated, errors, error_details, summary)


def create_customer_import_log(xml_source, status, imported=0, updated=0, errors=0, error_details="", summary=None):
	"""Create log specifically for customer imports"""
	return create_import_log("Customers", xml_source, status, imported, updated, errors, error_details, summary)


def create_product_update_log(xml_source, status, imported=0, updated=0, errors=0, error_details="", summary=None):
	"""Create log specifically for product updates"""
	return create_import_log("Product Updates", xml_source, status, imported, updated, errors, error_details, summary)
