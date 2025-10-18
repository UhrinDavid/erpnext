# Copyright (c) 2025, Herbatica and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import now

class XMLImportConfiguration(Document):
	@frappe.whitelist()
	def trigger_manual_import(self):
		"""Manually trigger XML import based on import type"""
		if not self.enabled:
			frappe.throw("XML Import is not enabled. Please enable it first.")

		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		try:
			if self.import_type == "Items":
				from xml_importer.xml_importer.item_importer import import_xml_items
				result = import_xml_items(self.xml_feed_url, self.company)
			elif self.import_type == "Orders":
				from xml_importer.xml_importer.order_importer import import_xml_orders
				result = import_xml_orders(self.xml_feed_url, self.company)
			else:
				frappe.throw(f"Import type '{self.import_type}' is not yet implemented")

			# Update last import status
			self.db_set("last_import", now())
			self.db_set("last_import_status", "Success" if result.get("success") else "Failed")

			# Create import log entry using the unified log
			from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_import_log

			log_doc = create_import_log(
				import_type=self.import_type,
				xml_source=self.xml_feed_url,
				status="Success" if result.get("success") else "Failed",
				imported=result.get("imported", 0),
				updated=result.get("updated", 0),
				errors=result.get("errors", 0),
				error_details="\n".join(result.get("error_messages", [])),
				summary=result
			)

			if result.get("success"):
				return {
					"success": True,
					"message": f"Import completed successfully!\n\nRecords imported: {result.get('imported', 0)}\nRecords updated: {result.get('updated', 0)}\nErrors: {result.get('errors', 0)}",
					"result": result
				}
			else:
				return {
					"success": False,
					"message": f"Import failed: {result.get('error', 'Unknown error')}",
					"result": result
				}

		except Exception as e:
			error_msg = f"Manual import failed: {str(e)}"
			frappe.log_error(error_msg)
			self.db_set("last_import_status", "Failed")

			return {
				"success": False,
				"message": error_msg,
				"result": {"error": str(e)}
			}

	def get_import_specific_fields(self):
		"""Get fields that are relevant for the current import type"""
		if self.import_type == "Items":
			return {
				"create_item_groups": self.create_item_groups,
				"create_manufacturers": self.create_manufacturers,
				"update_stock_levels": self.update_stock_levels,
				"download_images": self.download_images,
				"check_feed_changes": self.check_feed_changes
			}
		elif self.import_type == "Orders":
			return {
				"create_customers": self.create_customers,
				"create_placeholder_items": self.create_placeholder_items,
				"auto_submit_orders": self.auto_submit_orders
			}
		else:
			return {}
