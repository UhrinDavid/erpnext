# Copyright (c) 2024, Herbatica and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document


class XMLItemImportSettings(Document):
	def validate(self):
		"""Validate XML Import Settings"""
		if self.enabled and not self.xml_feed_url:
			frappe.throw("XML Feed URL is required when XML Import is enabled")

		if self.enabled and not self.company:
			frappe.throw("Company is required when XML Import is enabled")

	def on_update(self):
		"""Clear cache when settings are updated"""
		frappe.cache().delete_key("xml_import_settings")

	@frappe.whitelist()
	def test_connection(self):
		"""Test connection to XML feed"""
		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		from xml_importer.xml_importer.item_importer import XMLItemImporter

		try:
			importer = XMLItemImporter(self.xml_feed_url, self.company)
			xml_content = importer.fetch_xml_content()
			root = importer.parse_xml(xml_content)

			# Count items
			shopitems = root.findall('.//SHOPITEM')

			return {
				"success": True,
				"message": f"Connection successful! Found {len(shopitems)} items in XML feed.",
				"item_count": len(shopitems)
			}

		except Exception as e:
			frappe.log_error(f"XML connection test failed: {str(e)}")
			return {
				"success": False,
				"message": f"Connection failed: {str(e)}"
			}

	@frappe.whitelist()
	def trigger_manual_import(self):
		"""Manually trigger XML import"""
		if not self.enabled:
			frappe.throw("XML Import is not enabled. Please enable it first.")

		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		from xml_importer.xml_importer.item_importer import import_xml_items

		try:
			# Run the import
			result = import_xml_items(self.xml_feed_url, self.company)

			# Update last import status
			self.db_set("last_import", frappe.utils.now())
			self.db_set("last_import_status", "Success" if result.get("success") else "Failed")

			# Create import log entry
			log_doc = frappe.get_doc({
				"doctype": "XML Import Log",
				"import_date": frappe.utils.now(),
				"xml_source": self.xml_feed_url,
				"status": "Success" if result.get("success") else "Failed",
				"items_imported": result.get("imported", 0),
				"items_updated": result.get("updated", 0),
				"errors": result.get("errors", 0),
				"error_details": "\n".join(result.get("error_messages", [])),
				"summary": frappe.as_json(result)
			})
			log_doc.insert(ignore_permissions=True)

			if result.get("success"):
				return {
					"success": True,
					"message": f"Import completed successfully!\n\nItems imported: {result.get('imported', 0)}\nItems updated: {result.get('updated', 0)}\nErrors: {result.get('errors', 0)}",
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

			# Create error log entry
			try:
				log_doc = frappe.get_doc({
					"doctype": "XML Import Log",
					"import_date": frappe.utils.now(),
					"xml_source": self.xml_feed_url,
					"status": "Failed",
					"items_imported": 0,
					"items_updated": 0,
					"errors": 1,
					"error_details": error_msg,
					"summary": frappe.as_json({"success": False, "error": error_msg})
				})
				log_doc.insert(ignore_permissions=True)
			except:
				pass  # Don't fail if log creation fails

			return {
				"success": False,
				"message": error_msg
			}
