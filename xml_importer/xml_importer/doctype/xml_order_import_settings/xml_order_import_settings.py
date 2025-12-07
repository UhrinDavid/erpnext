# Copyright (c) 2025, Herbatica and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document

class XMLOrderImportSettings(Document):
	def validate(self):
		"""Validate XML Order Import Settings"""
		if self.enabled and not self.xml_feed_url:
			frappe.throw("XML Feed URL is required when XML Order Import is enabled")

		if self.enabled and not self.company:
			frappe.throw("Company is required when XML Order Import is enabled")

		# Validate XML feed URL format
		if self.xml_feed_url:
			if not self.xml_feed_url.startswith(('http://', 'https://', '/')):
				frappe.throw("XML Feed URL must be a valid HTTP/HTTPS URL or local file path")

		# Validate notification emails format
		if self.notification_emails:
			emails = [email.strip() for email in self.notification_emails.split(",")]
			for email in emails:
				if email and "@" not in email:
					frappe.throw(f"Invalid email format: {email}")

	def on_update(self):
		"""Clear cache when settings are updated"""
		frappe.cache().delete_key("xml_order_import_settings")

	@frappe.whitelist()
	def test_connection(self):
		"""Test connection to XML feed"""
		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		from xml_importer.xml_importer.order_importer import XMLOrderImporter

		try:
			importer = XMLOrderImporter(self.xml_feed_url, self.company)
			xml_content = importer.fetch_xml_content()
			root = importer.parse_xml(xml_content)

			# Count orders
			orders = root.findall('.//ORDER')

			return {
				"success": True,
				"message": f"Connection successful! Found {len(orders)} orders in XML feed.",
				"order_count": len(orders)
			}

		except Exception as e:
			frappe.log_error(f"XML order connection test failed: {str(e)}")
			return {
				"success": False,
				"message": f"Connection failed: {str(e)}"
			}

	@frappe.whitelist()
	def trigger_manual_import(self):
		"""Manually trigger XML order import using SAX parser with Redis queue"""
		if not self.enabled:
			frappe.throw("XML Order Import is not enabled. Please enable it first.")

		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		from xml_importer.xml_importer.order_importer import import_xml_orders_sax

		try:
			# Run the import using SAX parser with Redis queue
			result = import_xml_orders_sax(self.xml_feed_url, self.company)

			# Update last import status
			self.db_set("last_import", frappe.utils.now())
			self.db_set("last_import_status", "Success" if result.get("success") else "Failed")

			return {
				"success": True,
				"message": f"Import completed successfully! Imported: {result.get('imported', 0)}, Updated: {result.get('updated', 0)}, Errors: {result.get('errors', 0)}",
				"result": result
			}

		except Exception as e:
			error_msg = f"Manual order import failed: {str(e)}"
			frappe.log_error(error_msg)

			# Update last import status
			self.db_set("last_import_status", "Failed")

			return {
				"success": False,
				"message": error_msg
			}

		# Update status to show import is running
		self.db_set("last_import", frappe.utils.now())
		self.db_set("last_import_status", "Running")

		frappe.msgprint(
			f"Order import job started in background. Job ID: {job_id}. "
			f"You can check the progress in the background jobs list.",
			title="Import Started",
			indicator="blue"
		)

		return {"success": True, "job_id": job_id, "status": "started"}

def execute_background_order_import(xml_feed_url, company, config_name):
	"""Execute the actual order import in background"""
	try:
		from xml_importer.xml_importer.order_importer import import_xml_orders
		result = import_xml_orders(xml_feed_url, company)

		# Update the configuration document with results
		config_doc = frappe.get_doc("XML Order Import Settings", config_name)
		config_doc.db_set("last_import_status", "Success" if result.get("success") else "Failed")

		# Create import log entry
		from xml_importer.xml_importer.doctype.xml_order_import_log.xml_order_import_log import create_import_log
		create_import_log(
			import_type="Orders",
			xml_source=xml_feed_url,
			status="Success" if result.get("success") else "Failed",
			imported=result.get("imported", 0),
			updated=result.get("updated", 0),
			errors=result.get("errors", 0),
			error_details="\n".join(result.get("error_messages", [])),
			summary=result
		)

		frappe.publish_realtime(
			"order_import_completed",
			{
				"success": result.get("success"),
				"imported": result.get("imported", 0),
				"updated": result.get("updated", 0),
				"errors": result.get("errors", 0)
			},
			user=frappe.session.user
		)

		return result

	except Exception as e:
		error_msg = f"Background order import failed: {str(e)}"
		frappe.log_error(error_msg)

		# Update configuration status
		config_doc = frappe.get_doc("XML Order Import Settings", config_name)
		config_doc.db_set("last_import_status", "Failed")

		frappe.publish_realtime(
			"order_import_failed",
			{"error": str(e)},
			user=frappe.session.user
		)

		raise
