# Copyright (c) 2025, Herbatica and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import now

class XMLItemImportConfiguration(Document):
	@frappe.whitelist()
	def trigger_manual_import(self):
		"""Manually trigger XML import based on import type (asynchronous)"""
		if not self.enabled:
			frappe.throw("XML Import is not enabled. Please enable it first.")

		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		# Enqueue background job for async processing
		job_id = frappe.enqueue(
			'xml_importer.xml_importer.doctype.xml_item_import_configuration.xml_item_import_configuration.execute_background_import',
			queue='long',
			timeout=7200,  # 2 hours max
			job_name=f"XML Import - {self.import_type} - {self.name}",
			import_type=self.import_type,
			xml_feed_url=self.xml_feed_url,
			company=self.company,
			config_name=self.name
		)

		# Update status to show import is running
		self.db_set("last_import", now())
		self.db_set("last_import_status", "Running")

		frappe.msgprint(
			f"Import job started in background. Job ID: {job_id}. "
			f"You can check the progress in the background jobs list.",
			title="Import Started",
			indicator="blue"
		)

		return {"success": True, "job_id": job_id, "status": "started"}

def execute_background_import(import_type, xml_feed_url, company, config_name):
	"""Execute the actual import in background using SAX parser with Redis queue"""
	try:
		# Get configuration settings
		config_doc = frappe.get_doc("XML Item Import Configuration", config_name)
		use_sax = config_doc.get("use_sax_parser", True)  # Default to SAX for memory efficiency
		use_queue = config_doc.get("use_redis_queue", True)  # Default to queue for background processing

		if import_type == "Items":
			if use_sax:
				from xml_importer.xml_importer.sax_item_importer import import_xml_items_sax
				result = import_xml_items_sax(xml_feed_url, company, use_queue)
			else:
				from xml_importer.xml_importer.item_importer import import_xml_items
				result = import_xml_items(xml_feed_url, company)
		elif import_type == "Orders":
			if use_sax:
				from xml_importer.xml_importer.sax_order_importer import import_xml_orders_sax
				result = import_xml_orders_sax(xml_feed_url, company, use_queue)
			else:
				from xml_importer.xml_importer.order_importer import import_xml_orders
				result = import_xml_orders(xml_feed_url, company)
		else:
			raise Exception(f"Import type '{import_type}' is not yet implemented")

		# Update the configuration document with results
		config_doc = frappe.get_doc("XML Item Import Configuration", config_name)
		config_doc.db_set("last_import_status", "Success" if result.get("success") else "Failed")

		# Create import log entry
		from xml_importer.xml_importer.doctype.xml_item_import_log.xml_item_import_log import create_import_log
		create_import_log(
			import_type=import_type,
			xml_source=xml_feed_url,
			status="Success" if result.get("success") else "Failed",
			imported=result.get("imported", 0),
			updated=result.get("updated", 0),
			errors=result.get("errors", 0),
			error_details="\n".join(result.get("error_messages", [])),
			summary=result
		)

		frappe.publish_realtime(
			"import_completed",
			{
				"import_type": import_type,
				"success": result.get("success"),
				"imported": result.get("imported", 0),
				"updated": result.get("updated", 0),
				"errors": result.get("errors", 0)
			},
			user=frappe.session.user
		)

		return result

	except Exception as e:
		error_msg = f"Background import failed: {str(e)}"
		frappe.log_error(error_msg)

		# Update configuration status
		config_doc = frappe.get_doc("XML Item Import Configuration", config_name)
		config_doc.db_set("last_import_status", "Failed")

		frappe.publish_realtime(
			"import_failed",
			{"import_type": import_type, "error": str(e)},
			user=frappe.session.user
		)

		raise

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
