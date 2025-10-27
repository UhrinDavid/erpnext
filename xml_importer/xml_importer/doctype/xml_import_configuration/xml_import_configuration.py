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

			return {
				"success": True,
				"message": f"Import completed successfully. Imported: {result.get('imported', 0)}, Errors: {result.get('errors', 0)}",
				"result": result
			}

		except Exception as e:
			self.db_set("last_import", now())
			self.db_set("last_import_status", "Failed")
			frappe.log_error(f"Manual import failed: {str(e)}")
			frappe.throw(f"Import failed: {str(e)}")

	@frappe.whitelist()
	def aggressive_import_check(self):
		"""Check feed multiple times in succession to catch intermittent data"""
		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		import requests
		import time

		results = []
		max_attempts = 5

		for attempt in range(1, max_attempts + 1):
			try:
				frappe.logger().info(f"Aggressive check attempt {attempt}/{max_attempts}")

				response = requests.get(self.xml_feed_url, timeout=30)
				response.raise_for_status()
				content = response.text
				content_length = len(content)

				# Log this attempt
				attempt_result = {
					"attempt": attempt,
					"content_length": content_length,
					"timestamp": frappe.utils.now(),
					"has_orders": False,
					"order_count": 0
				}

				if content_length > 100:  # More than empty XML
					try:
						import xml.etree.ElementTree as ET
						root = ET.fromstring(content)
						orders = root.findall('.//ORDER')
						order_count = len(orders)

						attempt_result["has_orders"] = order_count > 0
						attempt_result["order_count"] = order_count

						if order_count > 0:
							# Found orders! Try to import immediately
							frappe.logger().info(f"Found {order_count} orders on attempt {attempt}, triggering import")

							# Save the content for debugging
							frappe.log_error(f"Orders found on attempt {attempt}: {content[:1000]}", "Aggressive Import Success")

							# Trigger import
							import_result = self.trigger_manual_import()
							attempt_result["import_triggered"] = True
							attempt_result["import_result"] = import_result

							results.append(attempt_result)
							break  # Stop checking once we find and process orders

					except Exception as e:
						attempt_result["parse_error"] = str(e)

				results.append(attempt_result)

				# Wait 10 seconds before next attempt (except on last attempt)
				if attempt < max_attempts:
					time.sleep(10)

			except Exception as e:
				results.append({
					"attempt": attempt,
					"error": str(e),
					"timestamp": frappe.utils.now()
				})

		return {
			"success": True,
			"total_attempts": max_attempts,
			"results": results
		}

	@frappe.whitelist()
	def debug_xml_feed(self):
		"""Debug method to show exactly what the XML feed returns"""
		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		try:
			import requests

			# Prepare headers - some feeds require specific headers
			headers = {
				'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
				'Accept': 'application/xml, text/xml, */*',
				'Accept-Encoding': 'gzip, deflate',
				'Connection': 'keep-alive'
			}

			# Add authentication if configured
			auth = None
			if hasattr(self, 'feed_username') and hasattr(self, 'feed_password'):
				if self.get('feed_username') and self.get('feed_password'):
					auth = (self.feed_username, self.feed_password)

			# Fetch XML content
			frappe.log_error(f"Making request to: {self.xml_feed_url}", "XML Feed Debug")
			response = requests.get(self.xml_feed_url, timeout=30, headers=headers, auth=auth)
			response.raise_for_status()

			content = response.text

			# Check if content looks like it might be redirected or contains error messages
			content_lower = content.lower()
			is_likely_error = any(keyword in content_lower for keyword in [
				'login', 'error', 'unauthorized', 'forbidden', 'not found', 'access denied'
			])

			# Check if content is mostly whitespace
			is_mostly_whitespace = len(content.strip()) < 10

			return {
				"url": self.xml_feed_url,
				"status_code": response.status_code,
				"headers": dict(response.headers),
				"content_length": len(content),
				"content_length_stripped": len(content.strip()),
				"raw_content": repr(content),  # Shows all characters including whitespace
				"content": content,
				"is_likely_error": is_likely_error,
				"is_mostly_whitespace": is_mostly_whitespace,
				"auth_used": auth is not None,
				"suggested_actions": self._get_debug_suggestions(content, response)
			}

		except Exception as e:
			return {
				"error": str(e),
				"url": self.xml_feed_url
			}

	def _get_debug_suggestions(self, content, response):
		"""Get suggestions based on the response"""
		suggestions = []

		if len(content.strip()) < 10:
			suggestions.append("Feed appears to be empty - check if there are any orders in the system")
			suggestions.append("Verify the patternId and partnerId parameters are correct")
			suggestions.append("Check if the feed requires a specific time range parameter")

		if 'text/html' in response.headers.get('content-type', ''):
			suggestions.append("Response is HTML instead of XML - might be redirected to login page")

		content_lower = content.lower()
		if any(keyword in content_lower for keyword in ['login', 'unauthorized']):
			suggestions.append("Authentication may be required - check credentials")

		if not suggestions:
			suggestions.append("Feed is accessible but returning minimal content")

		return suggestions

	@frappe.whitelist()
	def check_stream_length(self):
		"""Check XML stream length and basic structure"""
		if not self.xml_feed_url:
			frappe.throw("XML Feed URL is required")

		try:
			import requests
			import xml.etree.ElementTree as ET

			# Fetch XML content
			response = requests.get(self.xml_feed_url, timeout=30)
			response.raise_for_status()

			xml_content = response.text
			content_length = len(xml_content)

			# Log the actual content for debugging
			frappe.log_error(f"XML Feed Content (length: {content_length} bytes): {xml_content[:1000]}", "XML Feed Content Check")

			# Try to parse XML
			try:
				root = ET.fromstring(xml_content)
				root_tag = root.tag
				root_attributes = dict(root.attrib) if root.attrib else {}

				# Count elements based on import type
				if self.import_type == "Orders":
					orders = root.findall('.//ORDER')
					order_count = len(orders)

					# Get sample order info
					sample_info = []
					for i, order in enumerate(orders[:3]):  # First 3 orders
						order_id_elem = order.find('ORDER_ID')
						status_elem = order.find('STATUS')
						items_elem = order.find('ORDER_ITEMS')

						order_id = order_id_elem.text if order_id_elem is not None else "Unknown"
						status = status_elem.text if status_elem is not None else "Unknown"
						item_count = len(items_elem.findall('ITEM')) if items_elem is not None else 0

						sample_info.append({
							"order_id": order_id,
							"status": status,
							"item_count": item_count
						})

				elif self.import_type == "Items":
					items = root.findall('.//ITEM')
					order_count = len(items)
					sample_info = [{"note": "Item import structure check not implemented yet"}]

				else:
					order_count = 0
					sample_info = [{"note": f"Structure check for {self.import_type} not implemented yet"}]

				return {
					"success": True,
					"content_length": content_length,
					"content_size_human": f"{content_length / 1024:.2f} KB" if content_length < 1024*1024 else f"{content_length / (1024*1024):.2f} MB",
					"xml_valid": True,
					"root_tag": root_tag,
					"root_attributes": root_attributes,
					"element_count": order_count,
					"sample_elements": sample_info,
					"first_100_chars": xml_content[:100] + "..." if len(xml_content) > 100 else xml_content,
					"full_xml_content": xml_content  # Include full content for debugging
				}

			except ET.ParseError as e:
				return {
					"success": False,
					"content_length": content_length,
					"content_size_human": f"{content_length / 1024:.2f} KB" if content_length < 1024*1024 else f"{content_length / (1024*1024):.2f} MB",
					"xml_valid": False,
					"parse_error": str(e),
					"first_500_chars": xml_content[:500] + "..." if len(xml_content) > 500 else xml_content
				}

		except requests.RequestException as e:
			frappe.throw(f"Failed to fetch XML feed: {str(e)}")
		except Exception as e:
			frappe.throw(f"Error checking stream: {str(e)}")

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

	@frappe.whitelist()
	def import_from_pasted_content(self, xml_content):
		"""Import XML data from pasted content for debugging purposes"""
		if not self.enabled:
			frappe.throw("XML Import is not enabled. Please enable it first.")

		if not xml_content or not xml_content.strip():
			frappe.throw("XML Content is required")

		try:
			import xml.etree.ElementTree as ET

			# Parse the XML to validate it
			try:
				root = ET.fromstring(xml_content.strip())
				xml_valid = True
				root_tag = root.tag
				parse_error = None
			except ET.ParseError as e:
				xml_valid = False
				root_tag = None
				parse_error = str(e)

			result = {
				"content_length": len(xml_content),
				"xml_valid": xml_valid,
				"root_tag": root_tag,
				"parse_error": parse_error,
				"element_count": 0,
				"processed_items": [],
				"errors": [],
				"import_status": "Failed",
				"debug_info": {}
			}

			if not xml_valid:
				return result

			# Add debug info about XML structure
			result["debug_info"] = {
				"root_tag": root.tag,
				"root_attributes": dict(root.attrib) if root.attrib else {},
				"direct_children": [child.tag for child in root[:10]],  # First 10 children
				"total_children": len(list(root)),
				"all_unique_tags": list(set([elem.tag for elem in root.iter()][:20]))  # First 20 unique tags
			}

			# Count elements based on import type
			if self.import_type == "Items":
				# Look for item elements (could be <item>, <product>, etc.)
				elements = root.findall('.//item') or root.findall('.//product')
				result["element_count"] = len(elements)

				if elements:
					# Process items import
					from xml_importer.xml_importer.item_importer import XMLItemImporter
					importer = XMLItemImporter(company=self.company, config=self)
					import_result = importer.process_xml_content(xml_content)

					result.update({
						"import_status": "Success" if import_result.get("success") else "Failed",
						"processed_items": [f"Item: {item}" for item in import_result.get("imported_items", [])],
						"errors": import_result.get("errors", [])
					})

			elif self.import_type == "Orders":
				# Look for order elements - try multiple possible names
				elements = (root.findall('.//order') or
						   root.findall('.//Order') or
						   root.findall('.//ORDER') or
						   root.findall('.//objednavka') or  # Slovak for order
						   root.findall('.//OBJEDNAVKA'))

				# If still no elements found and root is ORDERS, check direct children
				if not elements and root.tag.upper() == 'ORDERS':
					elements = [child for child in root if child.tag.lower() in ['order', 'objednavka'] or 'order' in child.tag.lower()]

				result["element_count"] = len(elements)

				if elements:
					# Process orders import
					from xml_importer.xml_importer.order_importer import XMLOrderImporter
					importer = XMLOrderImporter(company=self.company, config=self)
					import_result = importer.process_xml_content(xml_content)

					result.update({
						"import_status": "Success" if import_result.get("success") else "Failed",
						"processed_items": [f"Order: {order}" for order in import_result.get("imported_orders", [])],
						"errors": import_result.get("errors", [])
					})

			# Update last import status if successful
			if result["import_status"] == "Success":
				self.db_set("last_import", now())
				self.db_set("last_import_status", "Success")
			else:
				self.db_set("last_import_status", "Failed")

			return result

		except Exception as e:
			error_msg = f"Pasted content import failed: {str(e)}"
			frappe.log_error(error_msg)
			self.db_set("last_import_status", "Failed")

			return {
				"content_length": len(xml_content) if xml_content else 0,
				"xml_valid": False,
				"parse_error": str(e),
				"element_count": 0,
				"processed_items": [],
				"errors": [error_msg],
				"import_status": "Failed"
			}
