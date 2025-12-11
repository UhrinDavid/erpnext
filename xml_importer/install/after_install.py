"""
After install hook for XML Importer
Creates required master data for the app
"""

import frappe


def after_install():
	"""Create required Item Groups and other master data after app installation"""
	create_services_item_group()


def create_services_item_group():
	"""Create Services Item Group if it doesn't exist"""
	try:
		if not frappe.db.exists('Item Group', 'Services'):
			item_group = frappe.get_doc({
				'doctype': 'Item Group',
				'item_group_name': 'Services',
				'parent_item_group': 'All Item Groups',
				'is_group': 0
			})
			item_group.insert(ignore_permissions=True)
			frappe.db.commit()
			frappe.logger().info("Created Services Item Group")
		else:
			frappe.logger().info("Services Item Group already exists")
	except Exception as e:
		frappe.log_error(f"Failed to create Services Item Group: {str(e)}")
		# Don't fail installation if this doesn't work
		pass
