"""
Patch: Create Services Item Group
Creates the Services Item Group for shipping and billing service items
"""

import frappe


def execute():
	"""Create Services Item Group if it doesn't exist"""
	if frappe.db.exists('Item Group', 'Services'):
		return
	
	try:
		item_group = frappe.get_doc({
			'doctype': 'Item Group',
			'item_group_name': 'Services',
			'parent_item_group': 'All Item Groups',
			'is_group': 0
		})
		item_group.insert(ignore_permissions=True)
		frappe.db.commit()
		print("✓ Created Services Item Group")
	except Exception as e:
		frappe.log_error(f"Failed to create Services Item Group: {str(e)}", "Create Services Item Group Patch")
		# Don't fail the patch if this doesn't work
		pass
