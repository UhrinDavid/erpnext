"""
After install hook for XML Importer
Creates required master data for the app
"""

import frappe


def after_install():
	"""Create required Item Groups and other master data after app installation"""
	create_services_item_group()
	create_imported_coupon_pricing_rule()


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


def create_imported_coupon_pricing_rule():
	"""Create 0% discount Pricing Rule for imported coupons"""
	rule_name = "Imported Order Coupons"

	# Check if it already exists
	if frappe.db.exists('Pricing Rule', rule_name):
		return

	# Get first company
	companies = frappe.get_all('Company', limit=1)
	if not companies:
		return

	# Create the pricing rule
	pricing_rule = frappe.get_doc({
		'doctype': 'Pricing Rule',
		'title': rule_name,
		'apply_on': 'Transaction',
		'price_or_product_discount': 'Price',
		'rate_or_discount': 'Discount Percentage',
		'discount_percentage': 0,
		'company': companies[0].name,
		'selling': 1,
		'buying': 0,
		'applicable_for': '',
		'coupon_code_based': 1,
		'priority': 1
	})

	pricing_rule.insert(ignore_permissions=True)
	frappe.db.commit()
