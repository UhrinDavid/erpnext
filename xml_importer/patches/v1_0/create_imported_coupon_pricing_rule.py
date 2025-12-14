"""
Patch: Create Imported Order Coupons Pricing Rule
Creates a 0% discount Pricing Rule for tracking imported order coupons
"""

import frappe


def execute():
	"""Create 0% discount Pricing Rule for imported coupons"""
	try:
		rule_name = "Imported Order Coupons"

		if frappe.db.exists('Pricing Rule', rule_name):
			print(f"✓ Pricing Rule '{rule_name}' already exists")
			return

		pricing_rule = frappe.get_doc({
			'doctype': 'Pricing Rule',
			'title': rule_name,
			'apply_on': 'Transaction',
			'price_or_product_discount': 'Price',
			'rate_or_discount': 'Discount Percentage',
			'discount_percentage': 0,
			'company': frappe.get_all('Company', limit=1)[0].name if frappe.get_all('Company', limit=1) else None,
			'selling': 1,
			'applicable_for': 'Customer',
			'coupon_code_based': 1,
			'priority': 1
		})

		pricing_rule.insert(ignore_permissions=True)
		frappe.db.commit()
		print(f"✓ Created Pricing Rule: {rule_name}")

	except Exception as e:
		frappe.log_error(f"Failed to create Imported Coupon Pricing Rule: {str(e)}", "Create Pricing Rule Patch")
		# Don't fail the patch if this doesn't work
		pass
