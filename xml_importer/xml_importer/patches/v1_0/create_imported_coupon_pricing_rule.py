"""
Patch: Create Imported Order Coupons Pricing Rule
Creates a 0% discount Pricing Rule for tracking imported order coupons
"""

import frappe


def execute():
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
