from frappe.model.document import Document
import frappe

class TaxAccountMappingItem(Document):
	"""Child table for mapping tax rates to tax accounts in item imports"""
	
	def validate(self):
		"""Fetch tax rate from the selected account"""
		if self.tax_account and not self.tax_rate:
			# Get tax rate from the account
			account = frappe.get_cached_doc("Account", self.tax_account)
			if account.tax_rate:
				self.tax_rate = account.tax_rate
			
			# Set description from account name if not set
			if not self.description:
				self.description = account.account_name
