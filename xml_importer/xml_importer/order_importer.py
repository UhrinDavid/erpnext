"""
XML Order Importer for ERPNext
Import orders from XML feeds into ERPNext Sales Orders and Customers
Part of the unified XML Importer app

Author: Herbatica
License: MIT
"""

import frappe
import requests
import xml.etree.ElementTree as ET
from frappe.model.document import Document
from frappe.utils import now, cstr, flt, cint, strip_html_tags, get_datetime
from frappe.utils.file_manager import save_file
import re
import os
from urllib.parse import urlparse
from typing import Dict, List, Optional, Any
from datetime import datetime

class XMLOrderImporter:
    """Import orders from XML feed into ERPNext Sales Orders"""

    def __init__(self, xml_source: str = None, company: str = None, config=None):
        """
        Initialize XML Order Importer

        Args:
            xml_source: URL or file path to XML feed
            company: Company name in ERPNext (default: default company)
            config: XML Import Configuration document (optional)
        """
        self.xml_source = xml_source
        self.company = company or frappe.defaults.get_global_default("company")
        self.config = config
        self.imported_count = 0
        self.updated_count = 0
        self.error_count = 0
        self.errors = []

        frappe.logger().info(f"XMLOrderImporter initialized with config: {self.config.name if self.config else 'None'}")
        if self.config:
            frappe.logger().info(f"Config has update_existing_orders: {getattr(self.config, 'update_existing_orders', 'NOT SET')}")
            frappe.logger().info(f"Config has tax_account_mappings_order: {len(getattr(self.config, 'tax_account_mappings_order', []))} mappings")

        # Initialize required data
        self.ensure_required_data()

        # Load tax account mappings from config
        self._load_tax_account_mappings()

    def _load_tax_account_mappings(self):
        """Load tax rate to account mappings from configuration"""
        self.tax_account_map = {}

        if self.config and hasattr(self.config, 'tax_account_mappings_order'):
            for mapping in self.config.tax_account_mappings_order:
                # Frappe Percent fields store values as decimals (0.23 for 23%)
                # but tax rates in XML are percentages (23.0 for 23%)
                # We need to convert: if value < 1, multiply by 100
                tax_rate = flt(mapping.tax_rate)
                if tax_rate < 1 and tax_rate > 0:
                    tax_rate = tax_rate * 100
                self.tax_account_map[tax_rate] = mapping.tax_account

        frappe.logger().info(f"Loaded {len(self.tax_account_map)} tax account mappings: {self.tax_account_map}")

    def get_tax_account_for_rate(self, tax_rate: float) -> Optional[str]:
        """
        Get the tax account for a given tax rate

        Args:
            tax_rate: Tax rate percentage (e.g., 20.0 for 20%)

        Returns:
            Tax account name or None
        """
        tax_rate = flt(tax_rate)
        return self.tax_account_map.get(tax_rate)

    def _add_taxes_to_order(self, sales_order, tax_rates_in_order: set):
        """
        Add tax rows to Sales Order based on tax rates found in items

        Args:
            sales_order: Sales Order document
            tax_rates_in_order: Set of unique tax rates (as percentages) found in order items
        """
        frappe.logger().info(f"Adding taxes to order - tax_rates: {tax_rates_in_order}, mappings: {self.tax_account_map}")

        if not tax_rates_in_order or not self.tax_account_map:
            frappe.logger().warning(f"Cannot add taxes - tax_rates: {tax_rates_in_order}, mappings: {len(self.tax_account_map) if self.tax_account_map else 0}")
            return

        # Clear existing taxes
        sales_order.taxes = []

        for tax_rate in sorted(tax_rates_in_order):
            tax_account = self.get_tax_account_for_rate(tax_rate)
            frappe.logger().info(f"Looking for account for tax rate {tax_rate}% -> {tax_account}")

            if tax_account:
                # Verify the account exists
                if not frappe.db.exists("Account", tax_account):
                    frappe.logger().warning(f"Tax account {tax_account} not found for rate {tax_rate}%")
                    continue

                # Add tax row
                tax_row = sales_order.append("taxes", {})
                tax_row.charge_type = "On Net Total"
                tax_row.account_head = tax_account
                tax_row.description = f"VAT {tax_rate}%"
                tax_row.rate = tax_rate
                frappe.logger().info(f"Added tax row: {tax_rate}% -> {tax_account}")
            else:
                frappe.logger().warning(f"No tax account mapping found for rate {tax_rate}%")

    def _get_shipping_account(self):
        """
        Get the expense account for shipping charges
        Returns the first matching account from common shipping account names
        """
        shipping_accounts = [
            "Freight and Forwarding Charges - H",
            "Shipping Charges - H",
            "Delivery Charges - H",
            "Freight and Forwarding Charges",
            "Shipping Charges",
            "Delivery Charges"
        ]

        for account in shipping_accounts:
            if frappe.db.exists("Account", account):
                return account

        # Fallback: try to find any Freight account
        freight_accounts = frappe.db.get_all(
            "Account",
            filters={
                "company": self.company,
                "account_name": ["like", "%Freight%"]
            },
            limit=1
        )
        if freight_accounts:
            return freight_accounts[0].name

        frappe.logger().warning("No shipping expense account found, using default")
        return self._get_default_charge_account()

    def _get_billing_account(self):
        """
        Get the expense account for billing charges
        Returns the first matching account from common billing/misc expense account names
        """
        billing_accounts = [
            "Miscellaneous Expenses - H",
            "Other Charges - H",
            "Billing Charges - H",
            "Miscellaneous Expenses",
            "Other Charges",
            "Billing Charges"
        ]

        for account in billing_accounts:
            if frappe.db.exists("Account", account):
                return account

        frappe.logger().warning("No billing expense account found, using default")
        return self._get_default_charge_account()

    def _get_default_charge_account(self):
        """
        Get a default expense account for charges when specific account not found
        """
        # Try to find a general expense account
        default_accounts = [
            "Miscellaneous Expenses - H",
            "Miscellaneous Expenses",
            "Expenses - H",
            "Expenses"
        ]

        for account in default_accounts:
            if frappe.db.exists("Account", account):
                return account

        # Last resort: get any expense account for this company
        expense_account = frappe.db.get_value(
            "Account",
            {
                "company": self.company,
                "root_type": "Expense",
                "is_group": 0
            },
            "name"
        )

        return expense_account

    def ensure_required_data(self):
        """Ensure required master data exists"""
        # Ensure default price list exists
        if not frappe.db.exists("Price List", "Standard Selling"):
            price_list = frappe.get_doc({
                "doctype": "Price List",
                "price_list_name": "Standard Selling",
                "currency": "EUR",
                "selling": 1,
                "buying": 0
            })
            price_list.insert(ignore_permissions=True)

        # Ensure default territory exists
        if not frappe.db.exists("Territory", "Slovakia"):
            territory = frappe.get_doc({
                "doctype": "Territory",
                "territory_name": "Slovakia",
                "parent_territory": "All Territories",
                "is_group": 0
            })
            territory.insert(ignore_permissions=True)

    def map_country_name(self, country_name: str) -> str:
        """Map Slovak/local country names to ERPNext country names"""
        if not country_name:
            return "Slovakia"  # Default country

        country_mapping = {
            # Slovak to English mappings
            "Slovensko": "Slovakia",
            "Česko": "Czech Republic",
            "Česká republika": "Czech Republic",
            "Rakúsko": "Austria",
            "Nemecko": "Germany",
            "Poľsko": "Poland",
            "Maďarsko": "Hungary",
            "Ukrajina": "Ukraine",
            "Francúzsko": "France",
            "Taliansko": "Italy",
            "Španielsko": "Spain",
            "Portugalsko": "Portugal",
            "Holandsko": "Netherlands",
            "Belgicko": "Belgium",
            "Švajčiarsko": "Switzerland",

            # Common variations
            "SK": "Slovakia",
            "CZ": "Czech Republic",
            "AT": "Austria",
            "DE": "Germany",
            "PL": "Poland",
            "HU": "Hungary",
            "UA": "Ukraine",
            "FR": "France",
            "IT": "Italy",
            "ES": "Spain",
            "PT": "Portugal",
            "NL": "Netherlands",
            "BE": "Belgium",
            "CH": "Switzerland"
        }

        # Try exact match first
        mapped_country = country_mapping.get(country_name.strip())
        if mapped_country:
            return mapped_country

        # Try case-insensitive match
        for slovak_name, english_name in country_mapping.items():
            if slovak_name.lower() == country_name.lower().strip():
                return english_name

        # If no mapping found, return original (might be already in English)
        return country_name.strip()

    def fetch_xml_content(self) -> str:
        """Fetch XML content from URL or file"""
        try:
            if self.xml_source.startswith(('http://', 'https://')):
                # Fetch from URL
                response = requests.get(self.xml_source, timeout=60)
                response.raise_for_status()
                content = response.text

                # Log content details for debugging
                content_length = len(content)
                frappe.logger().info(f"Fetched XML content: {content_length} bytes from {self.xml_source}")
                frappe.log_error(f"Order Import - XML Content ({content_length} bytes): {content[:500]}...", "Order Import Content")

                return content
            else:
                # Read from file
                with open(self.xml_source, 'r', encoding='utf-8') as f:
                    content = f.read()
                    frappe.logger().info(f"Read XML content: {len(content)} bytes from file {self.xml_source}")
                    return content
        except Exception as e:
            frappe.throw(f"Failed to fetch XML content: {str(e)}")

    def parse_xml(self, xml_content: str) -> ET.Element:
        """Parse XML content"""
        try:
            # Remove any BOM and clean content
            if xml_content.startswith('\ufeff'):
                xml_content = xml_content[1:]

            root = ET.fromstring(xml_content)
            return root
        except ET.ParseError as e:
            frappe.throw(f"XML parsing failed: {str(e)}")

    def clean_html_content(self, content: str) -> str:
        """Clean HTML content and extract text"""
        if not content:
            return ""

        # Remove CDATA
        content = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', content, flags=re.DOTALL)

        # Strip HTML tags but preserve line breaks
        content = strip_html_tags(content)

        # Clean up extra whitespace
        content = re.sub(r'\s+', ' ', content).strip()

        return content

    def clean_name(self, name: str) -> str:
        """Clean name to remove invalid characters"""
        if not name:
            return ""

        # Remove HTML tags first
        name = strip_html_tags(name)

        # Remove special characters that ERPNext doesn't allow in names
        name = re.sub(r'[<>&"\']', '', name)

        # Replace multiple spaces with single space
        name = re.sub(r'\s+', ' ', name)

        # Trim and return
        return name.strip()

    def get_element_text(self, parent: ET.Element, tag_name: str) -> str:
        """Get text content of XML element"""
        element = parent.find(tag_name)
        return element.text.strip() if element is not None and element.text else ""

    def parse_decimal(self, value: str) -> float:
        """Parse decimal value, handling Slovak number format (comma as decimal separator)"""
        if not value:
            return 0.0

        # Replace comma with dot for decimal separator
        value = value.replace(',', '.')

        try:
            return flt(value)
        except:
            return 0.0

    def parse_order(self, order_elem: ET.Element) -> Dict[str, Any]:
        """Parse ORDER XML element to dictionary with English property names"""
        order_data = {}

        # Basic order information
        order_data['external_order_id'] = self.get_element_text(order_elem, 'ORDER_ID')
        order_data['order_code'] = self.get_element_text(order_elem, 'CODE')
        order_data['order_date'] = self.get_element_text(order_elem, 'DATE')
        order_data['order_status'] = self.get_element_text(order_elem, 'STATUS')

        # Currency information
        currency_elem = order_elem.find('CURRENCY')
        if currency_elem is not None:
            order_data['currency_code'] = self.get_element_text(currency_elem, 'CODE')
            order_data['exchange_rate'] = self.parse_decimal(self.get_element_text(currency_elem, 'EXCHANGE_RATE'))

        # Customer information
        customer_elem = order_elem.find('CUSTOMER')
        if customer_elem is not None:
            order_data['customer_email'] = self.get_element_text(customer_elem, 'EMAIL')
            order_data['customer_phone'] = self.get_element_text(customer_elem, 'PHONE')
            order_data['ip_address'] = self.get_element_text(customer_elem, 'IP_ADDRESS')

            # Billing address
            billing_elem = customer_elem.find('BILLING_ADDRESS')
            if billing_elem is not None:
                order_data['billing_address'] = {
                    'customer_name': self.clean_name(self.get_element_text(billing_elem, 'NAME')),
                    'company_name': self.clean_name(self.get_element_text(billing_elem, 'COMPANY')),
                    'street': self.get_element_text(billing_elem, 'STREET'),
                    'house_number': self.get_element_text(billing_elem, 'HOUSENUMBER'),
                    'city': self.get_element_text(billing_elem, 'CITY'),
                    'postal_code': self.get_element_text(billing_elem, 'ZIP'),
                    'country': self.map_country_name(self.get_element_text(billing_elem, 'COUNTRY')),
                    'company_id': self.get_element_text(billing_elem, 'COMPANY_ID'),
                    'vat_id': self.get_element_text(billing_elem, 'VAT_ID'),
                    'customer_id_number': self.get_element_text(billing_elem, 'CUSTOMER_IDENTIFICATION_NUMBER')
                }

            # Shipping address
            shipping_elem = customer_elem.find('SHIPPING_ADDRESS')
            if shipping_elem is not None:
                order_data['shipping_address'] = {
                    'customer_name': self.clean_name(self.get_element_text(shipping_elem, 'NAME')),
                    'company_name': self.clean_name(self.get_element_text(shipping_elem, 'COMPANY')),
                    'street': self.get_element_text(shipping_elem, 'STREET'),
                    'house_number': self.get_element_text(shipping_elem, 'HOUSENUMBER'),
                    'city': self.get_element_text(shipping_elem, 'CITY'),
                    'postal_code': self.get_element_text(shipping_elem, 'ZIP'),
                    'country': self.map_country_name(self.get_element_text(shipping_elem, 'COUNTRY'))
                }

        # Order details
        order_data['customer_remark'] = self.clean_html_content(self.get_element_text(order_elem, 'REMARK'))
        order_data['shop_remark'] = self.clean_html_content(self.get_element_text(order_elem, 'SHOP_REMARK'))
        order_data['referer'] = self.clean_html_content(self.get_element_text(order_elem, 'REFERER'))
        order_data['package_number'] = self.get_element_text(order_elem, 'PACKAGE_NUMBER')
        order_data['total_weight'] = self.parse_decimal(self.get_element_text(order_elem, 'WEIGHT'))

        # Total pricing
        total_price_elem = order_elem.find('TOTAL_PRICE')
        if total_price_elem is not None:
            order_data['total_with_tax'] = self.parse_decimal(self.get_element_text(total_price_elem, 'WITH_VAT'))
            order_data['total_without_tax'] = self.parse_decimal(self.get_element_text(total_price_elem, 'WITHOUT_VAT'))
            order_data['total_tax'] = self.parse_decimal(self.get_element_text(total_price_elem, 'VAT'))
            order_data['rounding'] = self.parse_decimal(self.get_element_text(total_price_elem, 'ROUNDING'))
            order_data['amount_to_pay'] = self.parse_decimal(self.get_element_text(total_price_elem, 'PRICE_TO_PAY'))
            order_data['is_paid'] = cint(self.get_element_text(total_price_elem, 'PAID'))
            order_data['amount_paid'] = self.parse_decimal(self.get_element_text(total_price_elem, 'AMOUNT_PAID'))

        # Order items
        order_items = []
        items_elem = order_elem.find('ORDER_ITEMS')
        if items_elem is not None:
            for item in items_elem.findall('ITEM'):
                item_data = self.parse_order_item(item)
                order_items.append(item_data)

        order_data['order_items'] = order_items
        order_data['source_name'] = self.get_element_text(order_elem, 'SOURCE_NAME')

        return order_data

    def parse_order_item(self, item_elem: ET.Element) -> Dict[str, Any]:
        """Parse ORDER ITEM element"""
        item_data = {}

        # Basic item information
        item_data['item_type'] = self.get_element_text(item_elem, 'TYPE')  # product, shipping, billing
        item_data['item_name'] = self.clean_name(self.get_element_text(item_elem, 'NAME'))
        item_data['quantity'] = self.parse_decimal(self.get_element_text(item_elem, 'AMOUNT'))
        item_data['item_code'] = self.get_element_text(item_elem, 'CODE')
        item_data['variant_name'] = self.get_element_text(item_elem, 'VARIANT_NAME')
        item_data['barcode'] = self.get_element_text(item_elem, 'EAN')
        item_data['plu'] = self.get_element_text(item_elem, 'PLU')
        item_data['manufacturer'] = self.get_element_text(item_elem, 'MANUFACTURER')
        item_data['supplier'] = self.get_element_text(item_elem, 'SUPPLIER')
        item_data['unit'] = self.get_element_text(item_elem, 'UNIT')
        item_data['weight'] = self.parse_decimal(self.get_element_text(item_elem, 'WEIGHT'))
        item_data['item_status'] = self.get_element_text(item_elem, 'STATUS')
        item_data['discount'] = self.parse_decimal(self.get_element_text(item_elem, 'DISCOUNT'))

        # Unit pricing
        unit_price_elem = item_elem.find('UNIT_PRICE')
        if unit_price_elem is not None:
            item_data['unit_price_with_tax'] = self.parse_decimal(self.get_element_text(unit_price_elem, 'WITH_VAT'))
            item_data['unit_price_without_tax'] = self.parse_decimal(self.get_element_text(unit_price_elem, 'WITHOUT_VAT'))
            item_data['unit_tax'] = self.parse_decimal(self.get_element_text(unit_price_elem, 'VAT'))
            item_data['tax_rate'] = self.parse_decimal(self.get_element_text(unit_price_elem, 'VAT_RATE'))

        # Total pricing
        total_price_elem = item_elem.find('TOTAL_PRICE')
        if total_price_elem is not None:
            item_data['total_price_with_tax'] = self.parse_decimal(self.get_element_text(total_price_elem, 'WITH_VAT'))
            item_data['total_price_without_tax'] = self.parse_decimal(self.get_element_text(total_price_elem, 'WITHOUT_VAT'))
            item_data['total_tax'] = self.parse_decimal(self.get_element_text(total_price_elem, 'VAT'))
            item_data['item_tax_rate'] = self.parse_decimal(self.get_element_text(total_price_elem, 'VAT_RATE'))

        return item_data

    def create_or_update_customer(self, order_data: Dict[str, Any]) -> str:
        """Create or update customer and return customer name"""
        try:
            billing_address = order_data.get('billing_address', {})
            customer_email = order_data.get('customer_email', '')

            # Determine customer name - prefer company name if available
            customer_name = billing_address.get('company_name') or billing_address.get('customer_name')
            if not customer_name:
                customer_name = customer_email.split('@')[0] if customer_email else f"Customer-{order_data.get('external_order_id', 'Unknown')}"

            # Clean customer name
            customer_name = self.clean_name(customer_name)

            # Check if customer exists by email or name
            existing_customer = None
            if customer_email:
                existing_customer = frappe.db.get_value("Customer", {"email_id": customer_email}, "name")

            if not existing_customer and customer_name:
                existing_customer = frappe.db.get_value("Customer", {"customer_name": customer_name}, "name")

            if existing_customer:
                # Update existing customer
                customer_doc = frappe.get_doc("Customer", existing_customer)
                is_update = True
            else:
                # Create new customer
                customer_doc = frappe.new_doc("Customer")
                is_update = False

            # Update customer fields
            customer_doc.customer_name = customer_name
            customer_doc.customer_type = "Company" if billing_address.get('company_name') else "Individual"
            customer_doc.customer_group = "All Customer Groups"
            customer_doc.territory = "Slovakia"

            if customer_email:
                customer_doc.email_id = customer_email

            if order_data.get('customer_phone'):
                customer_doc.mobile_no = order_data.get('customer_phone')

            # Tax ID information
            if billing_address.get('vat_id'):
                customer_doc.tax_id = billing_address.get('vat_id')

            if billing_address.get('company_id'):
                customer_doc.customer_details = f"Company ID: {billing_address.get('company_id')}"

            # Save customer
            if is_update:
                customer_doc.save(ignore_permissions=True)
            else:
                customer_doc.insert(ignore_permissions=True)

            # Create or update addresses
            self.create_customer_addresses(customer_doc.name, order_data)

            return customer_doc.name

        except Exception as e:
            frappe.log_error(f"Failed to create/update customer: {str(e)}")
            return f"Customer-{order_data.get('external_order_id', 'Unknown')}"

    def create_customer_addresses(self, customer_name: str, order_data: Dict[str, Any]):
        """Create customer addresses"""
        try:
            billing_address = order_data.get('billing_address', {})
            shipping_address = order_data.get('shipping_address', {})

            # Create billing address
            if billing_address.get('customer_name') or billing_address.get('street'):
                self.create_address(customer_name, billing_address, "Billing")

            # Create shipping address if different from billing
            if (shipping_address.get('customer_name') or shipping_address.get('street')) and shipping_address != billing_address:
                self.create_address(customer_name, shipping_address, "Shipping")

        except Exception as e:
            frappe.log_error(f"Failed to create customer addresses: {str(e)}")

    def create_address(self, customer_name: str, address_data: Dict[str, Any], address_type: str):
        """Create address for customer"""
        try:
            # Create address line
            address_line1 = address_data.get('street', '')
            if address_data.get('house_number'):
                address_line1 += f" {address_data.get('house_number')}"

            if not address_line1:
                return

            # Check if address already exists
            address_title = f"{customer_name}-{address_type}"
            existing_address = frappe.db.get_value("Address", {"address_title": address_title}, "name")

            if existing_address:
                return existing_address

            # Map country name to ERPNext country
            country = self.map_country_name(address_data.get('country', ''))

            # Create new address
            address_doc = frappe.get_doc({
                "doctype": "Address",
                "address_title": address_title,
                "address_type": address_type,
                "address_line1": address_line1,
                "city": address_data.get('city', ''),
                "pincode": address_data.get('postal_code', ''),
                "country": country,
                "links": [{
                    "link_doctype": "Customer",
                    "link_name": customer_name
                }]
            })

            address_doc.insert(ignore_permissions=True)
            return address_doc.name

        except Exception as e:
            frappe.log_error(f"Failed to create address: {str(e)}")
            return None

    def create_or_update_order(self, order_data: Dict[str, Any]) -> tuple:
        """Create or update ERPNext Sales Order

        Returns:
            tuple: (status, reason) where status is:
                'imported' - new order created
                'skipped' - order skipped (cancelled or already exists)
                'error' - failed to process
            and reason is the skip/error reason
        """
        try:
            external_order_id = order_data.get('external_order_id')
            if not external_order_id:
                self.add_error("Missing external order ID")
                return ('error', 'missing_id')

            # Skip cancelled/storno orders
            order_status = order_data.get('order_status', '').lower()
            if 'storno' in order_status or 'cancel' in order_status or 'zrušen' in order_status:
                frappe.logger().info(f"Skipping cancelled order {external_order_id} with status: {order_data.get('order_status')}")
                return ('skipped', 'cancelled')

            # Check if order exists
            existing_order = frappe.db.get_value("Sales Order", {"po_no": external_order_id}, "name")

            if existing_order:
                # Check if we should update existing orders
                update_existing = self.config and getattr(self.config, 'update_existing_orders', 0)
                frappe.logger().info(f"Order {external_order_id} exists as {existing_order}. Update flag: {update_existing}")
                if update_existing:
                    frappe.logger().info(f"Updating existing order {existing_order}...")
                    return self._update_existing_order(existing_order, order_data)
                else:
                    # Skip if order already exists and update is disabled
                    frappe.logger().info(f"Order {external_order_id} already exists as {existing_order}, skipping (update disabled)")
                    return ('skipped', 'exists')

            # Create customer
            customer_name = self.create_or_update_customer(order_data)

            # Parse order date
            order_date = get_datetime(order_data.get('order_date', now()))

            # Create Sales Order
            sales_order = frappe.new_doc("Sales Order")
            sales_order.customer = customer_name
            sales_order.transaction_date = order_date.date()
            sales_order.delivery_date = order_date.date()
            sales_order.company = self.company
            sales_order.currency = order_data.get('currency_code', 'EUR')
            sales_order.selling_price_list = "Standard Selling"

            # Use po_no field for external order ID tracking (standard ERPNext field)
            sales_order.po_no = external_order_id
            sales_order.po_date = order_date.date()

            # Add customer remarks
            if order_data.get('customer_remark'):
                sales_order.remarks = order_data.get('customer_remark')

            # Process order items
            product_items_added = 0
            all_item_types = []
            failed_items = []
            tax_rates_in_order = set()  # Track unique tax rates found in items

            # Add ALL items as line items (products, shipping, billing, discount)
            for item_data in order_data.get('order_items', []):
                item_type = item_data.get('item_type', '').lower()
                all_item_types.append(f"{item_type}:{item_data.get('item_code', 'no_code')}")

                # Add all items (product, set, shipping, billing, discount) as line items
                if self.add_order_item(sales_order, item_data):
                    product_items_added += 1
                    # Track tax rate from this item
                    tax_rate = flt(item_data.get('tax_rate') or item_data.get('item_tax_rate', 0))
                    if tax_rate > 0:
                        tax_rates_in_order.add(tax_rate)
                    frappe.logger().info(f"Added {item_type} item: {item_data.get('item_name', 'unknown')} - tax rate {tax_rate}%")
                else:
                    failed_items.append(item_data.get('item_code', item_data.get('item_name', 'unknown')))

            frappe.logger().info(f"Collected tax rates from order items: {tax_rates_in_order}")

            # Only create order if we have items
            if product_items_added == 0:
                frappe.logger().warning(f"No valid items for order {external_order_id}: items={all_item_types}, failed={failed_items}")
                return ('skipped', 'no_products')

            # Add taxes based on tax rates found in items
            self._add_taxes_to_order(sales_order, tax_rates_in_order)

            # Set totals
            sales_order.run_method("calculate_taxes_and_totals")

            # Save order
            sales_order.insert(ignore_permissions=True)

            # Auto-submit if configuration allows
            if self.config and self.config.get('auto_submit_orders'):
                try:
                    sales_order.submit()
                    frappe.logger().info(f"Auto-submitted Sales Order: {sales_order.name}")
                except Exception as e:
                    frappe.logger().warning(f"Failed to auto-submit order {sales_order.name}: {str(e)}")
                    # Continue even if submit fails - order is still created
            else:
                frappe.logger().info(f"Order {sales_order.name} saved as draft (auto-submit disabled)")

            self.imported_count += 1
            frappe.logger().info(f"Created Sales Order: {sales_order.name} for external order {external_order_id}")

            return ('imported', sales_order.name)

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to process order {order_data.get('external_order_id', 'Unknown')}: {str(e)}"
            self.add_error(error_msg)
            frappe.log_error(error_msg)
            return ('error', str(e))

    def _update_existing_order(self, order_name: str, order_data: Dict[str, Any]) -> tuple:
        """Update an existing Sales Order with new data from XML

        Args:
            order_name: Name of existing Sales Order
            order_data: Order data from XML

        Returns:
            tuple: (status, order_name) where status is 'updated' or 'error'
        """
        try:
            # Get the existing order
            sales_order = frappe.get_doc("Sales Order", order_name)

            # Only update if order is in Draft status
            if sales_order.docstatus != 0:
                frappe.logger().warning(f"Cannot update {order_name} - already submitted or cancelled")
                return ('skipped', 'not_draft')

            # Clear existing items and taxes
            sales_order.items = []
            sales_order.taxes = []

            # Process order items
            product_items_added = 0
            tax_rates_in_order = set()

            for item_data in order_data.get('order_items', []):
                item_type = item_data.get('item_type', '').lower()

                # Add all items (product, set, shipping, billing, discount) as line items
                if self.add_order_item(sales_order, item_data):
                    product_items_added += 1
                    # Track tax rate from this item
                    tax_rate = flt(item_data.get('tax_rate') or item_data.get('item_tax_rate', 0))
                    if tax_rate > 0:
                        tax_rates_in_order.add(tax_rate)

            if product_items_added == 0:
                frappe.logger().warning(f"No valid items for order update {order_name}")
                return ('error', 'no_products')

            # Add taxes based on tax rates found in items
            self._add_taxes_to_order(sales_order, tax_rates_in_order)            # Update customer remarks if present
            if order_data.get('customer_remark'):
                sales_order.remarks = order_data.get('customer_remark')

            # Recalculate totals
            sales_order.run_method("calculate_taxes_and_totals")

            # Save updated order
            sales_order.save(ignore_permissions=True)

            self.updated_count += 1
            frappe.logger().info(f"Updated Sales Order: {sales_order.name}")

            return ('updated', sales_order.name)

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to update order {order_name}: {str(e)}"
            self.add_error(error_msg)
            frappe.log_error(error_msg)
            return ('error', str(e))

    def add_order_item(self, sales_order: Document, item_data: Dict[str, Any]) -> bool:
        """Add item to sales order"""
        try:
            item_code = item_data.get('item_code')
            if not item_code:
                frappe.logger().warning(f"No item code found for item: {item_data.get('item_name')}")
                return False

            # Check if item exists in ERPNext
            if not frappe.db.exists("Item", item_code):
                # Create a placeholder item if it doesn't exist
                if not self.create_placeholder_item(item_code, item_data):
                    return False

            # Add item to sales order
            item_row = sales_order.append("items", {})
            item_row.item_code = item_code

            # Ensure item_name is set (required field) - fallback to item_code if empty
            item_name = item_data.get('item_name') or ''
            item_row.item_name = item_name.strip() if item_name.strip() else item_code

            # Parse quantity (handle comma decimal separator)
            qty = self.parse_decimal(str(item_data.get('quantity', 1)))
            item_row.qty = qty if qty > 0 else 1

            # Get rate - try multiple field names and parse decimal
            rate = 0
            rate_fields = ['unit_price_without_tax', 'unit_price_without_vat', 'total_price_without_tax', 'unit_price_with_tax']
            for field in rate_fields:
                if item_data.get(field):
                    rate = self.parse_decimal(str(item_data.get(field)))
                    if rate > 0:
                        break

            item_row.rate = rate

            # Calculate amount
            item_row.amount = item_row.qty * item_row.rate

            # Set UOM - get from Item master if not in XML data
            uom = item_data.get('unit', '')
            if uom == 'ks':  # Slovak for pieces
                uom = 'Nos'

            # If no UOM from XML, get stock UOM from Item master
            if not uom:
                item_doc = frappe.get_cached_doc('Item', item_code)
                uom = item_doc.stock_uom or 'Nos'

            item_row.uom = uom

            # Set warehouse only for stock items (not for shipping/billing/discount service items)
            item_type = item_data.get('item_type', '').lower()
            is_service_item = item_type in ['shipping', 'billing', 'discount']

            if not is_service_item:
                default_warehouse = frappe.db.get_single_value("Stock Settings", "default_warehouse")
                if default_warehouse:
                    item_row.warehouse = default_warehouse

            # Add item tax template for service items (shipping/billing) based on VAT rate
            if is_service_item and item_data.get('vat_rate'):
                tax_rate = self.parse_decimal(str(item_data.get('vat_rate')))
                if tax_rate > 0:
                    tax_template = self.get_or_create_item_tax_template(tax_rate)
                    if tax_template:
                        item_row.item_tax_template = tax_template
                        frappe.logger().info(f"Applied tax template '{tax_template}' to {item_type} item {item_code} in order")

            frappe.logger().info(f"Added {item_type} item {item_code} qty={item_row.qty} rate={item_row.rate} to sales order")
            return True

        except Exception as e:
            frappe.log_error(f"Failed to add order item {item_data.get('item_code')}: {str(e)}")
            return False

    def create_placeholder_item(self, item_code: str, item_data: Dict[str, Any]) -> bool:
        """Create placeholder item if it doesn't exist"""
        try:
            # Clean item name
            item_name = item_data.get('item_name', item_code)
            if len(item_name) > 140:  # ERPNext limit
                item_name = item_name[:137] + "..."

            # Determine if this is a service item (shipping/billing/discount) or stock item
            item_type = item_data.get('item_type', '').lower()
            is_service = item_type in ['shipping', 'billing', 'discount']

            item_doc = frappe.get_doc({
                "doctype": "Item",
                "item_code": item_code,
                "item_name": item_name,
                "item_group": "Services" if is_service else "All Item Groups",
                "stock_uom": "Nos" if not is_service else "Unit",
                "is_stock_item": 0 if is_service else 1,
                "is_sales_item": 1,
                "is_purchase_item": 0,
                "description": f"Auto-created from order import: {item_name}"
            })

            # Add barcode if available (only for stock items)
            if not is_service and item_data.get('barcode'):
                item_doc.append("barcodes", {
                    "barcode": item_data.get('barcode'),
                    "barcode_type": "EAN"
                })

            # Add tax template for service items (shipping/billing) based on VAT rate from XML
            if is_service and item_data.get('vat_rate'):
                tax_rate = self.parse_decimal(str(item_data.get('vat_rate')))
                if tax_rate > 0:
                    tax_template = self.get_or_create_item_tax_template(tax_rate)
                    if tax_template:
                        item_doc.append("taxes", {
                            "item_tax_template": tax_template
                        })
                        frappe.logger().info(f"Added tax template '{tax_template}' to service item {item_code}")

            item_doc.insert(ignore_permissions=True)
            frappe.logger().info(f"Created placeholder {'service' if is_service else 'stock'} item: {item_code}")
            return True

        except Exception as e:
            frappe.log_error(f"Failed to create placeholder item {item_code}: {str(e)}")
            return False

    def get_or_create_item_tax_template(self, tax_rate: float) -> str:
        """
        Get or create Item Tax Template for the given VAT rate
        Uses tax account mapping from config if available

        Args:
            tax_rate: VAT rate percentage (e.g., 23.0)

        Returns:
            str: Name of the Item Tax Template (e.g., "SK DPH 23% - H")
        """
        try:
            # Get company abbreviation for naming
            company_abbr = frappe.get_cached_value("Company", self.company, "abbr")

            # Get tax account - prefer mapped account, fallback to generic VAT account
            tax_account = None
            if hasattr(self, 'tax_account_map') and tax_rate in self.tax_account_map:
                tax_account = self.tax_account_map[tax_rate]
                frappe.logger().info(f"Using mapped tax account '{tax_account}' for {tax_rate}%")
            else:
                # Fallback: search for VAT account
                vat_accounts = frappe.get_all(
                    "Account",
                    filters={
                        "company": self.company,
                        "account_type": "Tax",
                        "is_group": 0
                    },
                    fields=["name", "account_name"]
                )
                if vat_accounts:
                    tax_account = vat_accounts[0].name
                    frappe.logger().info(f"Using fallback VAT account '{tax_account}' for {tax_rate}%")

            if not tax_account:
                frappe.logger().error(f"No tax account found for company {self.company} and rate {tax_rate}%")
                return None

            # Get account name for better template naming
            account_name = frappe.db.get_value("Account", tax_account, "account_name") or "VAT"

            # Create template name based on account name (e.g., "SK DPH 23% - H")
            template_title = f"{account_name}"
            template_name = f"{template_title} - {company_abbr}"

            # Check if template already exists with this exact tax account
            if frappe.db.exists("Item Tax Template", template_name):
                # Verify it has the correct tax account
                existing_doc = frappe.get_doc("Item Tax Template", template_name)
                if existing_doc.taxes and existing_doc.taxes[0].tax_type == tax_account:
                    return template_name

            # Create new template
            template_doc = frappe.get_doc({
                "doctype": "Item Tax Template",
                "title": template_title,
                "company": self.company
            })
            template_doc.append("taxes", {
                "tax_type": tax_account,
                "tax_rate": tax_rate
            })
            template_doc.insert(ignore_permissions=True)
            frappe.db.commit()
            frappe.logger().info(f"Created Item Tax Template: {template_name} with account {tax_account} and rate {tax_rate}%")
            return template_name

        except Exception as e:
            frappe.log_error(f"Failed to create tax template for rate {tax_rate}%: {str(e)}")
            return None

    def add_error(self, error_msg: str) -> None:
        """Add error to error list"""
        self.errors.append(error_msg)
        self.error_count += 1

    def process_xml_content(self, xml_content: str) -> Dict[str, Any]:
        """Process XML content directly (for pasted content debugging)"""
        try:
            frappe.logger().info("Processing pasted XML content for order import")

            # Check if content is meaningful
            if not xml_content or len(xml_content.strip()) < 50:
                return {
                    "success": False,
                    "error": f"XML content is empty or too small: {len(xml_content) if xml_content else 0} bytes",
                    "imported_orders": [],
                    "errors": ["Content too small or empty"]
                }

            # Parse XML
            try:
                root = ET.fromstring(xml_content.strip())
                frappe.logger().info(f"Successfully parsed XML with root element: {root.tag}")
            except ET.ParseError as e:
                error_msg = f"Failed to parse XML: {str(e)}"
                frappe.logger().error(error_msg)
                return {
                    "success": False,
                    "error": error_msg,
                    "imported_orders": [],
                    "errors": [error_msg]
                }

            # Process orders
            imported_orders = []
            processing_errors = []

            # Find order elements - try multiple possible names
            order_elements = (root.findall('.//order') or
                            root.findall('.//Order') or
                            root.findall('.//ORDER') or
                            root.findall('.//objednavka') or  # Slovak for order
                            root.findall('.//OBJEDNAVKA'))

            # If still no elements found and root is ORDERS, check direct children
            if not order_elements and root.tag.upper() == 'ORDERS':
                order_elements = [child for child in root if child.tag.lower() in ['order', 'objednavka'] or 'order' in child.tag.lower()]

            frappe.logger().info(f"Found {len(order_elements)} order elements to process")
            frappe.logger().info(f"Root tag: {root.tag}, Direct children: {[child.tag for child in root[:5]]}")  # Log first 5 children

            for i, order_elem in enumerate(order_elements, 1):
                try:
                    frappe.logger().info(f"Processing order {i}/{len(order_elements)}")

                    # Get order ID for debugging
                    order_id = (order_elem.findtext('ORDER_ID') or
                               order_elem.findtext('order_id') or
                               order_elem.findtext('ID') or
                               f"order_{i}")

                    frappe.logger().info(f"Processing order with ID: {order_id}")

                    # Parse XML element to dictionary first
                    order_data = self.parse_order(order_elem)
                    success = self.create_or_update_order(order_data)
                    if success:
                        imported_orders.append(order_id)
                        frappe.logger().info(f"Successfully processed order: {order_id}")
                        self.imported_count += 1
                    else:
                        error_msg = f"Failed to create order document for order ID: {order_id}"
                        processing_errors.append(error_msg)
                        frappe.logger().warning(error_msg)

                except Exception as e:
                    error_msg = f"Failed to process order {order_id if 'order_id' in locals() else i}: {str(e)}"
                    processing_errors.append(error_msg)
                    frappe.logger().error(error_msg)
                    frappe.log_error(f"Order processing error: {str(e)}", "XML Order Import")

            # Prepare summary
            success = len(imported_orders) > 0
            summary = {
                "success": success,
                "imported_orders": imported_orders,
                "errors": processing_errors,
                "total_processed": len(order_elements),
                "successfully_imported": len(imported_orders),
                "error_count": len(processing_errors)
            }

            frappe.logger().info(f"Pasted XML order processing completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"XML content processing failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported_orders": [],
                "errors": [error_msg]
            }

    def import_from_xml(self) -> Dict[str, Any]:
        """Main import function"""
        try:
            frappe.logger().info(f"Starting XML order import from: {self.xml_source}")

            # Fetch and parse XML
            xml_content = self.fetch_xml_content()

            # Check if content is meaningful
            if not xml_content or len(xml_content.strip()) < 50:
                frappe.logger().warning(f"XML content is empty or too small: {len(xml_content) if xml_content else 0} bytes")
                return {
                    "success": True,  # It's "successful" but no data to process
                    "imported": 0,
                    "updated": 0,
                    "errors": 0,
                    "error_messages": ["XML feed returned empty or minimal content"],
                    "total_processed": 0,
                    "successfully_processed": 0
                }

            root = self.parse_xml(xml_content)

            # Find all ORDER elements
            orders = root.findall('.//ORDER')

            frappe.logger().info(f"Found {len(orders)} orders to process")

            # Process each order
            processed_count = 0
            for order in orders:
                try:
                    order_data = self.parse_order(order)
                    order_id = order_data.get('external_order_id', 'Unknown')
                    order_status = order_data.get('order_status', 'Unknown')

                    frappe.logger().info(f"Processing order {order_id} with status: {order_status}")

                    # Log order items for debugging
                    items = order_data.get('order_items', [])
                    product_items = [item for item in items if item.get('item_type') == 'product']
                    frappe.logger().info(f"Order {order_id} has {len(items)} total items, {len(product_items)} product items")

                    if self.create_or_update_order(order_data):
                        processed_count += 1

                except Exception as e:
                    error_msg = f"Error processing ORDER ID {order.find('ORDER_ID').text if order.find('ORDER_ID') is not None else 'Unknown'}: {str(e)}"
                    self.add_error(error_msg)
                    frappe.log_error(error_msg)
                    continue

            # Return summary
            summary = {
                "success": True,
                "imported": self.imported_count,
                "updated": self.updated_count,
                "errors": self.error_count,
                "error_messages": self.errors[:10],  # First 10 errors
                "total_processed": len(orders),
                "successfully_processed": processed_count
            }

            frappe.logger().info(f"Order import completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"XML order import failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported": self.imported_count,
                "updated": self.updated_count,
                "errors": self.error_count
            }


# Public API functions
@frappe.whitelist()
def import_xml_orders(xml_source: str, company: str = None) -> Dict[str, Any]:
    """
    Import orders from XML feed using SAX parser with Redis queue

    This function now always uses the memory-optimized SAX parser with Redis queue
    instead of the DOM-based ElementTree parser.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)

    Returns:
        Dict with import results
    """
    # Always use SAX parser with Redis queue for memory efficiency
    return import_xml_orders_sax(xml_source, company)


# ================================
# SAX PARSER IMPLEMENTATION FOR ORDERS
# ================================

import xml.sax
import redis
import json
import time
import os
import gc
import psutil
from datetime import datetime

try:
    REDIS_AVAILABLE = True
    import redis
except ImportError:
    REDIS_AVAILABLE = False


class SAXOrderHandler(xml.sax.ContentHandler):
    """SAX handler for processing XML order data"""

    def __init__(self, redis_client=None, queue_name=None):
        self.redis_client = redis_client
        self.queue_name = queue_name or f"xml_orders_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.current_order = {}
        self.current_item = {}
        self.current_address = {}
        self.element_stack = []
        self.current_data = ""
        self.orders_processed = 0

        # Context flags
        self.in_order = False
        self.in_customer = False
        self.in_billing_address = False
        self.in_shipping_address = False
        self.in_currency = False
        self.in_order_items = False
        self.in_item = False
        self.in_unit_price = False
        self.in_total_price = False
        self.in_item_total_price = False

    def startElement(self, name, attrs):
        self.element_stack.append(name.upper())
        self.current_data = ""

        name_upper = name.upper()

        if name_upper == "ORDER":
            self.in_order = True
            self.current_order = {
                "order_items": [],
                "billing_address": {},
                "shipping_address": {}
            }
        elif name_upper == "CUSTOMER" and self.in_order:
            self.in_customer = True
        elif name_upper == "BILLING_ADDRESS" and self.in_customer:
            self.in_billing_address = True
            self.current_address = {}
        elif name_upper == "SHIPPING_ADDRESS" and self.in_customer:
            self.in_shipping_address = True
            self.current_address = {}
        elif name_upper == "CURRENCY" and self.in_order and not self.in_item:
            self.in_currency = True
        elif name_upper == "ORDER_ITEMS" and self.in_order:
            self.in_order_items = True
        elif name_upper == "ITEM" and self.in_order_items:
            self.in_item = True
            self.current_item = {}
        elif name_upper == "UNIT_PRICE" and self.in_item:
            self.in_unit_price = True
        elif name_upper == "TOTAL_PRICE":
            if self.in_item:
                self.in_item_total_price = True
            elif self.in_order and not self.in_order_items:
                self.in_total_price = True

    def endElement(self, name):
        name_upper = name.upper()
        data = self.current_data.strip()

        if name_upper == "ORDER" and self.in_order:
            # Queue order for processing
            if self.redis_client and self.current_order.get("external_order_id"):
                order_json = json.dumps(self.current_order, ensure_ascii=False)
                self.redis_client.lpush(self.queue_name, order_json)
                self.orders_processed += 1
            self.current_order = {}
            self.in_order = False

        elif name_upper == "CUSTOMER":
            self.in_customer = False

        elif name_upper == "BILLING_ADDRESS" and self.in_billing_address:
            self.current_order["billing_address"] = self.current_address.copy()
            self.current_address = {}
            self.in_billing_address = False

        elif name_upper == "SHIPPING_ADDRESS" and self.in_shipping_address:
            self.current_order["shipping_address"] = self.current_address.copy()
            self.current_address = {}
            self.in_shipping_address = False

        elif name_upper == "CURRENCY" and self.in_currency:
            self.in_currency = False

        elif name_upper == "ORDER_ITEMS":
            self.in_order_items = False

        elif name_upper == "ITEM" and self.in_item:
            self.current_order["order_items"].append(self.current_item.copy())
            self.current_item = {}
            self.in_item = False

        elif name_upper == "UNIT_PRICE":
            self.in_unit_price = False

        elif name_upper == "TOTAL_PRICE":
            if self.in_item_total_price:
                self.in_item_total_price = False
            else:
                self.in_total_price = False

        # Parse order-level fields
        elif self.in_order and not self.in_customer and not self.in_order_items and not self.in_total_price:
            if name_upper == "ORDER_ID":
                self.current_order["external_order_id"] = data
            elif name_upper == "CODE":
                self.current_order["order_code"] = data
            elif name_upper == "DATE":
                self.current_order["order_date"] = data
            elif name_upper == "STATUS":
                self.current_order["order_status"] = data
            elif name_upper == "REMARK":
                self.current_order["customer_remark"] = data
            elif name_upper == "SHOP_REMARK":
                self.current_order["shop_remark"] = data
            elif name_upper == "PACKAGE_NUMBER":
                self.current_order["package_number"] = data
            elif name_upper == "WEIGHT":
                self.current_order["weight"] = data
            elif name_upper == "SOURCE_NAME":
                self.current_order["source_name"] = data

        # Parse currency fields
        elif self.in_currency:
            if name_upper == "CODE":
                self.current_order["currency_code"] = data
            elif name_upper == "EXCHANGE_RATE":
                self.current_order["exchange_rate"] = data

        # Parse customer fields
        elif self.in_customer and not self.in_billing_address and not self.in_shipping_address:
            if name_upper == "EMAIL":
                self.current_order["customer_email"] = data
            elif name_upper == "PHONE":
                self.current_order["customer_phone"] = data
            elif name_upper == "IP_ADDRESS":
                self.current_order["ip_address"] = data

        # Parse item fields - MUST come before address check since items have NAME too
        elif self.in_item:
            if self.in_unit_price:
                if name_upper == "WITH_VAT":
                    self.current_item["unit_price_with_vat"] = data
                elif name_upper == "WITHOUT_VAT":
                    self.current_item["unit_price_without_vat"] = data
                elif name_upper == "VAT_RATE":
                    self.current_item["vat_rate"] = data
            elif self.in_item_total_price:
                if name_upper == "WITH_VAT":
                    self.current_item["total_price_with_vat"] = data
                elif name_upper == "WITHOUT_VAT":
                    self.current_item["total_price_without_vat"] = data
            else:
                item_field_map = {
                    "TYPE": "item_type",
                    "NAME": "item_name",
                    "AMOUNT": "quantity",
                    "CODE": "item_code",
                    "VARIANT_NAME": "variant_name",
                    "EAN": "ean",
                    "MANUFACTURER": "manufacturer",
                    "SUPPLIER": "supplier",
                    "UNIT": "unit",
                    "WEIGHT": "weight",
                    "STATUS": "status",
                    "DISCOUNT": "discount"
                }
                if name_upper in item_field_map:
                    self.current_item[item_field_map[name_upper]] = data

        # Parse billing/shipping address fields
        elif self.in_billing_address or self.in_shipping_address:
            field_map = {
                "NAME": "customer_name",
                "COMPANY": "company_name",
                "STREET": "street",
                "HOUSENUMBER": "house_number",
                "CITY": "city",
                "ZIP": "postal_code",
                "COUNTRY": "country",
                "COMPANY_ID": "company_id",
                "VAT_ID": "vat_id",
                "CUSTOMER_IDENTIFICATION_NUMBER": "customer_id_number"
            }
            if name_upper in field_map:
                self.current_address[field_map[name_upper]] = data

        # Parse order total price fields
        elif self.in_total_price and not self.in_item:
            if name_upper == "WITH_VAT":
                self.current_order["total_with_vat"] = data
            elif name_upper == "WITHOUT_VAT":
                self.current_order["total_without_vat"] = data
            elif name_upper == "VAT":
                self.current_order["total_vat"] = data
            elif name_upper == "PRICE_TO_PAY":
                self.current_order["price_to_pay"] = data
            elif name_upper == "PAID":
                self.current_order["is_paid"] = data
            elif name_upper == "AMOUNT_PAID":
                self.current_order["amount_paid"] = data

        # Pop from stack
        if self.element_stack:
            self.element_stack.pop()
        self.current_data = ""

    def characters(self, content):
        self.current_data += content


def get_redis_client():
    """Get Redis client connection with retry"""
    if not REDIS_AVAILABLE:
        frappe.throw("Redis package not installed. Install with: pip install redis")

    try:
        # Get Redis configuration from Frappe
        redis_config = frappe.conf.get("redis_queue") or frappe.conf.get("redis_cache")

        if isinstance(redis_config, str):
            client = redis.from_url(redis_config, socket_timeout=5, socket_connect_timeout=5)
        elif isinstance(redis_config, dict):
            client = redis.Redis(**redis_config, socket_timeout=5, socket_connect_timeout=5)
        else:
            client = redis.Redis(host='localhost', port=6379, db=0, socket_timeout=5, socket_connect_timeout=5)

        # Test connection
        client.ping()
        return client

    except Exception as e:
        frappe.log_error(f"Redis connection error: {str(e)}")
        raise


@frappe.whitelist()
def import_xml_orders_sax(xml_source: str, company: str = None, config_name: str = None) -> Dict[str, Any]:
    """
    Import orders from XML feed using SAX parser with Redis queue

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        config_name: Name of XML Import Configuration document (optional)

    Returns:
        Dict with import results
    """
    try:
        result = _import_xml_orders_sax_internal(xml_source, company, config_name)

        # Always create import log and update config status
        _create_order_import_log_from_result(xml_source, result)
        _update_order_config_status(xml_source, result)

        return result

    except Exception as e:
        error_result = {
            "success": False,
            "error": str(e),
            "imported": 0,
            "updated": 0,
            "errors": 1,
            "error_messages": [str(e)]
        }

        # Log error
        _create_order_import_log_from_result(xml_source, error_result)
        _update_order_config_status(xml_source, error_result)

        raise


def _import_xml_orders_sax_internal(xml_source: str, company: str = None, config_name: str = None) -> Dict[str, Any]:
    """
    Import orders from XML feed using SAX parser with Redis queue

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        config_name: Name of XML Import Configuration document (optional)

    Returns:
        Dict with import results
    """
    try:
        frappe.logger().info(f"Starting SAX-based XML order import from: {xml_source}")

        # Load config if provided
        config = None
        if config_name:
            config = frappe.get_doc("XML Import Configuration", config_name)
            frappe.logger().info(f"Loaded config: {config_name}")

        # Create importer to use existing functionality
        importer = XMLOrderImporter(xml_source, company, config)

        # Fetch XML content
        xml_content = importer.fetch_xml_content()

        # Always use Redis for memory efficiency
        if not REDIS_AVAILABLE:
            frappe.throw("Redis package not installed. Install with: pip install redis")

        redis_client = get_redis_client()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        queue_name = f"xml_orders_{timestamp}"

        # Phase 1: Parse XML with SAX and queue orders
        frappe.logger().info("Phase 1: Parsing XML and queueing orders...")

        # Initialize progress tracking
        _update_order_import_progress(redis_client, "parsing", 0, 0, 0, queue_name=queue_name)

        handler = SAXOrderHandler(redis_client, queue_name)
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        from io import StringIO
        parser.parse(StringIO(xml_content))

        total_orders = handler.orders_processed
        frappe.logger().info(f"Queued {total_orders} orders for processing")

        # Update progress after parsing
        _update_order_import_progress(redis_client, "processing", total_orders, 0, 0, queue_name=queue_name)

        # Phase 2: Process queued orders
        frappe.logger().info("Phase 2: Processing queued orders...")
        processed_count = 0
        imported_count = 0
        updated_count = 0
        skipped_count = 0
        error_count = 0
        errors = []
        skip_reasons = {}  # Track skip reasons: {'exists': [...], 'cancelled': [...], 'no_products': [...]}
        consecutive_failures = 0
        max_consecutive_failures = 5

        while True:
            # Get order from queue with retry on Redis errors
            try:
                order_data = redis_client.rpop(handler.queue_name)
                consecutive_failures = 0  # Reset on success
            except redis.exceptions.ConnectionError as ce:
                consecutive_failures += 1
                frappe.log_error(f"Redis connection error (attempt {consecutive_failures}): {str(ce)}")

                if consecutive_failures >= max_consecutive_failures:
                    error_msg = f"Redis connection failed {max_consecutive_failures} times, aborting"
                    errors.append(error_msg)
                    frappe.log_error(error_msg)
                    break

                # Try to reconnect
                try:
                    import time
                    time.sleep(1)  # Wait before retry
                    redis_client = get_redis_client()
                    frappe.logger().info("Redis reconnected successfully")
                    continue
                except Exception as re:
                    frappe.log_error(f"Redis reconnection failed: {str(re)}")
                    continue
            except Exception as e:
                error_count += 1
                error_msg = f"Redis error getting order: {str(e)}"
                errors.append(error_msg)
                frappe.log_error(error_msg)
                break

            if not order_data:
                break

            try:
                order_dict = json.loads(order_data.decode('utf-8'))
                external_id = order_dict.get('external_order_id', 'unknown')

                # Convert SAX format to legacy format for processing
                legacy_order = convert_sax_order_to_legacy_format(order_dict)

                # Process the order using existing methods
                result, reason = importer.create_or_update_order(legacy_order)

                # Track based on result string
                if result == 'imported':
                    imported_count += 1
                    frappe.logger().info(f"[ORDER IMPORT] Order {external_id} imported as {reason}")
                elif result == 'skipped':
                    skipped_count += 1
                    # Track skip reasons
                    if reason not in skip_reasons:
                        skip_reasons[reason] = []
                    skip_reasons[reason].append(external_id)
                    frappe.logger().info(f"[ORDER IMPORT] Order {external_id} skipped: {reason}")
                elif result == 'error':
                    error_count += 1
                    frappe.logger().error(f"[ORDER IMPORT] Order {external_id} failed: {reason}")

                processed_count += 1

                # Progress update every 10 orders (more frequent for orders than items)
                if processed_count % 10 == 0:
                    _update_order_import_progress(
                        redis_client, "processing", total_orders, processed_count, error_count,
                        imported=imported_count, skipped=skipped_count, queue_name=queue_name,
                        skip_reasons=skip_reasons
                    )
                    frappe.publish_realtime(
                        "sax_order_import_progress",
                        {
                            "phase": "processing",
                            "processed": processed_count,
                            "total": total_orders,
                            "imported": imported_count,
                            "skipped": skipped_count,
                            "updated": updated_count,
                            "errors": error_count
                        },
                        user=frappe.session.user
                    )

            except Exception as e:
                error_count += 1
                error_msg = f"Error processing order {processed_count + 1}: {str(e)}"
                errors.append(error_msg)
                frappe.log_error(error_msg)

        # Clean up queue
        try:
            redis_client.delete(handler.queue_name)
        except Exception:
            pass  # Ignore cleanup errors

        # Final progress update (completed)
        _update_order_import_progress(
            redis_client, "complete", total_orders, processed_count, error_count,
            imported=imported_count, skipped=skipped_count, queue_name=queue_name,
            skip_reasons=skip_reasons, completed=True
        )

        # Log skip reasons summary
        if skip_reasons:
            frappe.logger().info(f"[ORDER IMPORT] Skip reasons summary:")
            for reason, order_ids in skip_reasons.items():
                frappe.logger().info(f"  - {reason}: {len(order_ids)} orders ({', '.join(order_ids[:5])}{'...' if len(order_ids) > 5 else ''})")

        # Final result
        result = {
            "success": True,
            "imported": imported_count,
            "updated": updated_count,
            "skipped": skipped_count,
            "skip_reasons": {k: len(v) for k, v in skip_reasons.items()},
            "errors": error_count,
            "error_messages": errors,
            "total_processed": processed_count,
            "method": "SAX with Redis queue"
        }

        frappe.logger().info(f"SAX order import completed: {result}")
        return result

    except Exception as e:
        error_msg = f"SAX XML order import failed: {str(e)}"
        frappe.log_error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "imported": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 1,
            "error_messages": [error_msg]
        }


def convert_sax_order_to_legacy_format(sax_order_data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert SAX-parsed order data to legacy format expected by create_or_update_order"""

    # Convert order items
    order_items = []
    for item in sax_order_data.get("order_items", []):
        order_items.append({
            "item_type": item.get("item_type", ""),
            "item_name": item.get("item_name", ""),
            "item_code": item.get("item_code", ""),
            "quantity": item.get("quantity", "1"),
            "ean": item.get("ean", ""),
            "manufacturer": item.get("manufacturer", ""),
            "supplier": item.get("supplier", ""),
            "unit": item.get("unit", ""),
            "weight": item.get("weight", ""),
            "unit_price_with_vat": item.get("unit_price_with_vat", ""),
            "unit_price_without_vat": item.get("unit_price_without_vat", ""),
            "total_price_with_vat": item.get("total_price_with_vat", ""),
            "total_price_without_vat": item.get("total_price_without_vat", ""),
            "vat_rate": item.get("vat_rate", ""),
            "discount": item.get("discount", "0")
        })

    return {
        "external_order_id": sax_order_data.get("external_order_id", ""),
        "order_code": sax_order_data.get("order_code", ""),
        "order_date": sax_order_data.get("order_date", ""),
        "order_status": sax_order_data.get("order_status", ""),
        "currency_code": sax_order_data.get("currency_code", "EUR"),
        "exchange_rate": sax_order_data.get("exchange_rate", "1"),
        "customer_email": sax_order_data.get("customer_email", ""),
        "customer_phone": sax_order_data.get("customer_phone", ""),
        "ip_address": sax_order_data.get("ip_address", ""),
        "customer_remark": sax_order_data.get("customer_remark", ""),
        "shop_remark": sax_order_data.get("shop_remark", ""),
        "package_number": sax_order_data.get("package_number", ""),
        "weight": sax_order_data.get("weight", ""),
        "source_name": sax_order_data.get("source_name", ""),
        "total_with_vat": sax_order_data.get("total_with_vat", ""),
        "total_without_vat": sax_order_data.get("total_without_vat", ""),
        "is_paid": sax_order_data.get("is_paid", "0"),
        "amount_paid": sax_order_data.get("amount_paid", ""),
        "billing_address": sax_order_data.get("billing_address", {}),
        "shipping_address": sax_order_data.get("shipping_address", {}),
        "order_items": order_items
    }


def _create_order_import_log_from_result(xml_source: str, result: Dict[str, Any]):
    """Create order import log entry from result"""
    try:
        from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_order_import_log
        create_order_import_log(
            xml_source=xml_source,
            status="Success" if result.get("success") else "Failed",
            imported=result.get("imported", 0),
            updated=result.get("updated", 0),
            errors=result.get("errors", 0),
            error_details="\n".join(result.get("error_messages", [])),
            summary=result
        )
    except Exception as e:
        frappe.log_error(f"Failed to create order import log: {str(e)}")


def _update_order_config_status(xml_source: str, result: Dict[str, Any]):
    """Update order configuration status based on result"""
    try:
        # Find configuration by XML feed URL (try exact match first, then partial)
        configs = frappe.get_all(
            "XML Import Configuration",
            filters={"xml_feed_url": xml_source, "import_type": "Orders"},
            fields=["name"]
        )

        # If no exact match, try to find by partial URL match
        if not configs:
            all_order_configs = frappe.get_all(
                "XML Import Configuration",
                filters={"import_type": "Orders"},
                fields=["name", "xml_feed_url"]
            )
            for config in all_order_configs:
                # Check if base URL matches (ignore query params)
                config_base = config.xml_feed_url.split("?")[0] if config.xml_feed_url else ""
                source_base = xml_source.split("?")[0] if xml_source else ""
                if config_base and source_base and config_base == source_base:
                    configs.append({"name": config.name})

        for config in configs:
            config_doc = frappe.get_doc("XML Import Configuration", config["name"])
            config_doc.db_set("last_import", frappe.utils.now())
            config_doc.db_set("last_import_status", "Success" if result.get("success") else "Failed")
            config_doc.db_set("last_import_count", result.get("imported", 0) + result.get("updated", 0))

        frappe.db.commit()

    except Exception as e:
        frappe.log_error(f"Failed to update order config status: {str(e)}")


def scheduled_xml_order_import():
    """
    Scheduled function to import XML orders
    """
    # This can be added to hooks.py for scheduled imports
    # For now, focusing on manual imports
    pass


# ==================== ORDER PROGRESS TRACKING ====================

# Progress tracking key for orders
ORDER_IMPORT_PROGRESS_KEY = "xml_order_import_progress"


def _update_order_import_progress(redis_client, phase: str, total: int, processed: int,
                                  errors: int, imported: int = 0, skipped: int = 0,
                                  queue_name: str = "", completed: bool = False,
                                  skip_reasons: Dict = None):
    """Update order import progress in Redis"""
    try:
        # Get existing data to preserve start_time
        existing = redis_client.get(ORDER_IMPORT_PROGRESS_KEY)
        start_time = datetime.now().isoformat()
        if existing:
            existing_data = json.loads(existing)
            start_time = existing_data.get("start_time", start_time)

        progress_data = {
            "phase": phase,
            "queue_name": queue_name,
            "total_orders": total,
            "processed": processed,
            "imported": imported,
            "skipped": skipped,
            "errors": errors,
            "skip_reasons": {k: len(v) for k, v in (skip_reasons or {}).items()},
            "start_time": start_time,
            "last_update": datetime.now().isoformat(),
            "status": "complete" if completed else "running"
        }
        redis_client.set(ORDER_IMPORT_PROGRESS_KEY, json.dumps(progress_data))
        # Set expiry - shorter if completed
        redis_client.expire(ORDER_IMPORT_PROGRESS_KEY, 300 if completed else 3600)
    except Exception as e:
        frappe.log_error(f"Failed to update order import progress: {str(e)}")


@frappe.whitelist()
def get_order_import_progress() -> Dict[str, Any]:
    """
    Get current order import progress from Redis

    Returns:
        Dict with progress information or empty status if no import running
    """
    try:
        if not REDIS_AVAILABLE:
            return {"status": "no_redis", "message": "Redis not available"}

        redis_client = get_redis_client()
        progress_data = redis_client.get(ORDER_IMPORT_PROGRESS_KEY)

        if not progress_data:
            return {
                "status": "idle",
                "message": "No order import in progress",
                "is_running": False
            }

        data = json.loads(progress_data)

        # Calculate percentage
        total = data.get("total_orders", 0)
        processed = data.get("processed", 0)
        percentage = round((processed / total * 100), 1) if total > 0 else 0

        # Calculate elapsed time
        start_time = data.get("start_time")
        elapsed_seconds = 0
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time)
                elapsed_seconds = (datetime.now() - start_dt).total_seconds()
            except:
                pass

        # Format elapsed time
        elapsed_str = ""
        if elapsed_seconds > 0:
            minutes = int(elapsed_seconds // 60)
            seconds = int(elapsed_seconds % 60)
            elapsed_str = f"{minutes}m {seconds}s" if minutes > 0 else f"{seconds}s"

        # Estimate remaining time
        remaining_str = ""
        if processed > 0 and total > processed:
            rate = processed / elapsed_seconds if elapsed_seconds > 0 else 0
            if rate > 0:
                remaining_seconds = (total - processed) / rate
                remaining_minutes = int(remaining_seconds // 60)
                remaining_secs = int(remaining_seconds % 60)
                remaining_str = f"{remaining_minutes}m {remaining_secs}s" if remaining_minutes > 0 else f"{remaining_secs}s"

        return {
            "status": data.get("status", "unknown"),
            "phase": data.get("phase", "unknown"),
            "is_running": data.get("status") == "running",
            "total_items": total,
            "processed": processed,
            "imported": data.get("imported", 0),
            "skipped": data.get("skipped", 0),
            "errors": data.get("errors", 0),
            "skip_reasons": data.get("skip_reasons", {}),
            "percentage": percentage,
            "elapsed_time": elapsed_str,
            "remaining_time": remaining_str,
            "queue_name": data.get("queue_name", ""),
            "last_update": data.get("last_update", ""),
            "message": f"Processing {processed}/{total} orders ({percentage}%)"
        }

    except Exception as e:
        frappe.log_error(f"Error getting order import progress: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "is_running": False
        }


@frappe.whitelist()
def cancel_order_import() -> Dict[str, Any]:
    """
    Cancel the current order import and clean up Redis

    Returns:
        Dict with cancellation result
    """
    try:
        if not REDIS_AVAILABLE:
            return {"success": False, "message": "Redis not available"}

        redis_client = get_redis_client()

        # Get current progress to find queue name
        progress_data = redis_client.get(ORDER_IMPORT_PROGRESS_KEY)
        queue_name = None

        if progress_data:
            data = json.loads(progress_data)
            queue_name = data.get("queue_name")

            # Mark as cancelled
            data["status"] = "cancelled"
            data["last_update"] = datetime.now().isoformat()
            redis_client.set(ORDER_IMPORT_PROGRESS_KEY, json.dumps(data))
            redis_client.expire(ORDER_IMPORT_PROGRESS_KEY, 60)  # Keep for 1 minute only

        # Clear the queue if it exists
        if queue_name:
            redis_client.delete(queue_name)

        return {
            "success": True,
            "message": f"Order import cancelled. Queue '{queue_name}' cleared.",
            "queue_cleared": queue_name
        }

    except Exception as e:
        frappe.log_error(f"Error cancelling order import: {str(e)}")
        return {
            "success": False,
            "message": str(e)
        }

