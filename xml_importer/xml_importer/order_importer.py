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

    def __init__(self, xml_source: str = None, company: str = None):
        """
        Initialize XML Order Importer

        Args:
            xml_source: URL or file path to XML feed
            company: Company name in ERPNext (default: default company)
        """
        self.xml_source = xml_source
        self.company = company or frappe.defaults.get_global_default("company")
        self.imported_count = 0
        self.updated_count = 0
        self.error_count = 0
        self.errors = []

        # Initialize required data
        self.ensure_required_data()

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

    def fetch_xml_content(self) -> str:
        """Fetch XML content from URL or file"""
        try:
            if self.xml_source.startswith(('http://', 'https://')):
                # Fetch from URL
                response = requests.get(self.xml_source, timeout=60)
                response.raise_for_status()
                return response.text
            else:
                # Read from file
                with open(self.xml_source, 'r', encoding='utf-8') as f:
                    return f.read()
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
                    'country': self.get_element_text(billing_elem, 'COUNTRY'),
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
                    'country': self.get_element_text(shipping_elem, 'COUNTRY')
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

            # Create new address
            address_doc = frappe.get_doc({
                "doctype": "Address",
                "address_title": address_title,
                "address_type": address_type,
                "address_line1": address_line1,
                "city": address_data.get('city', ''),
                "pincode": address_data.get('postal_code', ''),
                "country": address_data.get('country', 'Slovakia'),
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

    def create_or_update_order(self, order_data: Dict[str, Any]) -> bool:
        """Create or update ERPNext Sales Order"""
        try:
            external_order_id = order_data.get('external_order_id')
            if not external_order_id:
                self.add_error("Missing external order ID")
                return False

            # Check if order exists
            existing_order = frappe.db.get_value("Sales Order", {"custom_external_order_id": external_order_id}, "name")

            if existing_order:
                # Skip if order already exists
                frappe.logger().info(f"Order {external_order_id} already exists, skipping")
                return True

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

            # Custom fields for order tracking
            sales_order.custom_external_order_id = external_order_id
            sales_order.custom_order_code = order_data.get('order_code')
            sales_order.custom_order_status = order_data.get('order_status')
            sales_order.custom_source_name = order_data.get('source_name')

            # Add customer remarks
            if order_data.get('customer_remark'):
                sales_order.custom_customer_remark = order_data.get('customer_remark')

            # Process order items
            for item_data in order_data.get('order_items', []):
                # Only add product items to sales order
                if item_data.get('item_type') == 'product':
                    self.add_order_item(sales_order, item_data)

            # Set totals
            sales_order.run_method("calculate_taxes_and_totals")

            # Save order
            sales_order.insert(ignore_permissions=True)
            sales_order.submit()

            self.imported_count += 1
            frappe.logger().info(f"Created Sales Order: {sales_order.name} for external order {external_order_id}")

            return True

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to process order {order_data.get('external_order_id', 'Unknown')}: {str(e)}"
            self.add_error(error_msg)
            frappe.log_error(error_msg)
            return False

    def add_order_item(self, sales_order: Document, item_data: Dict[str, Any]):
        """Add item to sales order"""
        try:
            item_code = item_data.get('item_code')
            if not item_code:
                return

            # Check if item exists in ERPNext
            if not frappe.db.exists("Item", item_code):
                # Create a placeholder item if it doesn't exist
                self.create_placeholder_item(item_code, item_data)

            # Add item to sales order
            item_row = sales_order.append("items", {})
            item_row.item_code = item_code
            item_row.item_name = item_data.get('item_name', item_code)
            item_row.qty = item_data.get('quantity', 1)
            item_row.rate = item_data.get('unit_price_without_tax', 0)
            item_row.amount = item_data.get('total_price_without_tax', 0)

            # Set warehouse (get default)
            default_warehouse = frappe.db.get_single_value("Stock Settings", "default_warehouse")
            if default_warehouse:
                item_row.warehouse = default_warehouse

        except Exception as e:
            frappe.log_error(f"Failed to add order item {item_data.get('item_code')}: {str(e)}")

    def create_placeholder_item(self, item_code: str, item_data: Dict[str, Any]):
        """Create placeholder item if it doesn't exist"""
        try:
            item_doc = frappe.get_doc({
                "doctype": "Item",
                "item_code": item_code,
                "item_name": item_data.get('item_name', item_code),
                "item_group": "All Item Groups",
                "stock_uom": "Nos",
                "is_stock_item": 1,
                "is_sales_item": 1,
                "is_purchase_item": 0,
                "description": f"Auto-created from order import - {item_data.get('item_name', '')}"
            })

            item_doc.insert(ignore_permissions=True)
            frappe.logger().info(f"Created placeholder item: {item_code}")

        except Exception as e:
            frappe.log_error(f"Failed to create placeholder item {item_code}: {str(e)}")

    def add_error(self, error_msg: str) -> None:
        """Add error to error list"""
        self.errors.append(error_msg)
        self.error_count += 1

    def import_from_xml(self) -> Dict[str, Any]:
        """Main import function"""
        try:
            frappe.logger().info(f"Starting XML order import from: {self.xml_source}")

            # Fetch and parse XML
            xml_content = self.fetch_xml_content()
            root = self.parse_xml(xml_content)

            # Find all ORDER elements
            orders = root.findall('.//ORDER')

            frappe.logger().info(f"Found {len(orders)} orders to process")

            # Process each order
            for order in orders:
                try:
                    order_data = self.parse_order(order)
                    self.create_or_update_order(order_data)
                except Exception as e:
                    self.add_error(f"Error processing ORDER ID {order.get('ORDER_ID', 'Unknown')}: {str(e)}")
                    continue

            # Return summary
            summary = {
                "success": True,
                "imported": self.imported_count,
                "updated": self.updated_count,
                "errors": self.error_count,
                "error_messages": self.errors[:10],  # First 10 errors
                "total_processed": len(orders)
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
    Import orders from XML feed

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)

    Returns:
        Dict with import results
    """
    importer = XMLOrderImporter(xml_source, company)
    return importer.import_from_xml()


def scheduled_xml_order_import():
    """
    Scheduled function to import XML orders
    """
    # This can be added to hooks.py for scheduled imports
    # For now, focusing on manual imports
    pass
