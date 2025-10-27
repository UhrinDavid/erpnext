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

            # Skip cancelled/storno orders
            order_status = order_data.get('order_status', '').lower()
            if 'storno' in order_status or 'cancel' in order_status or 'zrušen' in order_status:
                frappe.logger().info(f"Skipping cancelled order {external_order_id} with status: {order_data.get('order_status')}")
                return True

            # Check if order exists
            existing_order = frappe.db.get_value("Sales Order", {"po_no": external_order_id}, "name")

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

            # Use po_no field for external order ID tracking (standard ERPNext field)
            sales_order.po_no = external_order_id
            sales_order.po_date = order_date.date()

            # Add customer remarks
            if order_data.get('customer_remark'):
                sales_order.remarks = order_data.get('customer_remark')

            # Process order items
            product_items_added = 0
            for item_data in order_data.get('order_items', []):
                # Only add product items to sales order
                if item_data.get('item_type') == 'product':
                    if self.add_order_item(sales_order, item_data):
                        product_items_added += 1

            # Only create order if we have product items
            if product_items_added == 0:
                frappe.logger().info(f"No valid product items found for order {external_order_id}, skipping")
                return True

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

            return True

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to process order {order_data.get('external_order_id', 'Unknown')}: {str(e)}"
            self.add_error(error_msg)
            frappe.log_error(error_msg)
            return False

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
            item_row.item_name = item_data.get('item_name', item_code)
            item_row.qty = item_data.get('quantity', 1)
            item_row.rate = item_data.get('unit_price_without_tax', 0)

            # If rate is 0, try to get it from with_tax price
            if item_row.rate == 0:
                item_row.rate = item_data.get('unit_price_with_tax', 0)

            # Calculate amount
            item_row.amount = item_row.qty * item_row.rate

            # Set UOM
            uom = item_data.get('unit', 'Nos')
            if uom == 'ks':  # Slovak for pieces
                uom = 'Nos'
            item_row.uom = uom

            # Set warehouse (get default)
            default_warehouse = frappe.db.get_single_value("Stock Settings", "default_warehouse")
            if default_warehouse:
                item_row.warehouse = default_warehouse

            frappe.logger().info(f"Added item {item_code} to sales order")
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

            item_doc = frappe.get_doc({
                "doctype": "Item",
                "item_code": item_code,
                "item_name": item_name,
                "item_group": "All Item Groups",
                "stock_uom": "Nos",
                "is_stock_item": 1,
                "is_sales_item": 1,
                "is_purchase_item": 0,
                "description": f"Auto-created from order import: {item_name}"
            })

            # Add barcode if available
            if item_data.get('barcode'):
                item_doc.append("barcodes", {
                    "barcode": item_data.get('barcode'),
                    "barcode_type": "EAN"
                })

            item_doc.insert(ignore_permissions=True)
            frappe.logger().info(f"Created placeholder item: {item_code}")
            return True

        except Exception as e:
            frappe.log_error(f"Failed to create placeholder item {item_code}: {str(e)}")
            return False

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
