"""
XML Item Importer for ERPNext
A module that imports items from XML feeds into ERPNext Item documents

Author: Herbatica
License: MIT
"""

import frappe
import requests
import xml.etree.ElementTree as ET
from frappe.model.document import Document
from frappe.utils import now, cstr, flt, cint, strip_html_tags
from frappe.utils.file_manager import save_file
import re
import os
from urllib.parse import urlparse
from typing import Dict, List, Optional, Any

class XMLItemImporter:
    """Import items from XML feed into ERPNext"""

    def __init__(self, xml_source: str = None, company: str = None, config=None):
        """
        Initialize XML Item Importer

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

        # Initialize required UOMs and custom fields
        self.ensure_required_uoms()
        self.ensure_additional_categories_field()
        self.ensure_short_description_field()

    def ensure_required_uoms(self):
        """Ensure commonly used UOMs exist"""
        required_uoms = [
            {"uom_name": "ks", "must_be_whole_number": 1},  # Slovak pieces
            {"uom_name": "Nos", "must_be_whole_number": 1}, # Standard pieces
            {"uom_name": "Kg", "must_be_whole_number": 0},  # Kilogram
            {"uom_name": "Gram", "must_be_whole_number": 0}, # Gram
            {"uom_name": "Litre", "must_be_whole_number": 0}, # Liter
            {"uom_name": "Millilitre", "must_be_whole_number": 0}, # Milliliter
        ]

        for uom_data in required_uoms:
            if not frappe.db.exists("UOM", uom_data["uom_name"]):
                try:
                    uom_doc = frappe.get_doc({
                        "doctype": "UOM",
                        **uom_data
                    })
                    uom_doc.insert(ignore_permissions=True)
                    frappe.db.commit()
                except Exception as e:
                    frappe.log_error(f"Failed to create UOM {uom_data['uom_name']}: {str(e)}")

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

    def get_or_create_uom(self, unit_code: str) -> str:
        """Get or create Unit of Measure"""
        if not unit_code:
            return "Nos"

        # Map common Slovak/Czech units to ERPNext equivalents
        unit_mapping = {
            'ks': 'Nos',  # pieces
            'kg': 'Kg',   # kilogram
            'g': 'Gram',  # gram
            'l': 'Litre', # liter
            'ml': 'Millilitre', # milliliter
            'm': 'Meter', # meter
            'cm': 'Centimeter', # centimeter
            'mm': 'Millimeter', # millimeter
            'pc': 'Nos',  # pieces
            'pcs': 'Nos', # pieces
            'box': 'Box', # box
            'pack': 'Pack', # pack
            'bottle': 'Bottle' # bottle
        }

        # Check if we have a direct mapping
        mapped_unit = unit_mapping.get(unit_code.lower())
        if mapped_unit:
            return mapped_unit

        # Check if UOM exists in ERPNext
        if frappe.db.exists("UOM", unit_code):
            return unit_code

        # Create new UOM if it doesn't exist
        try:
            uom_doc = frappe.get_doc({
                "doctype": "UOM",
                "uom_name": unit_code,
                "must_be_whole_number": 1 if unit_code.lower() in ['ks', 'pc', 'pcs'] else 0
            })
            uom_doc.insert(ignore_permissions=True)
            return unit_code
        except Exception as e:
            frappe.log_error(f"Failed to create UOM {unit_code}: {str(e)}")
            return "Nos"  # Default fallback

    def download_image(self, image_url: str, item_code: str, image_description: str = "") -> Optional[str]:
        """Download and save image to ERPNext files"""
        if not image_url:
            return None

        try:
            # Download image
            response = requests.get(image_url, timeout=30)
            response.raise_for_status()

            # Get file extension from URL
            parsed_url = urlparse(image_url)
            filename = os.path.basename(parsed_url.path)
            if not filename or '.' not in filename:
                filename = f"{item_code}_image.jpg"

            # Create folder for item images if it doesn't exist
            folder_name = "Home/Item Images"
            if not frappe.db.exists("File", {"file_name": "Item Images", "is_folder": 1, "folder": "Home"}):
                # Create the folder using File doctype
                folder_doc = frappe.get_doc({
                    "doctype": "File",
                    "file_name": "Item Images",
                    "is_folder": 1,
                    "folder": "Home"
                })
                folder_doc.insert(ignore_permissions=True)

            # Save file
            file_doc = save_file(
                filename,
                response.content,
                dt="Item",
                dn=item_code,
                folder="Home/Item Images",
                is_private=0
            )

            return file_doc.file_url

        except Exception as e:
            frappe.log_error(f"Failed to download image {image_url}: {str(e)}")
            return None

    def map_categories(self, categories: List[Dict]) -> List[str]:
        """Map XML categories to ERPNext Item Groups"""
        item_groups = []

        for category in categories:
            category_name = category.get('category_name', '').strip()
            if not category_name:
                continue

            # Clean category name and remove invalid characters
            category_name = self.clean_name(category_name)
            category_name = category_name.replace(' > ', ' - ')

            # Skip if name is empty after cleaning
            if not category_name:
                continue            # Check if Item Group exists, create if not
            if not frappe.db.exists("Item Group", category_name):
                try:
                    item_group = frappe.get_doc({
                        "doctype": "Item Group",
                        "item_group_name": category_name,
                        "parent_item_group": "All Item Groups",
                        "is_group": 0
                    })
                    item_group.insert(ignore_permissions=True)
                    frappe.db.commit()
                except Exception as e:
                    frappe.log_error(f"Failed to create Item Group {category_name}: {str(e)}")
                    continue

            item_groups.append(category_name)

        return item_groups

    def get_or_create_item_group(self, category_name: str) -> str:
        """Get or create a single item group"""
        if not category_name or not category_name.strip():
            return "All Item Groups"

        category_name = self.clean_name(category_name.strip())

        if not frappe.db.exists("Item Group", category_name):
            try:
                item_group = frappe.get_doc({
                    "doctype": "Item Group",
                    "item_group_name": category_name,
                    "parent_item_group": "All Item Groups",
                    "is_group": 0
                })
                item_group.insert(ignore_permissions=True)
                frappe.logger().info(f"Created Item Group: {category_name}")
            except Exception as e:
                frappe.log_error(f"Failed to create Item Group {category_name}: {str(e)}")
                return "All Item Groups"

        return category_name

    def get_or_create_brand(self, brand_name: str) -> str:
        """Get or create a brand"""
        if not brand_name or not brand_name.strip():
            return None

        brand_name = self.clean_name(brand_name.strip())

        if not frappe.db.exists("Brand", brand_name):
            try:
                brand = frappe.get_doc({
                    "doctype": "Brand",
                    "brand": brand_name
                })
                brand.insert(ignore_permissions=True)
                frappe.logger().info(f"Created Brand: {brand_name}")
            except Exception as e:
                frappe.log_error(f"Failed to create Brand {brand_name}: {str(e)}")
                return None

        return brand_name

    def get_or_create_supplier(self, supplier_name: str) -> str:
        """Get or create a supplier"""
        if not supplier_name or not supplier_name.strip():
            return None

        supplier_name = self.clean_name(supplier_name.strip())

        if not frappe.db.exists("Supplier", supplier_name):
            try:
                supplier = frappe.get_doc({
                    "doctype": "Supplier",
                    "supplier_name": supplier_name,
                    "supplier_group": "All Supplier Groups",
                    "supplier_type": "Company"
                })
                supplier.insert(ignore_permissions=True)
                frappe.logger().info(f"Created Supplier: {supplier_name}")
            except Exception as e:
                frappe.log_error(f"Failed to create Supplier {supplier_name}: {str(e)}")
                return None

        return supplier_name

    def create_item_barcode(self, item_doc, barcode_value: str) -> None:
        """Create or update item barcode (EAN type)"""
        if not barcode_value or not barcode_value.strip():
            return

        barcode_value = barcode_value.strip()

        # Check if barcode already exists for any item
        existing_barcode = frappe.db.get_value(
            "Item Barcode",
            {"barcode": barcode_value},
            "name"
        )

        if existing_barcode:
            frappe.logger().info(f"Barcode {barcode_value} already exists, skipping for item {item_doc.item_code}")
            return

        try:
            # Add barcode to item's barcode table
            item_doc.append("barcodes", {
                "barcode": barcode_value,
                "barcode_type": "EAN"
            })
            frappe.logger().info(f"Added EAN barcode {barcode_value} to item {item_doc.item_code}")
        except Exception as e:
            frappe.log_error(f"Failed to add barcode {barcode_value} to item {item_doc.item_code}: {str(e)}")

    def is_valid_ean(self, barcode: str) -> bool:
        """Validate EAN-8, EAN-13, or other numeric barcodes"""
        if not barcode:
            return False

        # Remove spaces and check if it's numeric
        barcode = barcode.replace(" ", "").replace("-", "")
        if not barcode.isdigit():
            return False

        # Accept EAN-8, EAN-13, UPC-A (12 digits), or other common lengths
        if len(barcode) not in [8, 12, 13, 14]:
            frappe.logger().warning(f"Barcode '{barcode}' has invalid length {len(barcode)}")
            return False

        return True

    def set_item_tax(self, item_doc, item_data: Dict[str, Any]) -> None:
        """Set item tax information using Item Tax Template"""
        try:
            tax_rate = flt(item_data.get('tax_rate', 0))
            if tax_rate <= 0:
                return

            # Get or create the appropriate Item Tax Template
            tax_template = self.get_or_create_item_tax_template(tax_rate)

            if tax_template:
                # Clear existing taxes and add the template
                item_doc.taxes = []
                item_doc.append("taxes", {
                    "item_tax_template": tax_template,
                    "tax_category": ""  # Default tax category
                })
                frappe.logger().info(f"Set Item Tax Template '{tax_template}' ({tax_rate}%) for item {item_doc.item_code}")
            else:
                frappe.logger().warning(f"Could not create/find tax template for {tax_rate}% - item {item_doc.item_code}")

            # Also store in custom fields if available (for reference)
            if hasattr(item_doc, 'tax_rate'):
                item_doc.tax_rate = tax_rate

            tax_amount = item_data.get('tax_amount', 0)
            if tax_amount and hasattr(item_doc, 'tax_amount'):
                item_doc.tax_amount = tax_amount

        except Exception as e:
            frappe.log_error(f"Failed to set tax info for item {item_doc.item_code}: {str(e)}")

    def get_or_create_item_tax_template(self, tax_rate: float) -> str:
        """
        Get or create Item Tax Template for the given VAT rate

        This method will:
        1. Look for existing template with this exact rate
        2. If not found, create a new one with the default VAT account
        3. Return the template name

        Args:
            tax_rate: VAT rate percentage (e.g., 20.0)

        Returns:
            str: Name of the Item Tax Template (e.g., "VAT 20% - COMP")
        """
        try:
            # Get company abbreviation for naming
            company_abbr = frappe.get_cached_value("Company", self.company, "abbr")

            # Standard template naming: "VAT {rate}% - {abbr}"
            template_title = f"VAT {tax_rate}%"
            template_name = f"{template_title} - {company_abbr}"

            # Check if template already exists
            if frappe.db.exists("Item Tax Template", template_name):
                frappe.logger().debug(f"Using existing Item Tax Template: {template_name}")
                return template_name

            # Get or identify the VAT account
            vat_account = self.get_vat_account()
            if not vat_account:
                frappe.logger().error(f"No VAT account found for company {self.company}")
                return None

            # Create new Item Tax Template
            frappe.logger().info(f"Creating new Item Tax Template: {template_name}")

            tax_template = frappe.get_doc({
                "doctype": "Item Tax Template",
                "title": template_title,
                "company": self.company,
                "taxes": [
                    {
                        "tax_type": vat_account,
                        "tax_rate": tax_rate
                    }
                ]
            })

            tax_template.insert(ignore_permissions=True)
            frappe.db.commit()

            frappe.logger().info(f"Created Item Tax Template: {template_name} with rate {tax_rate}%")
            return template_name

        except Exception as e:
            frappe.log_error(
                f"Failed to create Item Tax Template for rate {tax_rate}%: {str(e)}",
                "Item Tax Template Creation Error"
            )
            return None

    def get_vat_account(self) -> str:
        """
        Get the VAT account for this company

        Searches for accounts in this order:
        1. Account with name containing "VAT" and type "Tax"
        2. Account with name containing "Tax" and type "Tax"
        3. First Tax account found

        Returns:
            str: Account name or None
        """
        try:
            # Try to find VAT account (most common naming)
            vat_account = frappe.db.get_value(
                "Account",
                {
                    "company": self.company,
                    "account_type": "Tax",
                    "is_group": 0,
                    "name": ["like", "%VAT%"]
                },
                "name"
            )

            if vat_account:
                return vat_account

            # Try Output Tax VAT (common in Slovakia/EU)
            vat_account = frappe.db.get_value(
                "Account",
                {
                    "company": self.company,
                    "account_type": "Tax",
                    "is_group": 0,
                    "name": ["like", "%Output%"]
                },
                "name"
            )

            if vat_account:
                return vat_account

            # Try any Tax account as fallback
            vat_account = frappe.db.get_value(
                "Account",
                {
                    "company": self.company,
                    "account_type": "Tax",
                    "is_group": 0
                },
                "name"
            )

            if vat_account:
                frappe.logger().warning(
                    f"Using generic Tax account {vat_account} - consider creating specific VAT account"
                )
                return vat_account

            # No tax account found
            frappe.logger().error(
                f"No Tax account found for company {self.company}. "
                "Please create a Tax account (e.g., 'VAT - {abbr}') in Chart of Accounts."
            )
            return None

        except Exception as e:
            frappe.log_error(f"Error finding VAT account: {str(e)}", "VAT Account Lookup Error")
            return None

    def get_or_create_tax_account(self, tax_rate: float) -> str:
        """
        DEPRECATED: Use get_or_create_item_tax_template instead

        Get or create tax account for the given tax rate - simplified
        """
        # This method is kept for backward compatibility but should not be used
        return self.get_vat_account()

    def handle_item_categories(self, item_doc, categories: List[Dict], default_category: str = None) -> None:
        """
        Handle multiple categories for an item (1:N relationship)

        - Sets DEFAULT_CATEGORY as the primary item_group
        - Stores additional categories in custom field 'additional_categories'
        - Also adds categories to Website Item Groups if it's a website item

        Args:
            item_doc: Item document
            categories: List of category dictionaries from XML
            default_category: The DEFAULT_CATEGORY from XML (already set as item_group)
        """
        try:
            additional_categories = []

            # Collect all categories EXCEPT the default one (which is already set as item_group)
            for category in categories:
                category_name = category.get('category_name', '').strip()
                if category_name and category_name != default_category:
                    # Ensure the category exists as an item group
                    self.get_or_create_item_group(category_name)
                    additional_categories.append(category_name)

            # Remove duplicates while preserving order
            unique_additional_categories = []
            seen = set()
            for cat in additional_categories:
                if cat not in seen:
                    unique_additional_categories.append(cat)
                    seen.add(cat)

            # Store additional categories in custom field (comma-separated)
            # This custom field needs to be created in ERPNext first
            if hasattr(item_doc, 'additional_categories'):
                if unique_additional_categories:
                    item_doc.additional_categories = ', '.join(unique_additional_categories)
                else:
                    item_doc.additional_categories = ''

            # Store as Small Text custom field (allows up to 140 chars)
            # For longer lists, use Text field instead
            elif hasattr(item_doc, 'custom_additional_categories'):
                if unique_additional_categories:
                    item_doc.custom_additional_categories = ', '.join(unique_additional_categories)
                else:
                    item_doc.custom_additional_categories = ''

            # Also add to Website Item Groups if this is a website item
            if item_doc.published_in_website:
                # Clear existing website item groups
                item_doc.website_item_groups = []

                # Add default category first
                if default_category:
                    item_doc.append('website_item_groups', {
                        'item_group': default_category
                    })

                # Add all additional categories
                for category_name in unique_additional_categories:
                    item_doc.append('website_item_groups', {
                        'item_group': category_name
                    })

            if unique_additional_categories:
                frappe.logger().info(
                    f"Item {item_doc.item_code}: Primary category = '{default_category or item_doc.item_group}', "
                    f"Additional categories = {unique_additional_categories}"
                )
            else:
                frappe.logger().info(
                    f"Item {item_doc.item_code}: Primary category = '{default_category or item_doc.item_group}', "
                    f"No additional categories"
                )

        except Exception as e:
            frappe.log_error(f"Failed to handle categories for item {item_doc.item_code}: {str(e)}")

    def ensure_additional_categories_field(self) -> None:
        """
        Ensure the 'Additional Categories' custom field exists on Item doctype
        This creates a Text field to store comma-separated additional category names
        """
        try:
            custom_field_name = "additional_categories"

            # Check if custom field already exists
            if frappe.db.exists("Custom Field", {"dt": "Item", "fieldname": custom_field_name}):
                frappe.logger().debug(f"Custom field '{custom_field_name}' already exists on Item")
                return

            # Create the custom field
            custom_field = frappe.get_doc({
                "doctype": "Custom Field",
                "dt": "Item",
                "label": "Additional Categories",
                "fieldname": custom_field_name,
                "fieldtype": "Small Text",  # Allows up to 140 characters
                "insert_after": "item_group",  # Place it right after the main item group
                "read_only": 0,
                "translatable": 0,
                "allow_in_quick_entry": 0,
                "description": "Additional Item Groups/Categories (comma-separated). Primary category is in 'Item Group' field."
            })

            custom_field.insert(ignore_permissions=True)
            frappe.db.commit()

            frappe.logger().info(f"Created custom field '{custom_field_name}' on Item doctype")

        except Exception as e:
            # If field creation fails, log it but don't stop the import
            frappe.log_error(
                f"Failed to create 'additional_categories' custom field: {str(e)}",
                "Custom Field Creation Error"
            )

    def ensure_short_description_field(self) -> None:
        """
        Ensure the 'Short Description' custom field exists on Item doctype
        This creates a Text Editor field for short product descriptions
        Positioned between description and brand fields
        """
        try:
            custom_field_name = "short_description"

            # Check if custom field already exists
            if frappe.db.exists("Custom Field", {"dt": "Item", "fieldname": custom_field_name}):
                frappe.logger().debug(f"Custom field '{custom_field_name}' already exists on Item")
                return

            # Create the custom field
            custom_field = frappe.get_doc({
                "doctype": "Custom Field",
                "dt": "Item",
                "label": "Short Description",
                "fieldname": custom_field_name,
                "fieldtype": "Text Editor",  # Rich text editor like description field
                "insert_after": "description",  # Place it right after description, before brand
                "read_only": 0,
                "translatable": 1,  # Allow translation
                "allow_in_quick_entry": 0,
                "description": "Brief product description from XML feed"
            })

            custom_field.insert(ignore_permissions=True)
            frappe.db.commit()

            frappe.logger().info(f"Created custom field '{custom_field_name}' on Item doctype")

        except Exception as e:
            # If field creation fails, log it but don't stop the import
            frappe.log_error(
                f"Failed to create 'short_description' custom field: {str(e)}",
                "Custom Field Creation Error"
            )

    def create_item_category_links(self, item_code: str, categories: List[Dict], default_category: str = None) -> None:
        """Create item-category links in a custom way"""
        try:
            # We'll use Tags or create comments to store additional categories
            # since ERPNext doesn't have a built-in Item Categories child table

            category_info = []

            # Add default category
            if default_category:
                category_info.append(f"Primary: {default_category}")

            # Add additional categories
            additional_categories = []
            for category in categories:
                category_name = category.get('category_name', '').strip()
                category_id = category.get('category_id', '').strip()
                if category_name and category_name != default_category:
                    if category_id:
                        additional_categories.append(f"{category_name} (ID: {category_id})")
                    else:
                        additional_categories.append(category_name)

            if additional_categories:
                category_info.append(f"Additional: {', '.join(additional_categories)}")

            # Store as tags (ERPNext's built-in tagging system)
            if category_info:
                tags = []
                for category in categories:
                    category_name = category.get('category_name', '').strip()
                    if category_name:
                        # Clean category name for use as tag
                        tag_name = re.sub(r'[^\w\s-]', '', category_name).strip()
                        if tag_name:
                            tags.append(tag_name)

                if tags:
                    from frappe.desk.doctype.tag.tag import add_tag
                    for tag in tags[:10]:  # Limit to 10 tags to avoid clutter
                        try:
                            add_tag(tag, "Item", item_code)
                        except:
                            pass  # Tag might already exist

            frappe.logger().info(f"Created category links for item {item_code}")

        except Exception as e:
            frappe.log_error(f"Failed to create category links for item {item_code}: {str(e)}")

    def parse_shop_item(self, shopitem: ET.Element) -> Dict[str, Any]:
        """Parse SHOPITEM XML element to dictionary with English property names"""
        item_data = {}

        # Basic information
        item_data['external_id'] = shopitem.get('id', '')
        item_data['import_code'] = shopitem.get('import-code', '')
        item_data['item_name'] = self.get_element_text(shopitem, 'NAME')
        item_data['guid'] = self.get_element_text(shopitem, 'GUID')

        # Get item code from CODE tag, fallback to id attribute if CODE is empty
        item_code = self.get_element_text(shopitem, 'CODE')
        if not item_code or not item_code.strip():
            item_code = shopitem.get('id', '')
        item_data['item_code'] = item_code

        item_data['barcode'] = self.get_element_text(shopitem, 'EAN')

        # Descriptions
        # DESCRIPTION -> main description field
        # SHORT_DESCRIPTION -> custom field (Text Editor)
        item_data['description'] = self.clean_html_content(
            self.get_element_text(shopitem, 'DESCRIPTION')
        )
        item_data['short_description'] = self.clean_html_content(
            self.get_element_text(shopitem, 'SHORT_DESCRIPTION')
        )

        # Supplier and manufacturer
        item_data['manufacturer_name'] = self.get_element_text(shopitem, 'MANUFACTURER')
        item_data['supplier_name'] = self.get_element_text(shopitem, 'SUPPLIER')

        # Pricing information
        item_data['currency_code'] = self.get_element_text(shopitem, 'CURRENCY')
        item_data['selling_price_with_tax'] = flt(self.get_element_text(shopitem, 'PRICE_VAT'))
        item_data['purchase_price'] = flt(self.get_element_text(shopitem, 'PURCHASE_PRICE'))
        item_data['tax_rate'] = flt(self.get_element_text(shopitem, 'VAT'))

        # Calculate tax value from VAT rate and PRICE_VAT
        price_vat = flt(self.get_element_text(shopitem, 'PRICE_VAT'))
        vat_rate = flt(self.get_element_text(shopitem, 'VAT'))
        if price_vat > 0 and vat_rate > 0:
            # Calculate base price without tax and tax amount
            base_price = price_vat / (1 + vat_rate / 100)
            tax_amount = price_vat - base_price
            item_data['tax_amount'] = tax_amount
            item_data['price_without_tax'] = base_price

        # Wholesale price from <PRICELISTS><PRICELIST><TITLE>Veľkoobchod</TITLE><PRICE_VAT>...</PRICE_VAT></PRICELIST></PRICELISTS>
        wholesale_price = None
        pricelists_elem = shopitem.find('PRICELISTS')
        if pricelists_elem is not None:
            for pricelist in pricelists_elem.findall('PRICELIST'):
                title = self.get_element_text(pricelist, 'TITLE')
                if title.strip().lower() == 'veľkoobchod':
                    price_vat = self.get_element_text(pricelist, 'PRICE_VAT')
                    if price_vat:
                        wholesale_price = flt(price_vat)
                        break
        item_data['wholesale_price'] = wholesale_price

        # Stock information
        stock_elem = shopitem.find('STOCK')
        if stock_elem is not None:
            item_data['current_stock'] = flt(self.get_element_text(stock_elem, 'AMOUNT'))
            item_data['minimum_stock'] = flt(self.get_element_text(stock_elem, 'MINIMAL_AMOUNT'))
            item_data['maximum_stock'] = flt(self.get_element_text(stock_elem, 'MAXIMAL_AMOUNT'))

        # Physical properties
        logistics_elem = shopitem.find('LOGISTIC')
        if logistics_elem is not None:
            item_data['weight_kg'] = flt(self.get_element_text(logistics_elem, 'WEIGHT'))

        # Unit of measure
        item_data['unit_of_measure'] = self.get_element_text(shopitem, 'UNIT') or 'Nos'

        # Visibility and classification
        item_data['is_published'] = cint(self.get_element_text(shopitem, 'VISIBLE'))
        item_data['product_type'] = self.get_element_text(shopitem, 'ITEM_TYPE')

        # Product categories
        product_categories = []
        categories_elem = shopitem.find('CATEGORIES')
        if categories_elem is not None:
            for category in categories_elem.findall('CATEGORY'):
                product_categories.append({
                    'category_id': category.get('id', ''),
                    'category_name': category.text.strip() if category.text else ''
                })

        item_data['product_categories'] = product_categories

        # Default category (primary category for item group)
        item_data['default_category'] = self.get_element_text(shopitem, 'DEFAULT_CATEGORY')

        # Product images
        product_images = []
        images_elem = shopitem.find('IMAGES')
        if images_elem is not None:
            for image in images_elem.findall('IMAGE'):
                product_images.append({
                    'image_url': image.text.strip() if image.text else '',
                    'image_description': image.get('description', '')
                })

        item_data['product_images'] = product_images

        # Custom attributes
        custom_attributes = []
        text_props_elem = shopitem.find('TEXT_PROPERTIES')
        if text_props_elem is not None:
            for prop in text_props_elem.findall('TEXT_PROPERTY'):
                attribute_name = self.get_element_text(prop, 'NAME')
                attribute_value = self.get_element_text(prop, 'VALUE')
                if attribute_name and attribute_value:
                    custom_attributes.append({
                        'attribute_name': attribute_name,
                        'attribute_value': attribute_value,
                        'attribute_description': self.get_element_text(prop, 'DESCRIPTION')
                    })

        item_data['custom_attributes'] = custom_attributes

        # Related product codes
        related_product_codes = []
        related_elem = shopitem.find('RELATED_PRODUCTS')
        if related_elem is not None:
            for code in related_elem.findall('CODE'):
                if code.text:
                    related_product_codes.append(code.text.strip())

        item_data['related_product_codes'] = related_product_codes

        # SEO metadata
        item_data['seo_page_title'] = self.get_element_text(shopitem, 'SEO_TITLE')
        item_data['seo_meta_description'] = self.get_element_text(shopitem, 'META_DESCRIPTION')

        return item_data

    def get_element_text(self, parent: ET.Element, tag_name: str) -> str:
        """Get text content of XML element"""
        element = parent.find(tag_name)
        return element.text.strip() if element is not None and element.text else ""

    def create_or_update_item(self, item_data: Dict[str, Any]) -> bool:
        """Create or update ERPNext Item"""
        try:
            item_code = item_data.get('item_code')
            if not item_code:
                self.add_error(f"Missing item code for XML ID: {item_data.get('xml_id')}")
                return False

            # Check if item exists
            existing_item = None
            if frappe.db.exists("Item", item_code):
                existing_item = frappe.get_doc("Item", item_code)
                is_update = True
            else:
                existing_item = frappe.new_doc("Item")
                is_update = False

            # Map basic fields - clean the names
            existing_item.item_code = item_code
            existing_item.item_name = self.clean_name(item_data.get('item_name', item_code)) or item_code

            # Set main description from DESCRIPTION tag
            if item_data.get('description'):
                existing_item.description = self.clean_html_content(item_data.get('description'))

            # Set short description in custom field (Text Editor)
            if item_data.get('short_description'):
                if hasattr(existing_item, 'short_description'):
                    existing_item.short_description = self.clean_html_content(item_data.get('short_description'))
                elif hasattr(existing_item, 'custom_short_description'):
                    existing_item.custom_short_description = self.clean_html_content(item_data.get('short_description'))

            # Set primary item group - prioritize DEFAULT_CATEGORY
            default_category = item_data.get('default_category')

            if default_category:
                # Use DEFAULT_CATEGORY as the primary item group
                existing_item.item_group = self.get_or_create_item_group(default_category)
            else:
                # Fall back to first category or default
                item_groups = self.map_categories(item_data.get('product_categories', []))
                if item_groups:
                    existing_item.item_group = item_groups[0]
                else:
                    existing_item.item_group = "All Item Groups"

            # Handle multiple categories (1:N relationship)
            self.handle_item_categories(existing_item, item_data.get('product_categories', []), default_category)

            # Set basic properties - use proper UOM
            existing_item.stock_uom = self.get_or_create_uom(item_data.get('unit_of_measure', 'Nos'))
            existing_item.is_stock_item = 1
            existing_item.include_item_in_manufacturing = 0
            existing_item.is_sales_item = 1
            existing_item.is_purchase_item = 1

            # Set brand (from manufacturer field in XML)
            brand_name = self.clean_name(item_data.get('manufacturer_name', ''))
            if brand_name:
                brand = self.get_or_create_brand(brand_name)
                if brand:
                    existing_item.brand = brand

            # Set supplier - link via Item Supplier child table
            supplier_name = self.clean_name(item_data.get('supplier_name', ''))
            if supplier_name:
                supplier = self.get_or_create_supplier(supplier_name)
                if supplier:
                    # Check if supplier already exists in item_defaults (item supplier list)
                    existing_suppliers = [d.supplier for d in getattr(existing_item, 'supplier_items', [])]

                    if supplier not in existing_suppliers:
                        # Add to supplier_items child table
                        existing_item.append('supplier_items', {
                            'supplier': supplier
                        })
                        frappe.logger().info(f"Added supplier '{supplier}' to item {item_code}")
                    else:
                        frappe.logger().debug(f"Supplier '{supplier}' already linked to item {item_code}")

            # Set standard buying price (purchase price)
            purchase_price = flt(item_data.get('purchase_price'))
            price_vat = flt(item_data.get('selling_price_with_tax'))
            if purchase_price:
                existing_item.standard_rate = purchase_price
            elif price_vat:
                existing_item.standard_rate = price_vat

            # Set valuation rate (required for stock accounting)
            if hasattr(existing_item, 'valuation_rate'):
                if purchase_price:
                    existing_item.valuation_rate = purchase_price
                elif price_vat:
                    existing_item.valuation_rate = price_vat

            # Custom fields for XML-specific data
            if hasattr(existing_item, 'xml_external_id'):
                existing_item.xml_external_id = item_data.get('external_id')
            if hasattr(existing_item, 'xml_guid'):
                existing_item.xml_guid = item_data.get('guid')
            if hasattr(existing_item, 'weight_per_unit'):
                existing_item.weight_per_unit = item_data.get('weight_kg')
            if hasattr(existing_item, 'xml_last_sync'):
                existing_item.xml_last_sync = now()

            # Handle barcode (EAN type) - update if changed
            new_barcode = item_data.get('barcode')
            if new_barcode:
                new_barcode = new_barcode.strip()
                # Validate EAN code (must be numeric and correct length)
                if self.is_valid_ean(new_barcode):
                    # Get all existing barcodes for this item
                    existing_barcodes = [b.barcode for b in getattr(existing_item, 'barcodes', [])]
                    if new_barcode not in existing_barcodes:
                        # Remove all old barcodes for this item
                        existing_item.barcodes = []
                        # Only add if not used by another item
                        if not frappe.db.get_value("Item Barcode", {"barcode": new_barcode}):
                            existing_item.append("barcodes", {
                                "barcode": new_barcode,
                                "barcode_type": "EAN"
                            })
                            frappe.logger().info(f"Updated EAN barcode to {new_barcode} for item {item_code}")
                        else:
                            frappe.logger().info(f"Barcode {new_barcode} already exists for another item, not updating {item_code}")
                else:
                    frappe.logger().warning(f"Invalid EAN barcode '{new_barcode}' for item {item_code}, skipping")

            # Handle tax information
            if item_data.get('tax_rate'):
                self.set_item_tax(existing_item, item_data)

            # Save the item
            if is_update:
                existing_item.save(ignore_permissions=True)
                self.updated_count += 1
                frappe.logger().info(f"Updated item: {item_code}")
            else:
                existing_item.insert(ignore_permissions=True)
                self.imported_count += 1
                frappe.logger().info(f"Created item: {item_code}")

            # Handle images
            if item_data.get('product_images'):
                self.handle_item_images(existing_item, item_data.get('product_images', []))

            # Create/update item price
            self.create_item_price(existing_item, item_data)

            # Update stock levels
            if item_data.get('current_stock') is not None:
                self.update_stock_levels(existing_item, item_data)

            frappe.db.commit()
            return True

        except Exception as e:
            frappe.db.rollback()
            error_msg = f"Failed to process item {item_data.get('item_code', 'Unknown')}: {str(e)}"
            self.add_error(error_msg)
            frappe.log_error(error_msg)
            frappe.logger().error(f"Item import error: {error_msg}")
            import traceback
            frappe.logger().error(traceback.format_exc())
            return False

    def handle_item_images(self, item_doc: Document, images: List[Dict]) -> None:
        """Handle item image downloads and attachments"""
        try:
            if not images:
                return

            # Download first image as main product image
            first_image = images[0]
            image_url = first_image.get('image_url', '')

            if not image_url:
                return

            # Check if image already attached
            existing_file = frappe.db.get_value(
                "File",
                {"file_url": image_url, "attached_to_doctype": "Item", "attached_to_name": item_doc.name},
                "name"
            )

            if not existing_file:
                # Download and attach image
                image_url = self.download_image(
                    image_url,
                    item_doc.item_code,
                    first_image.get('image_description', '')
                )

            if image_url:
                item_doc.image = image_url
                item_doc.save(ignore_permissions=True)

        except Exception as e:
            frappe.log_error(f"Failed to handle images for item {item_doc.item_code}: {str(e)}")

    def create_item_price(self, item_doc: Document, item_data: Dict[str, Any]) -> None:
        """Create or update item prices for both retail and wholesale"""
        try:
            currency = item_data.get('currency_code', 'EUR')

            # Standard retail selling price (with tax)
            selling_price = item_data.get('selling_price_with_tax')
            if selling_price:
                self.create_or_update_price("Standard Selling", item_doc.item_code, selling_price, currency)

            # Wholesale selling price (if available)
            wholesale_price = item_data.get('wholesale_price')
            if wholesale_price:
                # Ensure wholesale price list exists
                self.ensure_price_list_exists("Veľkoobchod", currency)
                self.create_or_update_price("Veľkoobchod", item_doc.item_code, wholesale_price, currency)

            # Standard buying price (purchase price)
            purchase_price = item_data.get('purchase_price')
            if purchase_price:
                # Ensure buying price list exists
                self.ensure_price_list_exists("Standard Buying", currency, selling=False)
                self.create_or_update_price("Standard Buying", item_doc.item_code, purchase_price, currency)

        except Exception as e:
            frappe.log_error(f"Failed to create item prices for {item_doc.item_code}: {str(e)}")

    def create_or_update_price(self, price_list: str, item_code: str, price: float, currency: str) -> None:
        """Create or update a single item price"""
        try:
            existing_price = frappe.db.get_value("Item Price", {
                "item_code": item_code,
                "price_list": price_list
            })

            if existing_price:
                price_doc = frappe.get_doc("Item Price", existing_price)
                price_doc.price_list_rate = price
                price_doc.save(ignore_permissions=True)
            else:
                price_doc = frappe.get_doc({
                    "doctype": "Item Price",
                    "item_code": item_code,
                    "price_list": price_list,
                    "price_list_rate": price,
                    "currency": currency
                })
                price_doc.insert(ignore_permissions=True)

            frappe.logger().info(f"Set {price_list} price for {item_code}: {price} {currency}")

        except Exception as e:
            frappe.log_error(f"Failed to create/update {price_list} price for {item_code}: {str(e)}")

    def ensure_price_list_exists(self, price_list_name: str, currency: str, selling: bool = True) -> None:
        """Ensure price list exists"""
        if not frappe.db.exists("Price List", price_list_name):
            try:
                price_list = frappe.get_doc({
                    "doctype": "Price List",
                    "price_list_name": price_list_name,
                    "currency": currency,
                    "selling": 1 if selling else 0,
                    "buying": 0 if selling else 1,
                    "enabled": 1
                })
                price_list.insert(ignore_permissions=True)
                frappe.logger().info(f"Created price list: {price_list_name}")
            except Exception as e:
                frappe.log_error(f"Failed to create price list {price_list_name}: {str(e)}")

    def update_stock_levels(self, item_doc: Document, item_data: Dict[str, Any]) -> None:
        """Update stock levels using Stock Entry"""
        try:
            # Get default warehouse
            warehouse = frappe.db.get_single_value("Stock Settings", "default_warehouse")
            if not warehouse:
                # Get first warehouse
                warehouse = frappe.db.get_value("Warehouse", {"company": self.company}, "name")

            if not warehouse:
                frappe.log_error(f"No warehouse found for company {self.company}")
                return

            current_stock = frappe.db.get_value("Bin", {
                "item_code": item_doc.item_code,
                "warehouse": warehouse
            }, "actual_qty") or 0

            target_qty = flt(item_data.get('current_stock', 0))
            difference = target_qty - current_stock

            if abs(difference) > 0.001:  # Only update if significant difference
                stock_entry = frappe.get_doc({
                    "doctype": "Stock Entry",
                    "stock_entry_type": "Material Receipt" if difference > 0 else "Material Issue",
                    "company": self.company,
                    "items": [{
                        "item_code": item_doc.item_code,
                        "qty": abs(difference),
                        "t_warehouse": warehouse if difference > 0 else None,
                        "s_warehouse": warehouse if difference < 0 else None,
                        "basic_rate": item_data.get('purchase_price', 0)
                    }]
                })
                stock_entry.insert(ignore_permissions=True)
                stock_entry.submit()

        except Exception as e:
            frappe.log_error(f"Failed to update stock for {item_doc.item_code}: {str(e)}")

    def add_error(self, error_msg: str) -> None:
        """Add error to error list"""
        self.errors.append(error_msg)
        self.error_count += 1

    def process_xml_content(self, xml_content: str) -> Dict[str, Any]:
        """Process XML content directly (for pasted content debugging)"""
        try:
            frappe.logger().info("Processing pasted XML content for item import")

            # Check if content is meaningful
            if not xml_content or len(xml_content.strip()) < 50:
                return {
                    "success": False,
                    "error": f"XML content is empty or too small: {len(xml_content) if xml_content else 0} bytes",
                    "imported_items": [],
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
                    "imported_items": [],
                    "errors": [error_msg]
                }

            # Process items
            imported_items = []
            processing_errors = []

            # Find item elements (could be SHOPITEM, item, product, etc.)
            item_elements = (root.findall('.//SHOPITEM') or
                           root.findall('.//item') or
                           root.findall('.//product'))
            frappe.logger().info(f"Found {len(item_elements)} item elements to process")

            for item_elem in item_elements:
                try:
                    item_doc = self.create_or_update_item(item_elem)
                    if item_doc:
                        imported_items.append(item_doc.item_code)
                        frappe.logger().info(f"Successfully processed item: {item_doc.item_code}")
                except Exception as e:
                    error_msg = f"Failed to process item: {str(e)}"
                    processing_errors.append(error_msg)
                    frappe.logger().error(error_msg)

            # Prepare summary
            success = len(imported_items) > 0
            summary = {
                "success": success,
                "imported_items": imported_items,
                "errors": processing_errors,
                "total_processed": len(item_elements),
                "successfully_imported": len(imported_items),
                "error_count": len(processing_errors)
            }

            frappe.logger().info(f"Pasted XML item processing completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"XML content processing failed: {str(e)}"
            frappe.log_error(error_msg)
            return {
                "success": False,
                "error": error_msg,
                "imported_items": [],
                "errors": [error_msg]
            }

    def import_from_xml(self) -> Dict[str, Any]:
        """Main import function"""
        try:
            frappe.logger().info(f"Starting XML import from: {self.xml_source}")

            # Fetch and parse XML
            xml_content = self.fetch_xml_content()
            root = self.parse_xml(xml_content)

            # Find all SHOPITEM elements
            shopitems = root.findall('.//SHOPITEM')

            frappe.logger().info(f"Found {len(shopitems)} items to process")

            # Process each item with progress updates
            for idx, shopitem in enumerate(shopitems, 1):
                try:
                    # Update progress
                    progress = (idx / len(shopitems)) * 100
                    frappe.publish_realtime(
                        "import_progress",
                        {
                            "current": idx,
                            "total": len(shopitems),
                            "percent": progress,
                            "message": f"Processing item {idx} of {len(shopitems)}"
                        },
                        user=frappe.session.user
                    )

                    frappe.logger().info(f"Processing item {idx}/{len(shopitems)}: ID {shopitem.get('id', 'Unknown')}")
                    item_data = self.parse_shop_item(shopitem)
                    success = self.create_or_update_item(item_data)
                    if not success:
                        frappe.logger().warning(f"Failed to import item {idx}: {item_data.get('item_code', 'Unknown')}")
                except Exception as e:
                    error_msg = f"Error processing SHOPITEM {idx} ID {shopitem.get('id', 'Unknown')}: {str(e)}"
                    frappe.logger().error(error_msg)
                    self.add_error(error_msg)
                    continue

            # Send completion message
            frappe.publish_realtime(
                "import_progress",
                {
                    "current": len(shopitems),
                    "total": len(shopitems),
                    "percent": 100,
                    "message": "Import completed",
                    "completed": True
                },
                user=frappe.session.user
            )

            frappe.logger().info(f"Completed processing {len(shopitems)} items")

            # Return summary
            summary = {
                "success": True,
                "imported": self.imported_count,
                "updated": self.updated_count,
                "errors": self.error_count,
                "error_messages": self.errors[:10],  # First 10 errors
                "total_processed": len(shopitems)
            }

            frappe.logger().info(f"Import completed: {summary}")
            return summary

        except Exception as e:
            error_msg = f"XML import failed: {str(e)}"
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
def import_xml_items(xml_source: str, company: str = None) -> Dict[str, Any]:
    """
    Import items from XML feed

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)

    Returns:
        Dict with import results
    """
    importer = XMLItemImporter(xml_source, company)
    return importer.import_from_xml()


def scheduled_xml_import():
    """
    Scheduled function to import XML items based on configured frequency
    This is called by the scheduler and checks each configuration's frequency setting
    """
    # Get all enabled XML Import Configurations
    configs = frappe.get_all(
        "XML Import Configuration",
        filters={"enabled": 1},
        fields=["name", "import_type", "xml_feed_url", "company", "import_frequency", "last_import"]
    )

    if not configs:
        frappe.logger().debug("No enabled XML Import Configurations found")
        return

    from datetime import datetime, timedelta
    from frappe.utils import now_datetime, get_datetime

    for config in configs:
        try:
            # Check if it's time to import based on frequency
            if not should_run_import(config):
                continue

            frappe.logger().info(f"Running scheduled import for {config.name} ({config.import_type})")

            if config.import_type == "Items":
                result = import_xml_items(config.xml_feed_url, config.company)

                # Create import log
                from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_item_import_log
                create_item_import_log(
                    xml_source=config.xml_feed_url,
                    status="Success" if result.get("success") else "Failed",
                    imported=result.get("imported", 0),
                    updated=result.get("updated", 0),
                    errors=result.get("errors", 0),
                    error_details="\n".join(result.get("error_messages", [])),
                    summary=result
                )

            elif config.import_type == "Orders":
                from xml_importer.xml_importer.order_importer import import_xml_orders
                result = import_xml_orders(config.xml_feed_url, config.company)

                # Create import log
                from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_order_import_log
                create_order_import_log(
                    xml_source=config.xml_feed_url,
                    status="Success" if result.get("success") else "Failed",
                    imported=result.get("imported", 0),
                    errors=result.get("errors", 0),
                    error_details="\n".join(result.get("error_messages", [])),
                    summary=result
                )

            # Update last import time
            frappe.db.set_value("XML Import Configuration", config.name, {
                "last_import": now_datetime(),
                "last_import_status": "Success" if result.get("success") else "Failed"
            })
            frappe.db.commit()

        except Exception as e:
            frappe.log_error(f"Scheduled XML import error for {config.name}: {str(e)}")
            frappe.db.set_value("XML Import Configuration", config.name, {
                "last_import": now_datetime(),
                "last_import_status": "Failed"
            })
            frappe.db.commit()


def should_run_import(config):
    """
    Check if import should run based on frequency setting and last import time
    """
    from frappe.utils import now_datetime, get_datetime
    from datetime import timedelta

    if not config.get("last_import"):
        # Never imported before - run it
        return True

    last_import = get_datetime(config.last_import)
    now = now_datetime()
    time_diff = now - last_import

    frequency = config.get("import_frequency", "Hourly")

    # Map frequency to minutes
    frequency_map = {
        "Every 5 Minutes": 5,
        "Every 10 Minutes": 10,
        "Every 15 Minutes": 15,
        "Every 30 Minutes": 30,
        "Hourly": 60,
        "Every 2 Hours": 120,
        "Every 6 Hours": 360,
        "Daily": 1440,
        "Weekly": 10080
    }

    required_minutes = frequency_map.get(frequency, 60)  # Default to hourly
    required_delta = timedelta(minutes=required_minutes)

    should_run = time_diff >= required_delta

    if should_run:
        frappe.logger().info(
            f"Import scheduled for {config.name}: Last import {time_diff.total_seconds()/60:.1f} mins ago, "
            f"frequency is {frequency} ({required_minutes} mins)"
        )

    return should_run


def send_import_notification(result: Dict[str, Any], recipients: str):
    """Send email notification about import results"""
    try:
        recipients_list = [email.strip() for email in recipients.split(",")]

        subject = "XML Item Import Results"

        if result.get("success"):
            message = f"""
            XML Item Import completed successfully:

            - Items imported: {result.get('imported', 0)}
            - Items updated: {result.get('updated', 0)}
            - Errors: {result.get('errors', 0)}
            - Total processed: {result.get('total_processed', 0)}

            Time: {now()}
            """
        else:
            message = f"""
            XML Item Import failed:

            Error: {result.get('error', 'Unknown error')}
            Time: {now()}
            """

        if result.get('error_messages'):
            message += f"\n\nFirst few errors:\n" + "\n".join(result['error_messages'])

        frappe.sendmail(
            recipients=recipients_list,
            subject=subject,
            message=message
        )

    except Exception as e:
        frappe.log_error(f"Failed to send import notification: {str(e)}")
