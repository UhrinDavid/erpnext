"""
XML Item Importer for ERPNext
A module that imports items from XML feeds into ERPNext Item documents

Author: Herbatica
License: MIT
"""

import frappe
import requests
import xml.etree.ElementTree as ET
import xml.sax
import traceback
from frappe.model.document import Document
from frappe.utils import now, cstr, flt, cint, strip_html_tags
from frappe.utils.file_manager import save_file
import re
import os
import json
import uuid
import time
import gc
import psutil
from urllib.parse import urlparse
from typing import Dict, List, Optional, Any, Callable
from datetime import datetime

# Redis import with fallback
try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    redis = None

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
        """Pass-through - return as-is"""
        return content or ""

    def clean_text_content(self, content: str) -> str:
        """Pass-through - return as-is"""
        return content or ""

    def clean_name(self, name: str) -> str:
        """Pass-through - return as-is"""
        return name or ""

    def clean_category_name(self, name: str) -> str:
        """Pass-through - return as-is"""
        return name or ""

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

            # Check if file with same name already exists for this item
            existing_file = frappe.db.get_value(
                "File",
                {"file_name": filename, "attached_to_doctype": "Item", "attached_to_name": item_code},
                "name"
            )

            if existing_file:
                # Delete old file and replace with new content
                old_file = frappe.get_doc("File", existing_file)
                old_file_url = old_file.file_url
                old_file.delete(ignore_permissions=True)
                frappe.logger().info(f"Replaced existing image {filename} for item {item_code}")

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
            frappe.log_error(f"Failed to download image {image_url} for item {item_code}: {str(e)}\n{traceback.format_exc()}")
            return None

    def map_categories(self, categories: List[Dict]) -> List[str]:
        """Map XML categories to ERPNext Item Groups"""
        item_groups = []

        for category in categories:
            category_name = category.get('category_name', '').strip()
            if not category_name:
                continue

            # Skip if name is empty
            if not category_name:
                continue

            # Check if Item Group exists, create if not
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
        if not category_name:
            return "All Item Groups"

        # Use category name as-is without modifications
        category_name = self.clean_category_name(category_name)

        # If category has hierarchy (e.g. "Parent > Child > Final"), use only the last part
        if ">" in category_name:
            safe_name = category_name.split(">")[-1].strip()
        else:
            safe_name = category_name.strip()

        if not frappe.db.exists("Item Group", safe_name):
            try:
                item_group = frappe.get_doc({
                    "doctype": "Item Group",
                    "item_group_name": safe_name,
                    "parent_item_group": "All Item Groups",
                    "is_group": 0
                })
                item_group.insert(ignore_permissions=True)
                frappe.logger().info(f"Created Item Group: {safe_name}")
            except Exception as e:
                frappe.log_error(f"Failed to create Item Group {safe_name}: {str(e)}")
                return "All Item Groups"

        return safe_name

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
            # Use EAN type only if checksum is valid, otherwise no type (skip validation)
            barcode_type = "EAN" if self.is_valid_ean_checksum(barcode_value) else ""
            item_doc.append("barcodes", {
                "barcode": barcode_value,
                "barcode_type": barcode_type
            })
            frappe.logger().info(f"Added barcode {barcode_value} (type: {barcode_type or 'none'}) to item {item_doc.item_code}")
        except Exception as e:
            frappe.log_error(f"Failed to add barcode {barcode_value} to item {item_doc.item_code}: {str(e)}\n{traceback.format_exc()}")

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

    def is_valid_ean_checksum(self, barcode: str) -> bool:
        """Validate EAN/UPC check digit - returns True if checksum is valid"""
        if not barcode or not barcode.isdigit():
            return False

        if len(barcode) not in [8, 12, 13, 14]:
            return False

        # EAN/UPC checksum algorithm
        digits = [int(d) for d in barcode]
        if len(barcode) in [13, 8]:
            # EAN-13 or EAN-8
            odd_sum = sum(digits[::2][:-1])  # odd positions (exclude check digit)
            even_sum = sum(digits[1::2])     # even positions
            if len(barcode) == 13:
                total = odd_sum + even_sum * 3
            else:  # EAN-8
                total = odd_sum * 3 + even_sum
            check_digit = (10 - (total % 10)) % 10
            return check_digit == digits[-1]
        elif len(barcode) == 12:
            # UPC-A
            odd_sum = sum(digits[::2][:-1])
            even_sum = sum(digits[1::2])
            total = odd_sum * 3 + even_sum
            check_digit = (10 - (total % 10)) % 10
            return check_digit == digits[-1]
        elif len(barcode) == 14:
            # GTIN-14
            odd_sum = sum(digits[1::2])
            even_sum = sum(digits[::2][:-1])
            total = odd_sum + even_sum * 3
            check_digit = (10 - (total % 10)) % 10
            return check_digit == digits[-1]

        return False

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
            frappe.log_error(f"Failed to set tax info for item {item_doc.item_code}: {str(e)}\n{traceback.format_exc()}")

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
                    # Ensure the category exists as an item group (this also sanitizes the name)
                    safe_category_name = self.get_or_create_item_group(category_name)
                    additional_categories.append(safe_category_name)

            # Remove duplicates while preserving order
            unique_additional_categories = []
            seen = set()
            for cat in additional_categories:
                if cat not in seen:
                    unique_additional_categories.append(cat)
                    seen.add(cat)

            # Store additional categories in custom field (comma-separated)
            # Categories already have '>' replaced with '&gt;' from get_or_create_item_group
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
            if hasattr(item_doc, 'published_in_website') and item_doc.published_in_website:
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
            frappe.log_error(f"Failed to handle categories for item {item_doc.item_code}: {str(e)}\n{traceback.format_exc()}")

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

    def normalize_item_data(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize item data field names from various SAX parser formats.
        Maps old field names to expected field names for consistency.
        """
        # Field mapping: old_name -> new_name
        field_mappings = {
            # Basic fields
            'item_id': 'external_id',
            'product_name': 'item_name',
            'name': 'item_name',
            # Pricing
            'price_vat': 'selling_price_with_tax',
            'price': 'selling_price_with_tax',
            'vat_rate': 'tax_rate',
            # Product details
            'manufacturer': 'manufacturer_name',
            'ean': 'barcode',
            'unit': 'unit_of_measure',
            'weight': 'weight_kg',
            'currency': 'currency_code',
            # Stock
            'stock_quantity': 'current_stock',
            'stock_amount': 'current_stock',
            # Categories - handle list conversion
            'categories': 'product_categories',
            # Images - handle list conversion
            'images': 'product_images',
        }

        normalized = item_data.copy()

        # FIRST: Get item_name from the top-level NAME tag (stored in item_name or additional_data.name)
        # This takes priority over other name fields like product_name
        additional = normalized.get('additional_data', {})
        top_level_name = normalized.get('item_name') or additional.get('name')

        # Apply field mappings (only if target doesn't exist or is empty)
        for old_key, new_key in field_mappings.items():
            if old_key in normalized and new_key not in normalized:
                normalized[new_key] = normalized[old_key]
            elif old_key in normalized and not normalized.get(new_key):
                normalized[new_key] = normalized[old_key]

        # ALWAYS use the top-level NAME if available (overrides product_name mapping)
        if top_level_name:
            normalized['item_name'] = top_level_name

        # Handle default_category from additional_data or direct field
        if not normalized.get('default_category'):
            if additional.get('default_category'):
                normalized['default_category'] = additional['default_category']

        # Normalize categories list format
        if 'product_categories' in normalized:
            cats = normalized['product_categories']
            if cats and isinstance(cats, list):
                # Convert old format [{category_id, category_name}] to new format
                normalized_cats = []
                for cat in cats:
                    if isinstance(cat, dict):
                        normalized_cats.append({
                            'category_id': cat.get('category_id', cat.get('id', '')),
                            'category_name': cat.get('category_name', cat.get('name', ''))
                        })
                    elif isinstance(cat, str):
                        normalized_cats.append({'category_id': '', 'category_name': cat})
                normalized['product_categories'] = normalized_cats

        # Normalize images list format
        if 'product_images' in normalized:
            imgs = normalized['product_images']
            if imgs and isinstance(imgs, list):
                normalized_imgs = []
                for img in imgs:
                    if isinstance(img, dict):
                        normalized_imgs.append({
                            'image_url': img.get('image_url', img.get('url', '')),
                            'image_description': img.get('image_description', img.get('description', ''))
                        })
                    elif isinstance(img, str):
                        normalized_imgs.append({'image_url': img, 'image_description': ''})
                normalized['product_images'] = normalized_imgs

        # Extract wholesale_price from pricelists array
        pricelists = normalized.get('pricelists', [])
        for pl in pricelists:
            if isinstance(pl, dict):
                title = pl.get('title', '')
                # Match "Veľkoobchod" exactly (no string modifications)
                if title == 'Veľkoobchod':
                    try:
                        normalized['wholesale_price'] = float(pl.get('price_vat', 0))
                    except (ValueError, TypeError):
                        pass

        return normalized

    def create_or_update_item(self, item_data: Dict[str, Any]) -> bool:
        """Create or update ERPNext Item"""
        try:
            # Normalize field names from various SAX parser formats
            item_data = self.normalize_item_data(item_data)

            # Debug: Log what we received
            item_id = item_data.get('item_code') or item_data.get('external_id') or 'Unknown'
            frappe.logger().debug(f"Processing item {item_id}: item_name='{item_data.get('item_name')}', default_category='{item_data.get('default_category')}'")

            # Validate and clean input data
            item_code = item_data.get('item_code')
            if not item_code or not item_code.strip():
                # Try fallback to external_id
                item_code = item_data.get('external_id')
                if not item_code:
                    error_msg = f"Missing item code for item with external_id: {item_data.get('external_id', 'Unknown')}"
                    self.add_error(error_msg)
                    frappe.logger().warning(f"{error_msg}. Available fields: {list(item_data.keys())}")
                    return False
                else:
                    item_data['item_code'] = item_code

            # Clean item code - remove any problematic characters
            item_code = str(item_code).strip()

            # Validate item name
            item_name = item_data.get('item_name')
            if not item_name or not item_name.strip():
                # Use item_code as fallback
                item_name = item_code
                frappe.logger().info(f"Using item_code as item_name for {item_code}")

            # Clean item name
            item_name = self.clean_name(str(item_name)) or item_code

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
            existing_item.item_name = item_name

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

            try:
                if default_category:
                    # Use DEFAULT_CATEGORY as the primary item group (no string modifications)
                    existing_item.item_group = self.get_or_create_item_group(default_category)
                    frappe.logger().debug(f"Set item group from default_category: {default_category}")
                else:
                    # Fall back to first category or default
                    item_groups = self.map_categories(item_data.get('product_categories', []))
                    if item_groups:
                        existing_item.item_group = item_groups[0]
                        frappe.logger().debug(f"Set item group from first category: {item_groups[0]}")
                    else:
                        existing_item.item_group = "All Item Groups"
                        frappe.logger().debug(f"Using default item group: All Item Groups")
            except Exception as e:
                frappe.logger().error(f"Error setting item group for {item_code}: {str(e)}")
                existing_item.item_group = "All Item Groups"

            # Handle multiple categories (1:N relationship)
            try:
                self.handle_item_categories(existing_item, item_data.get('product_categories', []), default_category)
            except Exception as e:
                frappe.logger().error(f"Error handling categories for {item_code}: {str(e)}")

            # Set basic properties - use proper UOM
            uom = self.get_or_create_uom(item_data.get('unit_of_measure', 'Nos'))
            existing_item.stock_uom = uom

            # Determine if this is a set item (item_type='set' AND has set_items data)
            is_set_item = item_data.get('item_type') == 'set' and item_data.get('set_items')
            has_existing_bundle = frappe.db.exists("Product Bundle", item_code)

            # Handle is_stock_item setting
            if has_existing_bundle:
                # If there's an existing bundle, ALWAYS keep is_stock_item = 0
                existing_item.is_stock_item = 0
            elif is_set_item:
                # New set item - mark as non-stock (will create bundle later)
                existing_item.is_stock_item = 0
            elif not is_update:
                # New regular item - mark as stock item
                existing_item.is_stock_item = 1
            # For existing regular items, don't change is_stock_item

            existing_item.include_item_in_manufacturing = 0
            existing_item.is_sales_item = 1
            existing_item.is_purchase_item = 1

            # Ensure no duplicate UOM conversions - keep only one per UOM
            existing_uoms = {}
            for u in getattr(existing_item, 'uoms', []):
                existing_uoms[u.uom] = u
            existing_item.uoms = list(existing_uoms.values())

            # Ensure no duplicate Item Defaults - keep only one per company
            existing_defaults = {}
            for d in getattr(existing_item, 'item_defaults', []):
                existing_defaults[d.company] = d
            existing_item.item_defaults = list(existing_defaults.values())

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
                # Clean barcode - remove spaces and dashes
                new_barcode = new_barcode.strip().replace(" ", "").replace("-", "")
                # Validate EAN code (must be numeric and correct length)
                if self.is_valid_ean(new_barcode):
                    # Get all existing barcodes for this item
                    existing_barcodes = [b.barcode for b in getattr(existing_item, 'barcodes', [])]
                    if new_barcode not in existing_barcodes:
                        # Remove all old barcodes for this item
                        existing_item.barcodes = []
                        # Only add if not used by another item
                        if not frappe.db.get_value("Item Barcode", {"barcode": new_barcode}):
                            # Use EAN type only if checksum is valid, otherwise no type (skip validation)
                            barcode_type = "EAN" if self.is_valid_ean_checksum(new_barcode) else ""
                            existing_item.append("barcodes", {
                                "barcode": new_barcode,
                                "barcode_type": barcode_type
                            })
                            frappe.logger().info(f"Updated barcode to {new_barcode} (type: {barcode_type or 'none'}) for item {item_code}")
                        else:
                            frappe.logger().info(f"Barcode {new_barcode} already exists for another item, not updating {item_code}")
                else:
                    frappe.logger().warning(f"Invalid barcode '{new_barcode}' for item {item_code}, skipping")

            # Handle tax information
            if item_data.get('tax_rate'):
                self.set_item_tax(existing_item, item_data)

            # Note: is_set_item and has_existing_bundle were already set earlier
            # and is_stock_item was already set correctly based on those values

            # Save the item
            if is_update:
                existing_item.save(ignore_permissions=True)
                self.updated_count += 1
                frappe.logger().info(f"Updated item: {item_code}")
            else:
                existing_item.insert(ignore_permissions=True)
                self.imported_count += 1
                frappe.logger().info(f"Created item: {item_code}")

            # Handle images (only if download_images is enabled in config)
            should_download_images = True
            if self.config:
                should_download_images = self.config.get('download_images', True)

            if should_download_images:
                if item_data.get('product_images'):
                    self.handle_item_images(existing_item, item_data.get('product_images', []))
                elif item_data.get('image_url'):
                    # Handle single image URL (legacy format)
                    self.handle_item_images(existing_item, [{'image_url': item_data.get('image_url')}])
            else:
                # Log that images are being skipped
                if item_data.get('product_images') or item_data.get('image_url'):
                    frappe.logger().debug(f"Skipping image download for {item_code} (download_images disabled in config)")

            # Create/update item price
            self.create_item_price(existing_item, item_data)

            # For set items, create/update Product Bundle
            # is_set_item and has_existing_bundle were already determined before save
            if is_set_item:
                # Create/update Product Bundle
                # This will mark the item as non-stock if needed
                self.create_product_bundle(existing_item, item_data.get('set_items', []))
                # Reload item to get updated is_stock_item value
                existing_item.reload()

            # Update stock levels - but SKIP for non-stock items (bundles)
            # handle both string and numeric values
            if not is_set_item and existing_item.is_stock_item:
                stock_qty = item_data.get('current_stock')
                if stock_qty is not None and stock_qty != '':
                    try:
                        stock_qty_float = flt(stock_qty)
                        if stock_qty_float >= 0:
                            self.update_stock_levels(existing_item, {'current_stock': stock_qty_float, 'purchase_price': item_data.get('purchase_price', 0)})
                    except (ValueError, TypeError):
                        frappe.logger().warning(f"Invalid stock quantity '{stock_qty}' for item {item_code}")

            frappe.db.commit()
            return True

        except Exception as e:
            frappe.db.rollback()
            item_identifier = item_data.get('item_code') or item_data.get('external_id') or 'Unknown'
            error_msg = f"Failed to process item {item_identifier}: {str(e)}"

            # Add detailed error information
            error_details = {
                "item_identifier": item_identifier,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "item_data_keys": list(item_data.keys()) if item_data else [],
                "item_name": item_data.get('item_name', 'Not set'),
                "default_category": item_data.get('default_category', 'Not set'),
                "categories_count": len(item_data.get('product_categories', [])),
            }

            detailed_error_msg = f"{error_msg}\nDetails: {json.dumps(error_details, indent=2)}"
            self.add_error(detailed_error_msg)
            frappe.log_error(detailed_error_msg, "XML Item Import Error")
            frappe.logger().error(f"Item import error: {detailed_error_msg}")
            frappe.logger().error(f"Full traceback: {traceback.format_exc()}")
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

            # Download and attach image (will replace if exists)
            local_image_url = self.download_image(
                image_url,
                item_doc.item_code,
                first_image.get('image_description', '')
            )

            if local_image_url:
                item_doc.image = local_image_url
                item_doc.save(ignore_permissions=True)

        except Exception as e:
            frappe.log_error(f"Failed to handle images for item {item_doc.item_code}: {str(e)}\n{traceback.format_exc()}")

    def create_item_price(self, item_doc: Document, item_data: Dict[str, Any]) -> None:
        """Create or update item prices for both retail and wholesale"""
        try:
            currency = item_data.get('currency_code', 'EUR')

            # Standard retail selling price (with tax)
            selling_price = flt(item_data.get('selling_price_with_tax'))
            if selling_price:
                self.create_or_update_price("Standard Selling", item_doc.item_code, selling_price, currency)

            # Wholesale selling price (if available)
            wholesale_price = flt(item_data.get('wholesale_price'))
            if wholesale_price:
                self.ensure_price_list_exists("Veľkoobchod", currency)
                self.create_or_update_price("Veľkoobchod", item_doc.item_code, wholesale_price, currency)

            # Standard buying price (purchase price)
            purchase_price = flt(item_data.get('purchase_price'))
            if purchase_price:
                self.ensure_price_list_exists("Standard Buying", currency, selling=False)
                self.create_or_update_price("Standard Buying", item_doc.item_code, purchase_price, currency)

        except Exception as e:
            frappe.log_error(f"Failed to create item prices for {item_doc.item_code}: {str(e)}\n{traceback.format_exc()}")

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
            frappe.log_error(f"Failed to create/update {price_list} price for {item_code}: {str(e)}\n{traceback.format_exc()}")

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

    def create_product_bundle(self, item_doc: Document, set_items: List[Dict[str, Any]]) -> None:
        """
        Create or update a Product Bundle for set items.

        Note: Product Bundles in ERPNext are for items that DON'T maintain stock.
        When a bundle item is sold, it expands into component items.
        If the set item has existing stock, we clear it first to allow
        conversion to a non-stock Product Bundle.

        Args:
            item_doc: The parent Item document (the set)
            set_items: List of component items with 'code' and 'amount' keys
        """
        try:
            if not set_items:
                return

            item_code = item_doc.item_code

            # Check if item has stock - if so, clear it first
            # because ERPNext Product Bundles require is_stock_item = 0
            stock_by_warehouse = frappe.db.sql("""
                SELECT warehouse, actual_qty
                FROM tabBin
                WHERE item_code = %s AND actual_qty != 0
            """, item_code, as_dict=True)

            if stock_by_warehouse:
                total_stock = sum(s.actual_qty for s in stock_by_warehouse)
                frappe.logger().info(f"Set item {item_code} has stock ({total_stock}), clearing stock to convert to Product Bundle")

                # Clear stock from each warehouse
                for bin_data in stock_by_warehouse:
                    self._clear_stock_for_bundle(item_code, bin_data.warehouse, bin_data.actual_qty)

            # Also clear any Stock Entry records that reference this item
            # This is needed to allow changing is_stock_item
            self._clear_stock_entries_for_item(item_code)

            # Check if Product Bundle already exists
            existing_bundle = frappe.db.exists("Product Bundle", item_code)

            if existing_bundle:
                # Update existing bundle
                bundle_doc = frappe.get_doc("Product Bundle", item_code)
                # Clear existing items
                bundle_doc.items = []
            else:
                # Create new bundle - truncate description to max 140 chars
                bundle_description = (item_doc.item_name or item_code)[:140]
                bundle_doc = frappe.get_doc({
                    "doctype": "Product Bundle",
                    "new_item_code": item_code,
                    "description": bundle_description
                })

            # Add component items
            items_added = 0
            missing_items = []

            for set_item in set_items:
                component_code = set_item.get('code', '').strip()
                qty = set_item.get('amount', 1)

                if not component_code:
                    continue

                # Check if component item exists
                if not frappe.db.exists("Item", component_code):
                    missing_items.append(component_code)
                    frappe.logger().warning(f"Product Bundle {item_code}: Component item {component_code} does not exist, skipping")
                    continue

                bundle_doc.append("items", {
                    "item_code": component_code,
                    "qty": qty,
                    "description": frappe.db.get_value("Item", component_code, "item_name") or component_code
                })
                items_added += 1

            # Only save if we have items
            if items_added > 0:
                # First, mark the item as non-stock (required for Product Bundle)
                # BUT only if it's currently a stock item and no bundle exists yet
                if item_doc.is_stock_item and not existing_bundle:
                    item_doc.is_stock_item = 0
                    item_doc.save(ignore_permissions=True)
                    frappe.logger().info(f"Set {item_code} as non-stock item for Product Bundle")

                if existing_bundle:
                    bundle_doc.save(ignore_permissions=True)
                    frappe.logger().info(f"Updated Product Bundle for {item_code} with {items_added} components")
                else:
                    bundle_doc.insert(ignore_permissions=True)
                    frappe.logger().info(f"Created Product Bundle for {item_code} with {items_added} components")
            else:
                if missing_items:
                    frappe.logger().warning(f"Could not create Product Bundle for {item_code}: all component items missing: {missing_items}")

        except Exception as e:
            frappe.log_error(f"Failed to create Product Bundle for {item_doc.item_code}: {str(e)}\\n{traceback.format_exc()}")

    def _clear_stock_for_bundle(self, item_code: str, warehouse: str, qty: float) -> None:
        """
        Clear stock for an item in preparation for converting it to a Product Bundle.
        Uses direct SQL deletion for reliability (avoids validation issues with Stock Entries).

        Args:
            item_code: The item code to clear stock for
            warehouse: The warehouse to clear stock from
            qty: The quantity to clear (not used - we delete all stock data)
        """
        try:
            # Delete Stock Ledger Entries for this item and warehouse
            frappe.db.sql("""
                DELETE FROM `tabStock Ledger Entry`
                WHERE item_code = %s AND warehouse = %s
            """, (item_code, warehouse))

            # Delete the Bin record
            frappe.db.sql("""
                DELETE FROM `tabBin`
                WHERE item_code = %s AND warehouse = %s
            """, (item_code, warehouse))

            frappe.db.commit()
            frappe.logger().info(f"Cleared stock data for {item_code} from {warehouse} for Product Bundle conversion")

        except Exception as e:
            frappe.log_error(f"Failed to clear stock for {item_code} in {warehouse}: {str(e)}\n{traceback.format_exc()}")
            # Don't raise - try to continue with bundle creation

    def _clear_stock_entries_for_item(self, item_code: str) -> None:
        """
        Clear Stock Entry records that reference this item.
        This is needed to allow changing is_stock_item from 1 to 0.

        Args:
            item_code: The item code to clear stock entries for
        """
        try:
            # Find Stock Entries that contain this item
            stock_entries = frappe.db.sql("""
                SELECT DISTINCT sed.parent
                FROM `tabStock Entry Detail` sed
                WHERE sed.item_code = %s
            """, item_code, as_dict=True)

            if not stock_entries:
                return

            frappe.logger().info(f"Clearing {len(stock_entries)} Stock Entries for item {item_code}")

            for se in stock_entries:
                # Delete Stock Entry Detail records
                frappe.db.sql("""
                    DELETE FROM `tabStock Entry Detail` WHERE parent = %s
                """, se.parent)

                # Delete the Stock Entry
                frappe.db.sql("""
                    DELETE FROM `tabStock Entry` WHERE name = %s
                """, se.parent)

            # Also clear all Stock Ledger Entries and Bins for this item
            frappe.db.sql("DELETE FROM `tabStock Ledger Entry` WHERE item_code = %s", item_code)
            frappe.db.sql("DELETE FROM `tabBin` WHERE item_code = %s", item_code)

            # Clear GL Entries related to stock for this item
            frappe.db.sql("""
                DELETE FROM `tabGL Entry`
                WHERE voucher_type = 'Stock Entry'
                AND voucher_no IN (SELECT name FROM `tabStock Entry` WHERE name IN
                    (SELECT DISTINCT parent FROM `tabStock Entry Detail` WHERE item_code = %s))
            """, item_code)

            frappe.db.commit()
            frappe.logger().info(f"Cleared all stock entries for item {item_code}")

        except Exception as e:
            frappe.log_error(f"Failed to clear stock entries for {item_code}: {str(e)}\n{traceback.format_exc()}")
            # Don't raise - try to continue

    def update_stock_levels(self, item_doc: Document, item_data: Dict[str, Any]) -> None:
        """Update stock levels using Stock Entry"""
        try:
            # Skip stock updates for non-stock items (like Product Bundles)
            if not item_doc.is_stock_item:
                frappe.logger().debug(f"Skipping stock update for non-stock item {item_doc.item_code}")
                return

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
            frappe.log_error(f"Failed to update stock for {item_doc.item_code}: {str(e)}\n{traceback.format_exc()}")

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
    Import items from XML feed using SAX parser with Redis queue

    This function now always uses the memory-optimized SAX parser with Redis queue
    instead of the DOM-based ElementTree parser.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)

    Returns:
        Dict with import results
    """
    # Always use SAX parser with Redis queue for memory efficiency
    return import_xml_items_sax(xml_source, company, use_queue=True)


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

        frappe.sendmail(
            recipients=recipients,
            subject=subject,
            message=message
        )

    except Exception as e:
        frappe.log_error(f"Failed to send import notification email: {str(e)}")


# ================================
# SAX PARSER CLASSES FOR MEMORY OPTIMIZATION
# ================================

class SAXItemHandler(xml.sax.ContentHandler):
    """
    SAX Content Handler for parsing XML items and queuing them in Redis
    Memory-efficient streaming XML parser that extracts individual SHOPITEM elements
    """

    # Redis key for storing import progress
    PROGRESS_KEY = "xml_import_progress"

    def __init__(self, redis_client=None, queue_name: str = None):
        """Initialize SAX handler for Redis queue processing only"""
        super().__init__()
        self.redis_client = redis_client
        self.queue_name = queue_name

        # Parser state
        self.current_element = None
        self.current_data = ""
        self.current_item = {}
        self.element_stack = []
        self.in_shopitem = False
        self.in_categories = False
        self.in_images = False
        self.in_variants = False
        self.in_variant = False
        self.in_pricelists = False
        self.in_pricelist = False
        self.in_parameters = False
        self.in_set_items = False
        self.in_set_item = False
        self.current_category = {}
        self.current_image = {}
        self.current_supplier = {}
        self.current_variant = {}
        self.current_pricelist = {}
        self.current_parameter = {}
        self.current_set_item = {}

        # Counters
        self.items_queued = 0
        self.items_processed = 0
        self.parse_errors = 0

        # Progress tracking
        self.last_progress_update = datetime.now()

        # Initialize progress in Redis
        if self.redis_client:
            self._init_progress()

    def _init_progress(self):
        """Initialize progress tracking in Redis"""
        progress_data = {
            "phase": "parsing",
            "queue_name": self.queue_name,
            "total_items": 0,
            "processed": 0,
            "errors": 0,
            "start_time": datetime.now().isoformat(),
            "last_update": datetime.now().isoformat(),
            "status": "running"
        }
        self.redis_client.set(self.PROGRESS_KEY, json.dumps(progress_data))
        # Set expiry of 1 hour
        self.redis_client.expire(self.PROGRESS_KEY, 3600)

    def _update_redis_progress(self, phase: str = "parsing", processed: int = 0, errors: int = 0):
        """Update progress in Redis"""
        if not self.redis_client:
            return
        try:
            progress_data = {
                "phase": phase,
                "queue_name": self.queue_name,
                "total_items": self.items_queued,
                "processed": processed,
                "errors": errors,
                "start_time": None,  # Keep existing
                "last_update": datetime.now().isoformat(),
                "status": "running"
            }
            # Get existing start_time
            existing = self.redis_client.get(self.PROGRESS_KEY)
            if existing:
                existing_data = json.loads(existing)
                progress_data["start_time"] = existing_data.get("start_time")
            self.redis_client.set(self.PROGRESS_KEY, json.dumps(progress_data))
        except Exception as e:
            frappe.log_error(f"Failed to update progress: {str(e)}")

    def startElement(self, name: str, attrs):
        """Handle start of XML element"""
        self.element_stack.append(name)
        self.current_element = name
        self.current_data = ""

        if name == "SHOPITEM":
            self.in_shopitem = True
            self.in_pricelists = False
            self.in_pricelist = False
            self.in_variants = False
            self.in_variant = False
            self.in_parameters = False
            self.current_pricelist = {}
            self.current_variant = {}
            self.current_parameter = {}
            self.current_item = {
                "item_id": attrs.get("id", ""),
                "categories": [],
                "images": [],
                "pricelists": [],
                "variants": [],
                "set_items": [],
                "additional_data": {}
            }

        elif self.in_shopitem:
            if name == "CATEGORIES":
                self.in_categories = True
            elif name == "CATEGORY" and self.in_categories:
                self.current_category = {
                    "category_id": attrs.get("id", ""),
                    "category_name": ""
                }
            elif name == "DEFAULT_CATEGORY" and self.in_categories:
                self.current_item["default_category_id"] = attrs.get("id", "")
            elif name == "IMAGES":
                self.in_images = True
            elif name == "IMAGE" and self.in_images:
                self.current_image = {
                    "url": "",
                    "description": attrs.get("description", "")
                }
            elif name == "CATEGORYTEXT":
                self.current_category = {
                    "category_id": attrs.get("id", ""),
                    "category_name": ""
                }
            elif name == "VARIANTS":
                self.in_variants = True
            elif name == "VARIANT" and self.in_variants:
                self.in_variant = True
                self.current_variant = {
                    "variant_id": attrs.get("id", ""),
                    "pricelists": [],
                    "parameters": []
                }
            elif name == "PRICELISTS":
                self.in_pricelists = True
            elif name == "PRICELIST" and self.in_pricelists:
                self.in_pricelist = True
                self.current_pricelist = {
                    "title": "",
                    "price_vat": ""
                }
            elif name == "PARAMETERS" and self.in_variant:
                self.in_parameters = True
            elif name == "PARAMETER" and self.in_parameters:
                self.current_parameter = {"name": "", "value": ""}
            elif name == "SET_ITEMS":
                self.in_set_items = True
            elif name == "SET_ITEM" and self.in_set_items:
                self.in_set_item = True
                self.current_set_item = {"code": "", "amount": 1}

    def characters(self, content: str):
        """Handle character data between XML tags"""
        # Accumulate raw content as-is, no stripping
        self.current_data += content

    def endElement(self, name: str):
        """Handle end of XML element"""
        # Use data as-is, no stripping
        data = self.current_data
        if name == "SHOPITEM" and self.in_shopitem:
            # Check if item has variants
            variants = self.current_item.get("variants", [])

            if variants:
                # Has variants - queue each variant as a separate item
                base_item = self.current_item.copy()
                for variant in variants:
                    # Create item from variant, inheriting base item properties
                    variant_item = {
                        "item_id": variant.get("variant_id", ""),
                        "item_code": variant.get("code", ""),  # Variant CODE is the item_code
                        "item_name": base_item.get("item_name", ""),
                        "description": base_item.get("description", ""),
                        "short_description": base_item.get("short_description", ""),
                        "manufacturer": base_item.get("manufacturer", ""),
                        "supplier_name": base_item.get("supplier_name", ""),
                        "categories": base_item.get("categories", []),
                        "default_category": base_item.get("default_category", ""),
                        "images": [],  # Will use variant IMAGE_REF
                        "ean": variant.get("ean", ""),
                        "price_vat": variant.get("price_vat", ""),
                        "purchase_price": variant.get("purchase_price", ""),
                        "vat_rate": variant.get("vat", ""),
                        "currency": variant.get("currency", ""),
                        "unit": variant.get("unit", ""),
                        "weight": variant.get("weight", ""),
                        "stock_quantity": variant.get("stock_amount", ""),
                        "pricelists": variant.get("pricelists", []),
                        "parameters": variant.get("parameters", []),
                        "additional_data": base_item.get("additional_data", {}).copy()
                    }

                    # Use variant IMAGE_REF as the image
                    if variant.get("image_ref"):
                        variant_item["images"] = [{"url": variant["image_ref"], "description": ""}]

                    # Queue variant as separate item
                    if self.redis_client and self.queue_name:
                        self.queue_item(variant_item)
                    self.items_queued += 1
            else:
                # No variants - use top-level CODE if available
                if not self.current_item.get("item_code") or not self.current_item.get("item_code").strip():
                    self.current_item["item_code"] = self.current_item.get("item_id", "")

                # Complete item parsed - queue it in Redis
                if self.redis_client and self.queue_name:
                    self.queue_item(self.current_item)
                self.items_queued += 1

            self.current_item = {}
            self.in_shopitem = False
            self.in_categories = False
            self.in_images = False
            self.in_variants = False
            self.in_variant = False

            # Update progress periodically
            self.update_progress()

        elif self.in_shopitem:
            # Handle structure closings
            if name == "CATEGORIES":
                self.in_categories = False
            elif name == "IMAGES":
                self.in_images = False
            elif name == "VARIANTS":
                self.in_variants = False
            elif name == "VARIANT" and self.in_variant:
                # Complete variant - add to variants list
                self.current_item["variants"].append(self.current_variant.copy())
                self.current_variant = {}
                self.in_variant = False
            elif name == "PARAMETERS" and self.in_variant:
                self.in_parameters = False
            elif name == "PARAMETER" and self.in_parameters:
                if self.current_parameter.get("name") or self.current_parameter.get("value"):
                    self.current_variant["parameters"].append(self.current_parameter.copy())
                self.current_parameter = {}
            elif name == "SET_ITEMS":
                self.in_set_items = False
            elif name == "SET_ITEM" and self.in_set_item:
                # Complete set item - add to set_items list
                if self.current_set_item.get("code"):
                    self.current_item["set_items"].append(self.current_set_item.copy())
                self.current_set_item = {}
                self.in_set_item = False
            elif name == "PRICELISTS":
                self.in_pricelists = False
            elif name == "PRICELIST" and self.in_pricelists:
                self.in_pricelist = False
                if self.current_pricelist.get("title") and self.current_pricelist.get("price_vat"):
                    # Add pricelist to variant if inside variant, otherwise to item
                    if self.in_variant:
                        self.current_variant["pricelists"].append(self.current_pricelist.copy())
                    else:
                        self.current_item["pricelists"].append(self.current_pricelist.copy())
                self.current_pricelist = {}
            # Handle TITLE inside PRICELIST
            elif name == "TITLE" and self.in_pricelist:
                self.current_pricelist["title"] = data
            # Handle PRICE_VAT - context-aware (variant vs top-level vs pricelist)
            elif name == "PRICE_VAT":
                # Only set if not empty - avoid overwriting with empty value
                if data:
                    if self.in_pricelist:
                        self.current_pricelist["price_vat"] = data
                    elif self.in_variant:
                        self.current_variant["price_vat"] = data
                    else:
                        self.current_item["price_vat"] = data
            # Handle NAME element - only at top level under SHOPITEM (depth 3: SHOP > SHOPITEM > NAME)
            # Also handle NAME inside PARAMETER
            elif name == "NAME":
                if self.in_parameters and self.current_parameter is not None:
                    self.current_parameter["name"] = data
                elif len(self.element_stack) == 3:
                    self.current_item["item_name"] = data
                    self.current_item["additional_data"]["name"] = data
            # Handle VALUE inside PARAMETER
            elif name == "VALUE" and self.in_parameters:
                self.current_parameter["value"] = data
            # Handle various item fields
            elif name == "PRODUCT":
                self.current_item["product_name"] = data
            elif name == "PRODUCTNAME":
                self.current_item["product_name"] = data
            # Handle DESCRIPTION - only at top level under SHOPITEM (depth 3)
            elif name == "DESCRIPTION" and len(self.element_stack) == 3:
                self.current_item["description"] = data
            elif name == "SHORT_DESCRIPTION":
                self.current_item["short_description"] = data
            elif name == "URL":
                self.current_item["url"] = data
            elif name == "IMGURL":
                self.current_item["image_url"] = data
            elif name == "PRICE":
                # Only set if not empty - avoid overwriting with empty value
                if data:
                    if self.in_variant:
                        self.current_variant["price"] = data
                    else:
                        self.current_item["price"] = data
            elif name == "VAT":
                if self.in_variant:
                    self.current_variant["vat"] = data
                else:
                    self.current_item["vat_rate"] = data
            elif name == "PURCHASE_PRICE":
                # Only set if not empty - avoid overwriting with empty value
                if data:
                    if self.in_variant:
                        self.current_variant["purchase_price"] = data
                    else:
                        self.current_item["purchase_price"] = data
            elif name == "MANUFACTURER":
                self.current_item["manufacturer"] = data
            elif name == "EAN":
                if self.in_variant:
                    self.current_variant["ean"] = data
                else:
                    self.current_item["ean"] = data
            elif name == "CODE":
                if self.in_set_item:
                    self.current_set_item["code"] = data
                elif self.in_variant:
                    self.current_variant["code"] = data
                elif len(self.element_stack) == 3:  # Only top-level CODE
                    self.current_item["item_code"] = data
            elif name == "ITEM_TYPE":
                self.current_item["item_type"] = data.lower() if data else ""
            elif name == "PRODUCTNO":
                self.current_item["product_code"] = data
            elif name == "AVAILABILITY":
                self.current_item["availability"] = data
            elif name == "STOCK_QUANTITY":
                self.current_item["stock_quantity"] = data
            elif name == "AMOUNT":
                # Amount inside SET_ITEM
                if self.in_set_item:
                    try:
                        self.current_set_item["amount"] = int(data) if data else 1
                    except ValueError:
                        self.current_set_item["amount"] = 1
                # Stock amount inside STOCK tag
                elif self.in_variant:
                    self.current_variant["stock_amount"] = data
                else:
                    self.current_item["stock_quantity"] = data
            elif name == "UNIT":
                if self.in_variant:
                    self.current_variant["unit"] = data
                else:
                    self.current_item["unit"] = data
            elif name == "WEIGHT":
                if self.in_variant:
                    self.current_variant["weight"] = data
                else:
                    self.current_item["weight"] = data
            elif name == "CURRENCY":
                if self.in_variant:
                    self.current_variant["currency"] = data
                else:
                    self.current_item["currency"] = data
            elif name == "IMAGE_REF" and self.in_variant:
                self.current_variant["image_ref"] = data
            # Handle CATEGORY inside CATEGORIES
            elif name == "CATEGORY" and self.in_categories:
                self.current_category["category_name"] = data
                self.current_item["categories"].append(self.current_category.copy())
                self.current_category = {}
            # Handle DEFAULT_CATEGORY - only inside CATEGORIES (depth 4: SHOP > SHOPITEM > CATEGORIES > DEFAULT_CATEGORY)
            elif name == "DEFAULT_CATEGORY" and self.in_categories and len(self.element_stack) == 4:
                self.current_item["default_category"] = data
                self.current_item["additional_data"]["default_category"] = data
            # Legacy CATEGORYTEXT support
            elif name == "CATEGORYTEXT":
                self.current_category["category_name"] = data
                self.current_item["categories"].append(self.current_category.copy())
                self.current_category = {}
            # Handle IMAGE inside IMAGES - URL is the content
            elif name == "IMAGE" and self.in_images:
                if data:
                    self.current_image["url"] = data
                self.current_item["images"].append(self.current_image.copy())
                self.current_image = {}
            # Legacy IMAGE support (with url attribute)
            elif name == "IMAGE" and not self.in_images:
                if data:
                    self.current_image["description"] = data
                self.current_item["images"].append(self.current_image.copy())
                self.current_image = {}
            elif name == "SUPPLIER":
                # Single supplier value (not array)
                self.current_item["supplier_name"] = data
            else:
                # Store any other data in additional_data
                if data and not self.in_variant:
                    self.current_item["additional_data"][name.lower()] = data

        # Pop from stack
        if self.element_stack:
            self.element_stack.pop()

        # Reset current data
        self.current_data = ""
        self.current_element = self.element_stack[-1] if self.element_stack else None

    def queue_item(self, item_data: Dict[str, Any]):
        """Queue item for background processing"""
        try:
            if not REDIS_AVAILABLE:
                return

            # Generate unique ID for this queue entry
            queue_id = str(uuid.uuid4())

            # Add metadata
            queue_entry = {
                "id": queue_id,
                "timestamp": datetime.now().isoformat(),
                "item_data": item_data,
                "status": "queued"
            }

            # Push to Redis queue
            self.redis_client.lpush(
                self.queue_name,
                json.dumps(queue_entry, ensure_ascii=False)
            )

        except Exception as e:
            frappe.log_error(f"Failed to queue item: {str(e)}")
            self.parse_errors += 1

    def update_progress(self):
        """Update progress in realtime for UI feedback"""
        now = datetime.now()
        if (now - self.last_progress_update).seconds >= 2:  # Update every 2 seconds
            frappe.publish_realtime(
                "sax_parse_progress",
                {
                    "items_queued": self.items_queued,
                    "items_processed": self.items_processed,
                    "parse_errors": self.parse_errors,
                    "message": f"Parsed {self.items_queued} items, processed {self.items_processed}"
                },
                user=frappe.session.user
            )
            # Also update Redis progress
            self._update_redis_progress("parsing")
            self.last_progress_update = now

    def finalize_parsing(self):
        """Called when SAX parsing is complete - update total in Redis"""
        if self.redis_client:
            self._update_redis_progress("queued")
            frappe.logger().info(f"SAX parsing complete: {self.items_queued} items queued")

    def get_stats(self) -> Dict[str, int]:
        """Get parsing statistics"""
        return {
            "items_queued": self.items_queued,
            "items_processed": self.items_processed,
            "parse_errors": self.parse_errors
        }


def get_redis_client():
    """Get Redis client connection"""
    if not REDIS_AVAILABLE:
        frappe.throw("Redis package not installed. Install with: pip install redis")

    try:
        # Get Redis configuration from Frappe
        redis_config = frappe.conf.get("redis_queue") or frappe.conf.get("redis_cache")

        if isinstance(redis_config, str):
            return redis.from_url(redis_config)
        elif isinstance(redis_config, dict):
            return redis.Redis(**redis_config)
        else:
            return redis.Redis(host='localhost', port=6379, db=0)

    except Exception as e:
        frappe.log_error(f"Redis connection error: {str(e)}")
        return redis.Redis(host='localhost', port=6379, db=0)


# Progress tracking key
IMPORT_PROGRESS_KEY = "xml_import_progress"


def _update_import_progress(redis_client, phase: str, total: int, processed: int,
                           errors: int, queue_name: str, completed: bool = False):
    """Update import progress in Redis"""
    try:
        # Get existing data to preserve start_time
        existing = redis_client.get(IMPORT_PROGRESS_KEY)
        start_time = datetime.now().isoformat()
        if existing:
            existing_data = json.loads(existing)
            start_time = existing_data.get("start_time", start_time)

        progress_data = {
            "phase": phase,
            "queue_name": queue_name,
            "total_items": total,
            "processed": processed,
            "errors": errors,
            "start_time": start_time,
            "last_update": datetime.now().isoformat(),
            "status": "complete" if completed else "running"
        }
        redis_client.set(IMPORT_PROGRESS_KEY, json.dumps(progress_data))
        # Set expiry - shorter if completed
        redis_client.expire(IMPORT_PROGRESS_KEY, 300 if completed else 3600)
    except Exception as e:
        frappe.log_error(f"Failed to update import progress: {str(e)}")


@frappe.whitelist()
def get_import_progress() -> Dict[str, Any]:
    """
    Get current import progress from Redis

    Returns:
        Dict with progress information or empty status if no import running
    """
    try:
        if not REDIS_AVAILABLE:
            return {"status": "no_redis", "message": "Redis not available"}

        redis_client = get_redis_client()
        progress_data = redis_client.get(IMPORT_PROGRESS_KEY)

        if not progress_data:
            return {
                "status": "idle",
                "message": "No import in progress",
                "is_running": False
            }

        data = json.loads(progress_data)

        # Calculate percentage
        total = data.get("total_items", 0)
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
            "errors": data.get("errors", 0),
            "percentage": percentage,
            "elapsed_time": elapsed_str,
            "remaining_time": remaining_str,
            "queue_name": data.get("queue_name", ""),
            "last_update": data.get("last_update", ""),
            "message": f"Processing {processed}/{total} items ({percentage}%)"
        }

    except Exception as e:
        frappe.log_error(f"Error getting import progress: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "is_running": False
        }


@frappe.whitelist()
def cancel_import() -> Dict[str, Any]:
    """
    Cancel the current import and clean up Redis

    Returns:
        Dict with cancellation result
    """
    try:
        if not REDIS_AVAILABLE:
            return {"success": False, "message": "Redis not available"}

        redis_client = get_redis_client()

        # Get current progress to find queue name
        progress_data = redis_client.get(IMPORT_PROGRESS_KEY)
        queue_name = None

        if progress_data:
            data = json.loads(progress_data)
            queue_name = data.get("queue_name")

            # Mark as cancelled
            data["status"] = "cancelled"
            data["last_update"] = datetime.now().isoformat()
            redis_client.set(IMPORT_PROGRESS_KEY, json.dumps(data))
            redis_client.expire(IMPORT_PROGRESS_KEY, 60)  # Keep for 1 minute only

        # Clean up the queue if it exists
        items_removed = 0
        if queue_name:
            items_removed = redis_client.llen(queue_name)
            redis_client.delete(queue_name)
            frappe.logger().info(f"Cancelled import: removed {items_removed} items from queue {queue_name}")

        # Also clean up any other xml_import queues
        all_queues = redis_client.keys("xml_import_*")
        for queue in all_queues:
            queue_key = queue.decode() if isinstance(queue, bytes) else queue
            if queue_key != IMPORT_PROGRESS_KEY:
                redis_client.delete(queue_key)
                frappe.logger().info(f"Cleaned up queue: {queue_key}")

        # Delete progress key
        redis_client.delete(IMPORT_PROGRESS_KEY)

        return {
            "success": True,
            "message": f"Import cancelled. Removed {items_removed} pending items.",
            "items_removed": items_removed,
            "queue_name": queue_name
        }

    except Exception as e:
        frappe.log_error(f"Error cancelling import: {str(e)}")
        return {
            "success": False,
            "message": str(e)
        }


@frappe.whitelist()
def import_xml_items_sax(xml_source: str, company: str = None,
                        use_queue: bool = True, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Import items from XML feed using SAX parser with Redis queue

    This function always uses Redis queue for memory-optimized background processing.
    The use_queue parameter is kept for backward compatibility.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        use_queue: Kept for backward compatibility (always True)
        config: Import configuration options (download_images, create_item_groups, etc.)

    Returns:
        Dict with import results
    """
    try:
        result = _import_xml_items_sax_internal(xml_source, company, use_queue, config)

        # Create import log when running as background job
        if frappe.local.job:
            _create_import_log_from_result(xml_source, result, "Items")
            _update_config_status(xml_source, result)

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

        # Log error when running as background job
        if frappe.local.job:
            _create_import_log_from_result(xml_source, error_result, "Items")
            _update_config_status(xml_source, error_result)

        raise


def _import_xml_items_sax_internal(xml_source: str, company: str = None,
                                  use_queue: bool = True, config: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Import items from XML feed using SAX parser with Redis queue

    This function always uses Redis queue for memory-optimized background processing.
    The use_queue parameter is kept for backward compatibility.

    Args:
        xml_source: URL or file path to XML feed
        company: Company name (optional)
        use_queue: Kept for backward compatibility (always True)
        config: Import configuration options (download_images, create_item_groups, etc.)

    Returns:
        Dict with import results
    """
    try:
        frappe.logger().info(f"Starting SAX-based XML import from: {xml_source}")
        if config:
            frappe.logger().info(f"Import config: {config}")

        # Create importer to use existing functionality
        importer = XMLItemImporter(xml_source, company, config)

        # Fetch XML content
        xml_content = importer.fetch_xml_content()

        # Always use Redis for memory efficiency
        if not REDIS_AVAILABLE:
            frappe.throw("Redis package not installed. Install with: pip install redis")

        redis_client = get_redis_client()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        queue_name = f"xml_import_items_{timestamp}"

        # Phase 1: Parse XML with SAX and queue items
        frappe.logger().info("Phase 1: Parsing XML and queueing items...")
        handler = SAXItemHandler(redis_client, queue_name)
        parser = xml.sax.make_parser()
        parser.setContentHandler(handler)

        from io import StringIO
        parser.parse(StringIO(xml_content))

        # Finalize parsing phase - update total count in Redis
        handler.finalize_parsing()

        # Get statistics from parsing phase
        stats = handler.get_stats()
        total_items = stats['items_queued']
        frappe.logger().info(f"Queued {total_items} items for processing")

        # Phase 2: Process queued items
        frappe.logger().info("Phase 2: Processing queued items...")
        processed_count = 0
        error_count = 0
        errors = []

        # Update progress to show processing started
        _update_import_progress(redis_client, "processing", total_items, 0, 0, queue_name)

        while True:
            # Get item from queue
            item_data = redis_client.rpop(queue_name)
            if not item_data:
                break

            try:
                queue_entry = json.loads(item_data.decode('utf-8'))

                # Extract the actual item data from the queue entry
                item_dict = queue_entry.get("item_data", {})

                # Convert SAX format to legacy format for processing
                legacy_item = convert_sax_to_legacy_format(item_dict)

                # Process the item using existing methods
                success = importer.create_or_update_item(legacy_item)

                if success:
                    processed_count += 1
                else:
                    error_count += 1
                    item_identifier = legacy_item.get('item_code', item_dict.get('item_id', 'Unknown'))
                    errors.append(f"Failed to process item: {item_identifier}")

                    # Log more details for debugging
                    frappe.logger().warning(f"Failed to process item {item_identifier}. Item data keys: {list(item_dict.keys())}")

                # Progress update every 100 items
                total_processed = processed_count + error_count
                if total_processed % 100 == 0:
                    # Update Redis progress
                    _update_import_progress(redis_client, "processing", total_items, total_processed, error_count, queue_name)

                    frappe.publish_realtime(
                        "sax_import_progress",
                        {
                            "phase": "processing",
                            "total": total_items,
                            "processed": total_processed,
                            "imported": importer.imported_count,
                            "updated": importer.updated_count,
                            "errors": error_count
                        },
                        user=frappe.session.user
                    )

            except Exception as e:
                error_count += 1
                error_msg = f"Error processing item {processed_count + error_count}: {str(e)}"
                errors.append(error_msg)
                frappe.log_error(error_msg)

        # Clean up queue
        redis_client.delete(queue_name)

        # Final counts from the importer instance
        imported_count = importer.imported_count
        updated_count = importer.updated_count

        # Mark progress as complete
        _update_import_progress(redis_client, "complete", total_items, processed_count + error_count, error_count, queue_name, completed=True)

        # Send completion message
        frappe.publish_realtime(
            "sax_import_progress",
            {
                "phase": "complete",
                "message": "SAX import completed successfully",
                "completed": True,
                "total": total_items,
                "items_processed": processed_count,
                "errors": error_count
            },
            user=frappe.session.user
        )

        # Return summary
        summary = {
            "success": True,
            "imported": imported_count,
            "updated": updated_count,
            "errors": error_count,
            "error_messages": errors[:10],  # First 10 errors
            "total_processed": processed_count,
            "sax_stats": stats,
            "method": "sax_with_queue",
            "queue_name": queue_name
        }

        frappe.logger().info(f"SAX import completed: {summary}")
        return summary

    except Exception as e:
        error_msg = f"SAX XML import failed: {str(e)}"
        frappe.log_error(error_msg)
        return {
            "success": False,
            "error": error_msg,
            "imported": 0,
            "updated": 0,
            "errors": 1
        }


def convert_sax_to_legacy_format(sax_item_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert SAX-parsed item data to legacy format.
    This now passes through all data and lets normalize_item_data handle the conversion.
    """
    # Pass through all original data - normalize_item_data will handle field mapping
    converted = sax_item_data.copy()

    # Ensure we capture item_name from NAME tag (stored in additional_data)
    additional_data = sax_item_data.get("additional_data", {})

    # Map NAME from additional_data if item_name not set
    if additional_data.get("name") and not converted.get("item_name"):
        converted["item_name"] = additional_data["name"]

    # Map DEFAULT_CATEGORY from additional_data if not set
    if additional_data.get("default_category") and not converted.get("default_category"):
        converted["default_category"] = additional_data["default_category"]

    # Debug logging
    item_id = sax_item_data.get('item_code') or sax_item_data.get('item_id') or 'Unknown'
    frappe.logger().debug(f"Convert SAX item {item_id}: item_name={converted.get('item_name')}, default_category={converted.get('default_category')}")

    return converted


def _create_import_log_from_result(xml_source: str, result: Dict[str, Any], import_type: str):
    """Create import log entry from result"""
    try:
        if import_type == "Items":
            from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_item_import_log
            create_item_import_log(
                xml_source=xml_source,
                status="Success" if result.get("success") else "Failed",
                imported=result.get("imported", 0),
                updated=result.get("updated", 0),
                errors=result.get("errors", 0),
                error_details="\n".join(result.get("error_messages", [])),
                summary=result
            )
    except Exception as e:
        frappe.log_error(f"Failed to create import log: {str(e)}")


def _update_config_status(xml_source: str, result: Dict[str, Any]):
    """Update configuration status based on result"""
    try:
        # Find configuration by XML feed URL
        configs = frappe.get_all(
            "XML Import Configuration",
            filters={"xml_feed_url": xml_source},
            fields=["name"]
        )

        for config in configs:
            config_doc = frappe.get_doc("XML Import Configuration", config.name)
            config_doc.db_set("last_import", frappe.utils.now())
            config_doc.db_set("last_import_status", "Success" if result.get("success") else "Failed")

    except Exception as e:
        frappe.log_error(f"Failed to update config status: {str(e)}")


@frappe.whitelist()
def test_sax_memory_usage(xml_source: str, company: str = None) -> Dict[str, Any]:
    """
    Compare memory usage between DOM and SAX parsing methods

    Args:
        xml_source: URL or file path to XML feed
        company: Company name

    Returns:
        Dict with memory comparison results
    """
    results = {"dom_method": {}, "sax_method": {}, "comparison": {}}

    try:
        process = psutil.Process(os.getpid())

        # Test DOM method
        frappe.logger().info("Testing DOM memory usage...")
        gc.collect()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_time = time.time()

        dom_result = import_xml_items(xml_source, company)

        dom_time = time.time() - start_time
        dom_memory = process.memory_info().rss / 1024 / 1024 - start_memory

        results["dom_method"] = {
            "processing_time": dom_time,
            "memory_usage_mb": dom_memory,
            "success": dom_result.get("success", False),
            "items_processed": dom_result.get("imported", 0)
        }

        # Clean up and test SAX method
        gc.collect()
        time.sleep(1)

        frappe.logger().info("Testing SAX memory usage...")
        start_memory = process.memory_info().rss / 1024 / 1024
        start_time = time.time()

        sax_result = import_xml_items_sax(xml_source, company, use_queue=False)

        sax_time = time.time() - start_time
        sax_memory = process.memory_info().rss / 1024 / 1024 - start_memory

        results["sax_method"] = {
            "processing_time": sax_time,
            "memory_usage_mb": sax_memory,
            "success": sax_result.get("success", False),
            "items_processed": sax_result.get("imported", 0)
        }

        # Calculate improvement
        if dom_memory > 0:
            memory_improvement = ((dom_memory - sax_memory) / dom_memory) * 100
            time_change = ((dom_time - sax_time) / dom_time) * 100 if dom_time > 0 else 0

            results["comparison"] = {
                "memory_savings_mb": dom_memory - sax_memory,
                "memory_improvement_percent": memory_improvement,
                "time_change_percent": time_change,
                "recommendation": "SAX parser" if memory_improvement > 10 else "DOM parser"
            }

        frappe.logger().info(f"Memory comparison completed: {results['comparison']}")

    except Exception as e:
        results["error"] = str(e)
        frappe.log_error(f"Memory test failed: {str(e)}")

    return results
