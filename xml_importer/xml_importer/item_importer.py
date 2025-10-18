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

    def __init__(self, xml_source: str = None, company: str = None):
        """
        Initialize XML Item Importer

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

        # Initialize required UOMs
        self.ensure_required_uoms()

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

    def parse_shop_item(self, shopitem: ET.Element) -> Dict[str, Any]:
        """Parse SHOPITEM XML element to dictionary with English property names"""
        item_data = {}

        # Basic information
        item_data['external_id'] = shopitem.get('id', '')
        item_data['import_code'] = shopitem.get('import-code', '')
        item_data['item_name'] = self.get_element_text(shopitem, 'NAME')
        item_data['guid'] = self.get_element_text(shopitem, 'GUID')
        item_data['item_code'] = self.get_element_text(shopitem, 'CODE')
        item_data['barcode'] = self.get_element_text(shopitem, 'EAN')

        # Descriptions
        item_data['short_description'] = self.clean_html_content(
            self.get_element_text(shopitem, 'SHORT_DESCRIPTION')
        )
        item_data['long_description'] = self.clean_html_content(
            self.get_element_text(shopitem, 'DESCRIPTION')
        )

        # Supplier and manufacturer
        item_data['manufacturer_name'] = self.get_element_text(shopitem, 'MANUFACTURER')
        item_data['supplier_name'] = self.get_element_text(shopitem, 'SUPPLIER')

        # Pricing information
        item_data['currency_code'] = self.get_element_text(shopitem, 'CURRENCY')
        item_data['selling_price_with_tax'] = flt(self.get_element_text(shopitem, 'PRICE_VAT'))
        item_data['purchase_price'] = flt(self.get_element_text(shopitem, 'PURCHASE_PRICE'))
        item_data['tax_rate'] = flt(self.get_element_text(shopitem, 'VAT'))

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
            existing_item.description = self.clean_html_content(item_data.get('short_description') or item_data.get('long_description'))

            # Set item group (use first category or default)
            item_groups = self.map_categories(item_data.get('product_categories', []))
            if item_groups:
                existing_item.item_group = item_groups[0]
            else:
                existing_item.item_group = "All Item Groups"

            # Set basic properties - use proper UOM
            existing_item.stock_uom = self.get_or_create_uom(item_data.get('unit_of_measure', 'Nos'))
            existing_item.is_stock_item = 1
            existing_item.include_item_in_manufacturing = 0
            existing_item.is_sales_item = 1
            existing_item.is_purchase_item = 1

            # Set manufacturer if exists
            manufacturer = self.clean_name(item_data.get('manufacturer_name', ''))
            if manufacturer:
                if not frappe.db.exists("Manufacturer", manufacturer):
                    try:
                        man_doc = frappe.get_doc({
                            "doctype": "Manufacturer",
                            "short_name": manufacturer
                        })
                        man_doc.insert(ignore_permissions=True)
                    except Exception as e:
                        frappe.log_error(f"Failed to create manufacturer {manufacturer}: {str(e)}")
                        manufacturer = None

                if manufacturer:
                    existing_item.manufacturer = manufacturer

            # Custom fields for XML-specific data
            if hasattr(existing_item, 'xml_external_id'):
                existing_item.xml_external_id = item_data.get('external_id')
            if hasattr(existing_item, 'xml_guid'):
                existing_item.xml_guid = item_data.get('guid')
            if hasattr(existing_item, 'barcode'):
                existing_item.barcode = item_data.get('barcode')
            if hasattr(existing_item, 'weight_per_unit'):
                existing_item.weight_per_unit = item_data.get('weight_kg')
            if hasattr(existing_item, 'xml_last_sync'):
                existing_item.xml_last_sync = now()

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
            return False

    def handle_item_images(self, item_doc: Document, images: List[Dict]) -> None:
        """Handle item image downloads and attachments"""
        if not images:
            return

        try:
            # Use first image as main item image
            main_image = images[0]
            image_url = self.download_image(
                main_image.get('image_url', ''),
                item_doc.item_code,
                main_image.get('image_description', '')
            )

            if image_url:
                item_doc.image = image_url
                item_doc.save(ignore_permissions=True)

        except Exception as e:
            frappe.log_error(f"Failed to handle images for item {item_doc.item_code}: {str(e)}")

    def create_item_price(self, item_doc: Document, item_data: Dict[str, Any]) -> None:
        """Create or update item price"""
        try:
            selling_price = item_data.get('selling_price_with_tax')
            if not selling_price:
                return

            # Check if price exists
            existing_price = frappe.db.get_value("Item Price", {
                "item_code": item_doc.item_code,
                "price_list": "Standard Selling"
            })

            if existing_price:
                price_doc = frappe.get_doc("Item Price", existing_price)
                price_doc.price_list_rate = selling_price
                price_doc.save(ignore_permissions=True)
            else:
                price_doc = frappe.get_doc({
                    "doctype": "Item Price",
                    "item_code": item_doc.item_code,
                    "price_list": "Standard Selling",
                    "price_list_rate": selling_price,
                    "currency": item_data.get('currency_code', 'EUR')
                })
                price_doc.insert(ignore_permissions=True)

        except Exception as e:
            frappe.log_error(f"Failed to create item price for {item_doc.item_code}: {str(e)}")

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

            # Process each item
            for shopitem in shopitems:
                try:
                    item_data = self.parse_shop_item(shopitem)
                    self.create_or_update_item(item_data)
                except Exception as e:
                    self.add_error(f"Error processing SHOPITEM ID {shopitem.get('id', 'Unknown')}: {str(e)}")
                    continue

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
    Scheduled function to import XML items
    """
    # Get XML feed URL from XML Import Settings
    if not frappe.db.exists("DocType", "XML Import Settings"):
        frappe.logger().info("XML Import Settings doctype not found")
        return

    settings = frappe.get_single("XML Import Settings")

    if not settings.enabled or not settings.xml_feed_url:
        frappe.logger().info("XML import not enabled or URL not configured")
        return

    try:
        result = import_xml_items(settings.xml_feed_url, settings.company)

        # Create import log using unified system
        from xml_importer.xml_importer.doctype.xml_import_log.xml_import_log import create_item_import_log

        log_doc = create_item_import_log(
            xml_source=settings.xml_feed_url,
            status="Success" if result.get("success") else "Failed",
            imported=result.get("imported", 0),
            updated=result.get("updated", 0),
            errors=result.get("errors", 0),
            error_details="\n".join(result.get("error_messages", [])),
            summary=result
        )

        # Send notification if configured
        if settings.notification_emails and result.get("success"):
            send_import_notification(result, settings.notification_emails)

    except Exception as e:
        frappe.log_error(f"Scheduled XML import error: {str(e)}")


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
