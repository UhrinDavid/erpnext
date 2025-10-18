#!/usr/bin/env python3
"""
XML Item Importer CLI Utility
Test and debug XML imports from command line
"""

import frappe
import sys
import os

def import_from_url(url, company=None, dry_run=False):
    """Import from XML URL"""
    try:
        frappe.init(site='erpnext.localhost')
        frappe.connect()

        if not company:
            company = frappe.defaults.get_global_default("company")

        from xml_importer.xml_importer.item_importer import XMLItemImporter

        print(f"Starting XML import from: {url}")
        print(f"Company: {company}")
        print(f"Dry run: {dry_run}")
        print("-" * 50)

        importer = XMLItemImporter(url, company)

        if dry_run:
            # Just parse and show info
            xml_content = importer.fetch_xml_content()
            root = importer.parse_xml(xml_content)
            shopitems = root.findall('.//SHOPITEM')

            print(f"XML parsed successfully!")
            print(f"Found {len(shopitems)} items")

            # Show first 3 items
            for i, item in enumerate(shopitems[:3]):
                item_data = importer.parse_shop_item(item)
                print(f"\nItem {i+1}:")
                print(f"  Code: {item_data.get('item_code')}")
                print(f"  Name: {item_data.get('name')}")
                print(f"  Price: {item_data.get('standard_rate')} {item_data.get('currency')}")
                print(f"  Categories: {[c.get('name') for c in item_data.get('categories', [])]}")
        else:
            # Actual import
            result = importer.import_from_xml()

            print("\nImport Results:")
            print(f"  Success: {result.get('success')}")
            print(f"  Items imported: {result.get('imported', 0)}")
            print(f"  Items updated: {result.get('updated', 0)}")
            print(f"  Errors: {result.get('errors', 0)}")

            if result.get('error_messages'):
                print(f"\nFirst few errors:")
                for error in result.get('error_messages', [])[:5]:
                    print(f"  - {error}")

        print("\nCompleted successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        frappe.destroy()

def test_xml_parsing(file_path):
    """Test XML parsing without ERPNext"""
    try:
        frappe.init(site='erpnext.localhost')
        frappe.connect()

        from xml_importer.xml_importer.item_importer import XMLItemImporter

        print(f"Testing XML parsing: {file_path}")
        print("-" * 50)

        importer = XMLItemImporter(file_path)
        xml_content = importer.fetch_xml_content()
        root = importer.parse_xml(xml_content)

        # Find all SHOPITEM elements
        shopitems = root.findall('.//SHOPITEM')

        print(f"Root element: {root.tag}")
        print(f"Found {len(shopitems)} SHOPITEM elements")

        if shopitems:
            print("\nFirst item details:")
            item_data = importer.parse_shop_item(shopitems[0])

            for key, value in item_data.items():
                if isinstance(value, list) and value:
                    print(f"  {key}: {len(value)} items")
                    if key == 'categories':
                        for cat in value[:3]:
                            print(f"    - {cat.get('name', 'Unknown')}")
                    elif key == 'images':
                        for img in value[:2]:
                            print(f"    - {img.get('url', 'No URL')}")
                else:
                    print(f"  {key}: {value}")

        print("\nXML parsing test completed!")

    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        frappe.destroy()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python cli.py import <url> [company] [--dry-run]")
        print("  python cli.py test <file_path>")
        print("")
        print("Examples:")
        print("  python cli.py import https://example.com/feed.xml")
        print("  python cli.py import https://example.com/feed.xml 'My Company' --dry-run")
        print("  python cli.py test /path/to/sample.xml")
        sys.exit(1)

    command = sys.argv[1]

    if command == "import":
        if len(sys.argv) < 3:
            print("URL is required for import command")
            sys.exit(1)

        url = sys.argv[2]
        company = sys.argv[3] if len(sys.argv) > 3 and not sys.argv[3].startswith('--') else None
        dry_run = '--dry-run' in sys.argv

        import_from_url(url, company, dry_run)

    elif command == "test":
        if len(sys.argv) < 3:
            print("File path is required for test command")
            sys.exit(1)

        file_path = sys.argv[2]
        test_xml_parsing(file_path)

    else:
        print(f"Unknown command: {command}")
        print("Available commands: import, test")
        sys.exit(1)
