import frappe

def execute():
    """
    Migrate existing XML Import Settings to new unified XML Import Configuration
    """
    # Check if old XML Import Settings exists
    if frappe.db.exists("DocType", "XML Import Settings"):
        try:
            settings = frappe.get_single("XML Import Settings")

            # Create new XML Import Configuration for Items
            if settings.xml_feed_url:
                existing_config = frappe.db.exists("XML Import Configuration", {
                    "import_type": "Items",
                    "xml_feed_url": settings.xml_feed_url
                })

                if not existing_config:
                    config_doc = frappe.get_doc({
                        "doctype": "XML Import Configuration",
                        "name": "Item Import Configuration",
                        "import_type": "Items",
                        "enabled": settings.enabled,
                        "xml_feed_url": settings.xml_feed_url,
                        "company": settings.company,
                        "import_frequency": settings.import_frequency or "Daily",
                        "check_feed_changes": getattr(settings, "check_feed_changes", 1),
                        "last_import": settings.last_import,
                        "last_import_status": settings.last_import_status,
                        "last_etag": getattr(settings, "last_etag", ""),
                        "last_modified": getattr(settings, "last_modified", ""),
                        "last_content_size": getattr(settings, "last_content_size", 0),
                        "notification_emails": getattr(settings, "notification_emails", ""),
                        "create_item_groups": getattr(settings, "create_item_groups", 1),
                        "create_manufacturers": getattr(settings, "create_manufacturers", 1),
                        "update_stock_levels": getattr(settings, "update_stock_levels", 1),
                        "download_images": getattr(settings, "download_images", 1)
                    })
                    config_doc.insert(ignore_permissions=True)
                    print(f"✅ Created XML Import Configuration for Items from XML Import Settings")

        except Exception as e:
            print(f"Warning: Could not migrate XML Import Settings: {str(e)}")

    # Check if XML Order Import Settings exists
    if frappe.db.exists("DocType", "XML Order Import Settings"):
        try:
            settings = frappe.get_single("XML Order Import Settings")

            # Create new XML Import Configuration for Orders
            if settings.xml_feed_url:
                existing_config = frappe.db.exists("XML Import Configuration", {
                    "import_type": "Orders",
                    "xml_feed_url": settings.xml_feed_url
                })

                if not existing_config:
                    config_doc = frappe.get_doc({
                        "doctype": "XML Import Configuration",
                        "name": "Order Import Configuration",
                        "import_type": "Orders",
                        "enabled": settings.enabled,
                        "xml_feed_url": settings.xml_feed_url,
                        "company": settings.company,
                        "import_frequency": settings.import_frequency or "Hourly",
                        "last_import": settings.last_import,
                        "last_import_status": settings.last_import_status,
                        "notification_emails": getattr(settings, "notification_emails", ""),
                        "create_customers": getattr(settings, "create_customers", 1),
                        "create_placeholder_items": getattr(settings, "create_placeholder_items", 1),
                        "auto_submit_orders": getattr(settings, "auto_submit_orders", 0)
                    })
                    config_doc.insert(ignore_permissions=True)
                    print(f"✅ Created XML Import Configuration for Orders from XML Order Import Settings")

        except Exception as e:
            print(f"Warning: Could not migrate XML Order Import Settings: {str(e)}")

    frappe.db.commit()
