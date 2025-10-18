import frappe

def execute():
    """
    Migrate existing XML Order Import Log records to unified XML Import Log
    """
    if not frappe.db.exists("DocType", "XML Order Import Log"):
        return

    try:
        # Get all XML Order Import Log records
        order_logs = frappe.get_all("XML Order Import Log",
            fields=["name", "import_date", "xml_source", "status", "orders_imported",
                   "orders_updated", "errors", "error_details", "summary"])

        for log in order_logs:
            # Check if this record was already migrated
            existing = frappe.db.exists("XML Import Log", {
                "xml_source": log.xml_source,
                "import_datetime": log.import_date,
                "import_type": "Orders"
            })

            if not existing:
                # Create corresponding XML Import Log entry
                try:
                    new_log = frappe.get_doc({
                        "doctype": "XML Import Log",
                        "import_datetime": log.import_date,
                        "import_type": "Orders",
                        "xml_source": log.xml_source,
                        "status": log.status,
                        "records_imported": log.orders_imported or 0,
                        "records_updated": log.orders_updated or 0,
                        "error_count": log.errors or 0,
                        "total_processed": (log.orders_imported or 0) + (log.orders_updated or 0),
                        "error_message": log.error_details or "",
                        "summary": log.summary or "{}"
                    })
                    new_log.insert(ignore_permissions=True)

                except Exception as e:
                    print(f"Warning: Could not migrate order log {log.name}: {str(e)}")

        if order_logs:
            print(f"✅ Migrated {len(order_logs)} XML Order Import Log records to unified XML Import Log")

    except Exception as e:
        print(f"Warning: Error during XML Order Import Log migration: {str(e)}")

    frappe.db.commit()
