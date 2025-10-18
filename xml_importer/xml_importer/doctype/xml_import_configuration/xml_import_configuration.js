// Copyright (c) 2025, David and contributors
// For license information, please see license.txt

frappe.ui.form.on('XML Import Configuration', {
	refresh: function(frm) {
		// Add custom button for manual import
		if (frm.doc.enabled && frm.doc.xml_feed_url && !frm.is_new()) {
			frm.add_custom_button(__('Trigger Manual Import'), function() {
				frappe.confirm(
					__('Are you sure you want to trigger a manual import for {0}?', [frm.doc.name]),
					function() {
						frappe.call({
							method: 'trigger_manual_import',
							doc: frm.doc,
							callback: function(r) {
								if (!r.exc) {
									frappe.msgprint(__('Manual import has been triggered successfully.'));
									frm.reload_doc();
								}
							}
						});
					}
				);
			}, __('Actions'));
		}
	}
});
