// Copyright (c) 2024, Herbatica and contributors
// For license information, please see license.txt

frappe.ui.form.on('XML Import Settings', {
	refresh: function(frm) {
		// Add Test Connection button
		frm.add_custom_button(__('Test Connection'), function() {
			test_xml_connection(frm);
		}, __('Actions'));

		// Add Manual Import button (only if enabled)
		if (frm.doc.enabled && frm.doc.xml_feed_url) {
			frm.add_custom_button(__('Manual Import'), function() {
				trigger_manual_import(frm);
			}, __('Actions'));
		}

		// Add View Import Logs button
		frm.add_custom_button(__('View Import Logs'), function() {
			frappe.set_route("List", "XML Import Log");
		}, __('Actions'));
	},

	enabled: function(frm) {
		// Refresh to show/hide manual import button
		frm.refresh();
	},

	xml_feed_url: function(frm) {
		// Refresh to show/hide manual import button
		frm.refresh();
	}
});

function test_xml_connection(frm) {
	if (!frm.doc.xml_feed_url) {
		frappe.msgprint(__('Please enter XML Feed URL first'));
		return;
	}

	frappe.show_alert({
		message: __('Testing connection...'),
		indicator: 'blue'
	});

	frappe.call({
		method: 'test_connection',
		doc: frm.doc,
		callback: function(r) {
			if (r.message && r.message.success) {
				frappe.show_alert({
					message: r.message.message,
					indicator: 'green'
				});

				frappe.msgprint({
					title: __('Connection Test Successful'),
					message: r.message.message,
					indicator: 'green'
				});
			} else {
				frappe.show_alert({
					message: r.message ? r.message.message : 'Connection test failed',
					indicator: 'red'
				});

				frappe.msgprint({
					title: __('Connection Test Failed'),
					message: r.message ? r.message.message : 'Connection test failed',
					indicator: 'red'
				});
			}
		},
		error: function(err) {
			frappe.show_alert({
				message: __('Connection test failed'),
				indicator: 'red'
			});
		}
	});
}

function trigger_manual_import(frm) {
	frappe.confirm(
		__('Are you sure you want to trigger a manual import?<br><br>This may take some time depending on the feed size.'),
		function() {
			// Show progress indicator
			frappe.show_alert({
				message: __('Starting import...'),
				indicator: 'blue'
			});

			// Disable form during import
			frm.disable_form();

			frappe.call({
				method: 'trigger_manual_import',
				doc: frm.doc,
				callback: function(r) {
					frm.enable_form();

					if (r.message && r.message.success) {
						frappe.show_alert({
							message: __('Import completed successfully'),
							indicator: 'green'
						});

						frappe.msgprint({
							title: __('Import Successful'),
							message: r.message.message,
							indicator: 'green'
						});

						// Refresh to update last import fields
						frm.reload_doc();

					} else {
						frappe.show_alert({
							message: __('Import failed'),
							indicator: 'red'
						});

						frappe.msgprint({
							title: __('Import Failed'),
							message: r.message ? r.message.message : 'Import failed',
							indicator: 'red'
						});
					}
				},
				error: function(err) {
					frm.enable_form();
					frappe.show_alert({
						message: __('Import failed'),
						indicator: 'red'
					});

					frappe.msgprint({
						title: __('Import Error'),
						message: err.message || 'Import failed with error',
						indicator: 'red'
					});
				}
			});
		}
	);
}
