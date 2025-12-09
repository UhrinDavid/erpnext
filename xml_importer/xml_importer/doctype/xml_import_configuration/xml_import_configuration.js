// Copyright (c) 2025, David and contributors
// For license information, please see license.txt

frappe.ui.form.on('XML Import Configuration', {
	refresh: function(frm) {
		// Add custom button for manual import
		if (frm.doc.enabled && frm.doc.xml_feed_url && !frm.is_new()) {
			frm.add_custom_button(__('▶️ Trigger Manual Import'), function() {
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

		// Add button to check import progress
		if (!frm.is_new()) {
			frm.add_custom_button(__('📊 Check Import Progress'), function() {
				show_progress_dialog(frm.doc.import_type);
			}, __('Actions'));
		}
	}
});

function show_progress_dialog(import_type) {
	let dialog = new frappe.ui.Dialog({
		title: __('Import Progress'),
		size: 'large',
		fields: [
			{
				fieldtype: 'HTML',
				fieldname: 'progress_html',
				options: '<div class="text-center"><i class="fa fa-spinner fa-spin"></i> Loading...</div>'
			}
		],
		primary_action_label: __('Refresh'),
		primary_action: function() {
			refresh_progress(dialog, import_type);
		},
		secondary_action_label: __('Close'),
		secondary_action: function() {
			if (dialog.auto_refresh_interval) {
				clearInterval(dialog.auto_refresh_interval);
			}
			dialog.hide();
		}
	});

	// Store import_type in dialog for later use
	dialog.import_type = import_type;

	dialog.show();

	// Initial load
	refresh_progress(dialog, import_type);

	// Auto-refresh every 60 seconds if import is running
	dialog.auto_refresh_interval = setInterval(function() {
		if (dialog.$wrapper.is(':visible')) {
			refresh_progress(dialog, import_type, true);
		} else {
			clearInterval(dialog.auto_refresh_interval);
		}
	}, 60000);

	// Clear interval when dialog is closed
	dialog.$wrapper.on('hidden.bs.modal', function() {
		if (dialog.auto_refresh_interval) {
			clearInterval(dialog.auto_refresh_interval);
		}
	});
}

function refresh_progress(dialog, import_type, silent) {
	if (!silent) {
		dialog.fields_dict.progress_html.$wrapper.html(
			'<div class="text-center"><i class="fa fa-spinner fa-spin"></i> Loading...</div>'
		);
	}

	// Use different method based on import_type
	let progress_method = 'xml_importer.xml_importer.item_importer.get_import_progress';
	let cancel_method = 'xml_importer.xml_importer.item_importer.cancel_import';
	let item_label = 'Items';

	if (import_type === 'Orders') {
		progress_method = 'xml_importer.xml_importer.order_importer.get_order_import_progress';
		cancel_method = 'xml_importer.xml_importer.order_importer.cancel_order_import';
		item_label = 'Orders';
	}

	frappe.call({
		method: progress_method,
		callback: function(r) {
			if (!r.exc && r.message) {
				let result = r.message;
				let html = '';

				if (result.status === 'idle') {
					html = `
						<div class="alert alert-info">
							<h5>📭 No Import Running</h5>
							<p>There is no ${item_label.toLowerCase()} import currently in progress.</p>
						</div>
					`;
					// Stop auto-refresh if no import running
					if (dialog.auto_refresh_interval) {
						clearInterval(dialog.auto_refresh_interval);
						dialog.auto_refresh_interval = null;
					}
				} else if (result.status === 'complete') {
					let extraInfo = '';
					if (import_type === 'Orders' && result.skip_reasons) {
						let skipReasonsList = Object.entries(result.skip_reasons)
							.map(([reason, count]) => `<li>${reason}: ${count}</li>`)
							.join('');
						if (skipReasonsList) {
							extraInfo = `<tr><td><strong>Skip Reasons:</strong></td><td><ul style="margin:0;padding-left:20px;">${skipReasonsList}</ul></td></tr>`;
						}
					}
					html = `
						<div class="alert alert-success">
							<h5>✅ Import Complete</h5>
							<table class="table table-bordered">
								<tr><td><strong>Total ${item_label}:</strong></td><td>${result.total_items}</td></tr>
								<tr><td><strong>Processed:</strong></td><td>${result.processed}</td></tr>
								${import_type === 'Orders' ? `<tr><td><strong>Imported:</strong></td><td>${result.imported || 0}</td></tr>` : ''}
								${import_type === 'Orders' ? `<tr><td><strong>Skipped:</strong></td><td>${result.skipped || 0}</td></tr>` : ''}
								<tr><td><strong>Errors:</strong></td><td>${result.errors}</td></tr>
								${extraInfo}
								<tr><td><strong>Elapsed Time:</strong></td><td>${result.elapsed_time}</td></tr>
							</table>
						</div>
					`;
					// Stop auto-refresh when complete
					if (dialog.auto_refresh_interval) {
						clearInterval(dialog.auto_refresh_interval);
						dialog.auto_refresh_interval = null;
					}
				} else if (result.is_running) {
					let progressPercent = result.percentage || 0;
					let progressClass = progressPercent > 75 ? 'bg-success' :
									   (progressPercent > 25 ? 'bg-info' : 'bg-warning');

					let extraInfo = '';
					if (import_type === 'Orders') {
						extraInfo = `
							<tr><td><strong>Imported:</strong></td><td>${result.imported || 0}</td></tr>
							<tr><td><strong>Skipped:</strong></td><td>${result.skipped || 0}</td></tr>
						`;
					}

					html = `
						<div class="alert alert-warning">
							<h5>🔄 Import In Progress</h5>
							<table class="table table-bordered">
								<tr><td><strong>Phase:</strong></td><td>${result.phase}</td></tr>
								<tr><td><strong>Progress:</strong></td><td>${result.processed} / ${result.total_items} (${result.percentage}%)</td></tr>
								${extraInfo}
								<tr><td><strong>Errors:</strong></td><td>${result.errors}</td></tr>
								<tr><td><strong>Elapsed:</strong></td><td>${result.elapsed_time}</td></tr>
								<tr><td><strong>Remaining:</strong></td><td>${result.remaining_time || 'Calculating...'}</td></tr>
							</table>
							<div class="progress" style="height: 25px; margin-top: 10px;">
								<div class="progress-bar ${progressClass}" role="progressbar"
									style="width: ${progressPercent}%; line-height: 25px; font-size: 14px;"
									aria-valuenow="${progressPercent}" aria-valuemin="0" aria-valuemax="100">
									${progressPercent}%
								</div>
							</div>
							<div style="margin-top: 15px; text-align: center;">
								<button class="btn btn-danger btn-sm cancel-import-btn">
									<i class="fa fa-stop"></i> Cancel Import
								</button>
							</div>
							<p style="margin-top: 10px; font-size: 12px; color: #666; text-align: center;">
								<i class="fa fa-refresh fa-spin"></i> Auto-refreshing every 1 minute...
							</p>
						</div>
					`;
				} else if (result.status === 'cancelled') {
					html = `
						<div class="alert alert-danger">
							<h5>🛑 Import Cancelled</h5>
							<p>The import was cancelled by user.</p>
						</div>
					`;
					// Stop auto-refresh
					if (dialog.auto_refresh_interval) {
						clearInterval(dialog.auto_refresh_interval);
						dialog.auto_refresh_interval = null;
					}
				} else {
					html = `
						<div class="alert alert-secondary">
							<h5>Status: ${result.status}</h5>
							<p>${result.message || 'Unknown status'}</p>
						</div>
					`;
				}

				dialog.fields_dict.progress_html.$wrapper.html(html);

				// Attach cancel button handler
				dialog.fields_dict.progress_html.$wrapper.find('.cancel-import-btn').on('click', function() {
					cancel_import(dialog, cancel_method);
				});
			}
		}
	});
}

function cancel_import(dialog, cancel_method) {
	frappe.confirm(
		__('Are you sure you want to cancel the import? This will remove all pending items from the queue.'),
		function() {
			frappe.call({
				method: cancel_method,
				callback: function(r) {
					if (!r.exc && r.message) {
						let result = r.message;
						if (result.success) {
							frappe.show_alert({
								message: result.message,
								indicator: 'orange'
							});
							// Stop auto-refresh
							if (dialog.auto_refresh_interval) {
								clearInterval(dialog.auto_refresh_interval);
								dialog.auto_refresh_interval = null;
							}
							// Refresh the progress display
							refresh_progress(dialog, dialog.import_type);
						} else {
							frappe.msgprint({
								title: __('Error'),
								message: result.message,
								indicator: 'red'
							});
						}
					}
				}
			});
		}
	);
}
