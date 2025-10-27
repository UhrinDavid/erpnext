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

		// Add button to check stream length
		if (frm.doc.xml_feed_url && !frm.is_new()) {
			frm.add_custom_button(__('🔍 Check Stream Length'), function() {
				frappe.call({
					method: 'check_stream_length',
					doc: frm.doc,
					callback: function(r) {
						if (!r.exc && r.message) {
							let result = r.message;
							console.log('XML Stream Analysis Result:', result); // Debug log
							let message = `
								<h4>XML Stream Analysis</h4>
								<table class="table table-bordered">
									<tr><td><strong>Content Length:</strong></td><td>${result.content_length} bytes (${result.content_size_human})</td></tr>
									<tr><td><strong>XML Valid:</strong></td><td>${result.xml_valid ? 'Yes' : 'No'}</td></tr>
							`;

							if (result.xml_valid) {
								message += `
									<tr><td><strong>Root Element:</strong></td><td>${result.root_tag}</td></tr>
									<tr><td><strong>Elements Found:</strong></td><td>${result.element_count} ${frm.doc.import_type.toLowerCase()}</td></tr>
								`;

								if (result.sample_elements && result.sample_elements.length > 0) {
									message += '<tr><td><strong>Sample Data:</strong></td><td>';
									result.sample_elements.forEach((sample, index) => {
										if (sample.order_id) {
											message += `Order ${sample.order_id}: Status "${sample.status}", ${sample.item_count} items<br>`;
										} else {
											message += `${sample.note}<br>`;
										}
									});
									message += '</td></tr>';
								}
							} else {
								message += `<tr><td><strong>Parse Error:</strong></td><td>${result.parse_error}</td></tr>`;
							}

							message += `
									<tr><td><strong>First Characters:</strong></td><td><code>${result.first_100_chars || result.first_500_chars}</code></td></tr>
							`;

							// If content is very small, show the full XML
							if (result.content_length < 500 && result.full_xml_content) {
								message += `
									<tr><td><strong>Full XML Content:</strong></td><td><pre style="max-height: 300px; overflow-y: auto; background: #f8f9fa; padding: 10px;">${result.full_xml_content}</pre></td></tr>
								`;
							}

							message += `
								</table>
							`;

							// Add troubleshooting suggestions if no elements found
							if (result.xml_valid && result.element_count === 0) {
								message += `
									<div class="alert alert-warning" style="margin-top: 15px;">
										<h5>⚠️ No ${frm.doc.import_type.toLowerCase()} found!</h5>
										<p><strong>Possible issues:</strong></p>
										<ul>
											<li>The XML feed might be empty or returning no data</li>
											<li>Authentication might be required for the URL</li>
											<li>The XML structure might be different than expected</li>
											<li>The feed might return data only at certain times</li>
										</ul>
										<p><strong>Next steps:</strong></p>
										<ul>
											<li>Check if the URL requires authentication headers</li>
											<li>Verify the URL works in a browser</li>
											<li>Contact the feed provider about data availability</li>
										</ul>
									</div>
								`;
							}

							frappe.msgprint({
								title: __('XML Stream Analysis'),
								message: message,
								wide: true
							});
						}
					}
				});
			});
		}

		// Add button to debug XML feed content
		if (frm.doc.xml_feed_url && !frm.is_new()) {
			frm.add_custom_button(__('🐛 Debug XML Feed'), function() {
				frappe.call({
					method: 'debug_xml_feed',
					doc: frm.doc,
					callback: function(r) {
						if (!r.exc && r.message) {
							let result = r.message;
							console.log('Debug XML Feed Result:', result);

							let message = `
								<h4>XML Feed Debug Information</h4>
								<table class="table table-bordered">
									<tr><td><strong>URL:</strong></td><td>${result.url}</td></tr>
							`;

							if (result.error) {
								message += `<tr><td><strong>Error:</strong></td><td class="text-danger">${result.error}</td></tr>`;
							} else {
								message += `
									<tr><td><strong>Status Code:</strong></td><td>${result.status_code}</td></tr>
									<tr><td><strong>Content Length:</strong></td><td>${result.content_length} bytes (stripped: ${result.content_length_stripped} bytes)</td></tr>
									<tr><td><strong>Content Type:</strong></td><td>${result.headers['content-type'] || 'Not specified'}</td></tr>
									<tr><td><strong>Authentication Used:</strong></td><td>${result.auth_used ? 'Yes' : 'No'}</td></tr>
								`;

								if (result.is_mostly_whitespace) {
									message += `<tr><td><strong>Content Analysis:</strong></td><td class="text-warning">⚠️ Content is mostly whitespace</td></tr>`;
								}

								if (result.is_likely_error) {
									message += `<tr><td><strong>Content Analysis:</strong></td><td class="text-danger">❌ Content may contain error messages</td></tr>`;
								}

								message += `
									<tr><td><strong>Raw Content:</strong></td><td><code>${result.raw_content}</code></td></tr>
									<tr><td><strong>Formatted Content:</strong></td><td><pre style="max-height: 400px; overflow-y: auto; background: #f8f9fa; padding: 10px; white-space: pre-wrap;">${result.content}</pre></td></tr>
								`;

								if (result.suggested_actions && result.suggested_actions.length > 0) {
									message += `<tr><td><strong>Suggestions:</strong></td><td>`;
									result.suggested_actions.forEach(action => {
										message += `• ${action}<br>`;
									});
									message += `</td></tr>`;
								}
							}

							message += `</table>`;

							frappe.msgprint({
								title: __('XML Feed Debug'),
								message: message,
								wide: true
							});
						}
					}
				});
			});
		}

		// Add aggressive import check button
		if (frm.doc.xml_feed_url && !frm.is_new()) {
			frm.add_custom_button(__('🎯 Aggressive Import Check'), function() {
				frappe.confirm(
					__('This will check the feed 5 times (50 seconds total) and import immediately if orders are found. Continue?'),
					function() {
						frappe.call({
							method: 'aggressive_import_check',
							doc: frm.doc,
							callback: function(r) {
								if (!r.exc && r.message) {
									let result = r.message;
									console.log('Aggressive Import Result:', result);

									let message = `
										<h4>Aggressive Import Check Results</h4>
										<p><strong>Total Attempts:</strong> ${result.total_attempts}</p>
										<table class="table table-bordered">
											<tr><th>Attempt</th><th>Time</th><th>Content Size</th><th>Orders Found</th><th>Status</th></tr>
									`;

									result.results.forEach(attempt => {
										let status = "No Data";
										if (attempt.error) {
											status = `❌ Error: ${attempt.error}`;
										} else if (attempt.has_orders) {
											status = `🎉 ${attempt.order_count} orders found!`;
											if (attempt.import_triggered) {
												status += " Import triggered!";
											}
										} else if (attempt.content_length > 100) {
											status = "📭 Empty orders";
										}

										message += `
											<tr>
												<td>${attempt.attempt}</td>
												<td>${attempt.timestamp}</td>
												<td>${attempt.content_length || 'N/A'} bytes</td>
												<td>${attempt.order_count || 0}</td>
												<td>${status}</td>
											</tr>
										`;
									});

									message += `</table>`;

									// Check if any import was triggered
									let importTriggered = result.results.some(r => r.import_triggered);
									if (importTriggered) {
										message += `
											<div class="alert alert-success">
												<h5>🎉 Import was triggered!</h5>
												<p>Orders were found and import was automatically started. Check the Import Log for results.</p>
											</div>
										`;
									} else {
										message += `
											<div class="alert alert-info">
												<h5>📭 No orders found</h5>
												<p>The feed was checked multiple times but no orders were detected. Try again later when new orders are placed.</p>
											</div>
										`;
									}

									frappe.msgprint({
										title: __('Aggressive Import Check Results'),
										message: message,
										wide: true
									});

									if (importTriggered) {
										frm.reload_doc();
									}
								}
							}
						});
					}
				);
			});
		}

		// Add debug import from pasted content button
		if (!frm.is_new()) {
			frm.add_custom_button(__('📋 Debug Import from Pasted XML'), function() {
				// Create a dialog with textarea for XML content
				let dialog = new frappe.ui.Dialog({
					title: __('Import from Pasted XML Content'),
					fields: [
						{
							fieldtype: 'Small Text',
							fieldname: 'xml_content',
							label: __('XML Content'),
							reqd: 1,
							description: __('Paste your XML content here for debugging import process')
						}
					],
					primary_action_label: __('Import XML'),
					primary_action: function() {
						let values = dialog.get_values();
						if (values.xml_content) {
							frappe.call({
								method: 'import_from_pasted_content',
								doc: frm.doc,
								args: {
									xml_content: values.xml_content
								},
								callback: function(r) {
									if (!r.exc && r.message) {
										let result = r.message;
										console.log('Pasted XML Import Result:', result);

										let message = `
											<h4>Pasted XML Import Results</h4>
											<table class="table table-bordered">
												<tr><td><strong>Import Type:</strong></td><td>${frm.doc.import_type}</td></tr>
												<tr><td><strong>Content Length:</strong></td><td>${result.content_length} bytes</td></tr>
												<tr><td><strong>XML Valid:</strong></td><td>${result.xml_valid ? '✅ Yes' : '❌ No'}</td></tr>
										`;

										if (result.xml_valid) {
											message += `
												<tr><td><strong>Root Element:</strong></td><td>${result.root_tag}</td></tr>
												<tr><td><strong>Elements Found:</strong></td><td>${result.element_count} ${frm.doc.import_type.toLowerCase()}</td></tr>
												<tr><td><strong>Import Status:</strong></td><td>${result.import_status}</td></tr>
											`;

											if (result.processed_items && result.processed_items.length > 0) {
												message += `<tr><td><strong>Processed Items:</strong></td><td>`;
												result.processed_items.forEach(item => {
													message += `• ${item}<br>`;
												});
												message += `</td></tr>`;
											}

											if (result.errors && result.errors.length > 0) {
												message += `<tr><td><strong>Errors:</strong></td><td class="text-danger">`;
												result.errors.forEach(error => {
													message += `• ${error}<br>`;
												});
												message += `</td></tr>`;
											}

											// Add debug information if available
											if (result.debug_info) {
												message += `<tr><td><strong>XML Structure:</strong></td><td>`;
												message += `<strong>Root:</strong> ${result.debug_info.root_tag}<br>`;
												if (result.debug_info.direct_children && result.debug_info.direct_children.length > 0) {
													message += `<strong>Direct Children:</strong> ${result.debug_info.direct_children.join(', ')}<br>`;
												}
												message += `<strong>Total Children:</strong> ${result.debug_info.total_children}<br>`;
												if (result.debug_info.all_unique_tags && result.debug_info.all_unique_tags.length > 0) {
													message += `<strong>All Tags Found:</strong> ${result.debug_info.all_unique_tags.join(', ')}<br>`;
												}
												message += `</td></tr>`;
											}
										} else {
											message += `<tr><td><strong>Parse Error:</strong></td><td class="text-danger">${result.parse_error}</td></tr>`;
										}

										message += `</table>`;

										frappe.msgprint({
											title: __('Pasted XML Import Results'),
											message: message,
											wide: true
										});

										if (result.import_status === 'Success') {
											frm.reload_doc();
										}
									}
								}
							});
							dialog.hide();
						}
					}
				});
				dialog.show();
			}, __('Debug'));
		}
	}
});
