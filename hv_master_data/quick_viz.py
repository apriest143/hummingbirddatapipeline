### RUN THIS WHEN MAP IS READY: python -m http.server 8000

import os

# HTML template for the map
html_content = """<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Institution Asset Map</title>
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    
    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.css" />
    
    <!-- Leaflet MarkerCluster CSS -->
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
    
    <style>
        body {
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
        }
        #map {
            width: 100%;
            height: 100vh;
        }
        .legend {
            background: white;
            padding: 10px;
            border-radius: 5px;
            box-shadow: 0 0 15px rgba(0,0,0,0.2);
        }
        .legend-item {
            margin: 5px 0;
            display: flex;
            align-items: center;
        }
        .legend-color {
            width: 20px;
            height: 20px;
            border-radius: 50%;
            margin-right: 8px;
            border: 2px solid #666;
        }
        .popup-content {
            max-width: 320px;
        }
        .popup-content h3 {
            margin: 0 0 10px 0;
            color: #333;
        }
        .popup-field {
            margin: 5px 0;
            font-size: 13px;
        }
        .popup-label {
            font-weight: bold;
            color: #555;
        }
        .popup-section {
            margin-top: 10px;
            padding-top: 8px;
            border-top: 1px solid #ddd;
        }
        .popup-section-title {
            font-weight: bold;
            color: #333;
            margin-bottom: 5px;
            font-size: 14px;
        }
        .loading {
            position: fixed;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            font-size: 18px;
            z-index: 9999;
            background: white;
            padding: 30px;
            border-radius: 10px;
            box-shadow: 0 0 20px rgba(0,0,0,0.3);
            text-align: center;
        }
        .error {
            color: red;
            font-weight: bold;
        }
    </style>
</head>
<body>
    <div id="loading" class="loading">
        <div>Loading map libraries...</div>
        <div id="status" style="margin-top: 10px; font-size: 14px; color: #666;"></div>
    </div>
    <div id="map"></div>

    <script src="https://cdn.jsdelivr.net/npm/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"></script>

    <script>
        const statusDiv = document.getElementById('status');
        const loadingDiv = document.getElementById('loading');

        setTimeout(function() {
            statusDiv.textContent = 'Checking libraries...';
            
            if (typeof L === 'undefined') {
                loadingDiv.innerHTML = `
                    <div class="error">⚠️ Map library failed to load</div>
                    <div style="margin-top: 15px; font-size: 14px;">
                        Run with: python -m http.server 8000<br>
                        Then open: http://localhost:8000/institution_map.html
                    </div>
                `;
                return;
            }
            
            if (typeof Papa === 'undefined') {
                loadingDiv.innerHTML = '<div class="error">CSV parser failed to load</div>';
                return;
            }

            statusDiv.textContent = 'Libraries loaded! Initializing map...';
            initializeMap();
        }, 1000);

        function initializeMap() {
            try {
                const institutionColors = {
                    'Private College': '#3388ff',
                    'Wellness/Retreat': '#33cc66',
                    'Tribal Center': '#ff6b6b',
                    'Religious Institution': '#9b59b6'
                };

                const map = L.map('map').setView([39.8283, -98.5795], 4);

                L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                    attribution: '© OpenStreetMap contributors',
                    maxZoom: 19
                }).addTo(map);

                const markers = L.markerClusterGroup({
                    chunkedLoading: true,
                    maxClusterRadius: 50
                });

                function formatCurrency(value) {
                    if (!value || value === '' || value === '#VALUE!' || value === 'N/A') return 'N/A';
                    const num = parseFloat(value);
                    if (isNaN(num)) return 'N/A';
                    return '$' + num.toLocaleString('en-US', {maximumFractionDigits: 0});
                }

                function createPopupContent(row) {
                    return `
                        <div class="popup-content">
                            <h3>${row.institution_name || 'Unknown'}</h3>
                            <div class="popup-field">
                                <span class="popup-label">Type:</span> ${row.institution_type || 'N/A'}
                            </div>
                            <div class="popup-field">
                                <span class="popup-label">Location:</span> ${row.city || 'N/A'}, ${row.state || 'N/A'}
                            </div>
                            <div class="popup-field">
                                <span class="popup-label">EIN:</span> ${row.ein_number || 'N/A'}
                            </div>
                            
                            <div class="popup-section">
                                <div class="popup-section-title">📊 Previous Financials (990)</div>
                                <div class="popup-field">
                                    <span class="popup-label">Revenue:</span> ${formatCurrency(row.previous_revenue)}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Expenses:</span> ${formatCurrency(row.previous_expenses)}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Net Income:</span> ${formatCurrency(row.previous_net_income)}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Assets:</span> ${formatCurrency(row.previous_assets)}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Liabilities:</span> ${formatCurrency(row.previous_liabilities)}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Net Assets:</span> ${formatCurrency(row.previous_net_assets)}
                                </div>
                            </div>
                            
                            <div class="popup-section">
                                <div class="popup-section-title">📈 3-Year Averages</div>
                                <div class="popup-field">
                                    <span class="popup-label">Avg Revenue:</span> ${formatCurrency(row.avg_revenue_3yr)}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Avg Expenses:</span> ${formatCurrency(row.avg_expenses_3yr)}
                                </div>
                            </div>
                            
                            <div class="popup-section">
                                <div class="popup-section-title">🎓 Institution Details</div>
                                <div class="popup-field">
                                    <span class="popup-label">Enrollment:</span> ${row['12_month_enrollment'] || 'N/A'}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Student/Faculty Ratio:</span> ${row.student_to_faculty_ratio || 'N/A'}
                                </div>
                                <div class="popup-field">
                                    <span class="popup-label">Total Acreage:</span> ${row.total_acreage || 'N/A'}
                                </div>
                            </div>
                        </div>
                    `;
                }

                function getMarkerColor(type) {
                    return institutionColors[type] || '#888888';
                }

                statusDiv.textContent = 'Loading CSV data...';
                
                Papa.parse('hv_master_data/data/HummingbirdDataWorking_990_merged.csv', {
                    download: true,
                    header: true,
                    dynamicTyping: false,
                    skipEmptyLines: true,
                    complete: function(results) {
                        loadingDiv.style.display = 'none';
                        
                        let validCount = 0;
                        let invalidCount = 0;

                        results.data.forEach(function(row) {
                            const lat = parseFloat(row.latitude);
                            const lng = parseFloat(row.longitude);

                            if (!isNaN(lat) && !isNaN(lng) && lat !== 0 && lng !== 0) {
                                validCount++;
                                
                                const color = getMarkerColor(row.institution_type);
                                
                                const marker = L.circleMarker([lat, lng], {
                                    radius: 8,
                                    fillColor: color,
                                    color: '#fff',
                                    weight: 2,
                                    opacity: 1,
                                    fillOpacity: 0.8
                                });

                                marker.bindPopup(createPopupContent(row));
                                markers.addLayer(marker);
                            } else {
                                invalidCount++;
                            }
                        });

                        map.addLayer(markers);

                        console.log(`Loaded ${validCount} valid locations`);
                        console.log(`Skipped ${invalidCount} invalid locations`);

                        const legend = L.control({position: 'bottomright'});
                        legend.onAdd = function(map) {
                            const div = L.DomUtil.create('div', 'legend');
                            div.innerHTML = '<h4 style="margin: 0 0 10px 0;">Institution Types</h4>';
                            
                            for (const [type, color] of Object.entries(institutionColors)) {
                                div.innerHTML += `
                                    <div class="legend-item">
                                        <div class="legend-color" style="background-color: ${color};"></div>
                                        <span>${type}</span>
                                    </div>
                                `;
                            }
                            
                            div.innerHTML += `<hr><div style="font-size: 12px; color: #666;">Total Points: ${validCount}</div>`;
                            
                            return div;
                        };
                        legend.addTo(map);
                    },
                    error: function(error) {
                        loadingDiv.innerHTML = '<div class="error">Error loading CSV: ' + error.message + '</div>';
                        console.error('Error parsing CSV:', error);
                    }
                });
            } catch (error) {
                loadingDiv.innerHTML = '<div class="error">Error initializing map: ' + error.message + '</div>';
                console.error('Map initialization error:', error);
            }
        }
    </script>
</body>
</html>"""

# Generate the HTML file
output_file = "institution_map.html"

with open(output_file, 'w', encoding='utf-8') as f:
    f.write(html_content)

print(f"✓ Map generated: {output_file}")
print(f"\nTo view the map:")
print(f"1. Run: python -m http.server 8000")
print(f"2. Open: http://localhost:8000/{output_file}")