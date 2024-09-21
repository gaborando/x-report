from jinja2 import Template

def generate_html_report(pipeline):
    template = Template("""
  <!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Data Pipeline Workflow Diagram</title>
    <script type="module" src="https://cdn.jsdelivr.net/npm/@ionic/core/dist/ionic/ionic.esm.js"></script>
    <script nomodule src="https://cdn.jsdelivr.net/npm/@ionic/core/dist/ionic/ionic.js"></script>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@ionic/core/css/ionic.bundle.css"/>
    <script type="module" src="https://cdn.jsdelivr.net/npm/ionicons/dist/ionicons/ionicons.esm.js"></script>
    <script nomodule src="https://cdn.jsdelivr.net/npm/ionicons/dist/ionicons/ionicons.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/PapaParse/5.3.0/papaparse.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js"></script>

    <style>
        ion-modal {
            --height: 100%;
            --width: 100%;
        }
    </style>
</head>
<body>
<ion-app>
    <ion-header>
        <ion-toolbar>
            <ion-title>
                {{pipeline.name}}
            </ion-title>
            <ion-title slot="end">
                {{pipeline.timestamp}}
            </ion-title>
        </ion-toolbar>
    </ion-header>
    <ion-content>

        <ion-grid>
            <ion-row>
                <ion-text>
                    <p style="margin-bottom:0">{{pipeline.description}}</p>
                </ion-text>
            </ion-row>
            <ion-row id="container"></ion-row>
        </ion-grid>
    </ion-content>
</ion-app>

</body>
<script>

    function stage_chip(stage) {
        const map = {
            'SourceStage': {
                name: 'Source',
                icon: "play",
                color: 'primary'
            },
            'FilterStage': {
                name: 'Filter',
                icon: "cut",
                color: 'danger'
            },
            'ProjectionStage': {
                name: 'Projection',
                icon: "contract",
                color: 'success'
            },
            'RenameStage': {
                name: 'Rename',
                icon: "create",
                color: 'secondary'
            },
            'MapStage': {
                name: 'Map',
                icon: "repeat",
                color: 'medium'
            },
            'ExpandStage': {
                name: 'Expand',
                icon: "expand",
                color: 'tertiary'
            },
            'GroupByStage': {
                name: 'GroupBy',
                icon: "grid",
                color: 'warning'
            }
        }

        const data = map[stage.stage_type]
        return `<ion-chip color="${data.color}">
                  <ion-icon name="${data.icon}"></ion-icon>
                  <ion-label>${data.name}</ion-label>
                </ion-chip>`
    }


    function closeModal(id) {
        var modal = document.getElementById(id);
        modal.dismiss(null, 'cancel');
    }

    function generateOutputTableHtml(stage) {
        // Parse the output_data CSV
        const outputData = Papa.parse(stage.output_data, {header: true});

        // Start building the HTML table with a scrollable div
        let tableHtml = `<div style="height: 100%; overflow: auto;">`; // Set your desired max-height here
        tableHtml += `<table style="width: 100%; border-collapse: collapse;">`;

        // Generate table headers
        tableHtml += `<thead><tr>`;
        for (const header of outputData.meta.fields) {
            tableHtml += `<th style="border: 1px solid #ccc; padding: 8px;">${header}</th>`;
        }
        tableHtml += `</tr></thead>`;

        // Generate table rows
        tableHtml += `<tbody>`;
        for (const row of outputData.data) {
            // Check if the row has any non-empty values
            if (Object.values(row).some(value => value !== "")) {
                tableHtml += `<tr>`;
                for (const key in row) {
                    tableHtml += `<td style="border: 1px solid #ccc; padding: 8px;">${row[key]}</td>`;
                }
                tableHtml += `</tr>`;
            }
        }
        tableHtml += `</tbody>`;

        tableHtml += `</table>`;
        tableHtml += `</div>`;

        return tableHtml;
    }


    function generateComputeTableHtml(stage) {
        // Parse the output_data CSV
        const outputData = Papa.parse(stage.computation_data, {header: true});

        // Start building the HTML table with a scrollable div
        let tableHtml = `<div style="height: 100%; overflow: auto;">`; // Set your desired max-height here
        tableHtml += `<table style="width: 100%; border-collapse: collapse;">`;

        // Generate table headers
        tableHtml += `<thead><tr>`;
        for (const header of outputData.meta.fields) {
            tableHtml += `<th style="border: 1px solid #ccc; padding: 8px;">${header}</th>`;
        }
        tableHtml += `</tr></thead>`;

        // Generate table rows
        tableHtml += `<tbody>`;
        for (const row of outputData.data) {
            // Check if the row has any non-empty values
            if (Object.values(row).some(value => value !== "")) {
                if (row['all_conditions'] === 'False' && stage.stage_type === 'FilterStage') {
                    tableHtml += `<tr style="background: #ad000d17">`;
                } else {
                    tableHtml += `<tr>`;
                }
                for (const key in row) {
                    if (key === 'all_conditions' && stage.stage_type === 'FilterStage') {
                        let check = `<ion-chip color="danger">
                  <ion-icon name="close-circle-outline"></ion-icon>
                  <ion-label>False</ion-label>
                </ion-chip>`
                        if (row[key] === 'True') {
                            check = `<ion-chip color="success">
                                      <ion-icon name="checkmark-circle-outline"></ion-icon>
                                      <ion-label>True</ion-label>
                                    </ion-chip>`
                        }
                        tableHtml += `<td style="border: 1px solid #ccc; padding: 8px;">${check}</td>`;
                    } else {
                        tableHtml += `<td style="border: 1px solid #ccc; padding: 8px;">${row[key]}</td>`;
                    }
                }
                tableHtml += `</tr>`;
            }
        }
        tableHtml += `</tbody>`;

        tableHtml += `</table>`;
        tableHtml += `</div>`;

        return tableHtml;
    }
    
    function downloadExcel(stage_number){
        const stage = data.pipeline.find(e => e.stage_number === stage_number);
        const outputData = Papa.parse(stage.output_data, {header: true}).data;
        const computationData = Papa.parse(stage.computation_data, {header: true}).data;
        console.log(outputData)


        // Create a new workbook
        const workbook = XLSX.utils.book_new();

        // Create sheets from data
        const sheet1 = XLSX.utils.json_to_sheet(computationData);
        const sheet2 = XLSX.utils.json_to_sheet(outputData);

        // Append sheets to the workbook
        XLSX.utils.book_append_sheet(workbook, sheet1, "Computed");
        XLSX.utils.book_append_sheet(workbook, sheet2, "Output");

        // Export the workbook to an Excel file
        XLSX.writeFile(workbook, `${data.name}-${stage.stage_number}-${stage.stage_name}.xlsx`);
    }


    const data = {{pipeline}}
    console.log(data)

    document.getElementById("container").innerHTML = data.pipeline.map((stage) =>
        `<ion-col size="6"><ion-card style="margin: 0" id="stage_card_${stage.stage_number}">
<ion-card-content>
<ion-grid>
<ion-row>
<ion-col>
<ion-card-title>${stage.stage_number}&nbsp;-&nbsp;${stage_chip(stage)}<br/>${stage.stage_name}</ion-card-title>
<ion-card-subtitle>${stage.description}</ion-card-subtitle>
</ion-col>

</ion-row>
<ion-row>
<ion-col>
Sizes: <br/>
${(stage.output_shape[0])}&nbsp;rows x ${(stage.output_shape[1])}&nbsp;columns
</ion-col>
<ion-col class="ion-text-end">
Completed in: <br/>
${(stage.execution_time).toFixed(3)}&nbsp;sec
</ion-col>
</ion-row>
</ion-grid>
</ion-card-content>
</ion-card></ion-col>
<ion-modal trigger="stage_card_${stage.stage_number}" id="stage_modal_${stage.stage_number}">
<ion-header>
      <ion-toolbar>
        <ion-title>${stage.stage_number} - ${stage.stage_name} &nbsp;${stage_chip(stage)}</ion-title>
        <ion-buttons slot="end">
          <ion-button onclick="closeModal('stage_modal_${stage.stage_number}')" strong="true">Close</ion-button>
        </ion-buttons>
      </ion-toolbar>
    </ion-header>
    <ion-content class="ion-padding">
    <ion-text>
        <p>${stage.description}</p>
    </ion-text>
   <ion-segment class="ion-margin-bottom" value="${stage.stage_type === 'SourceStage' ? 'output' : 'computed'}" id="stage_modal_segment_${stage.stage_number}">
      <ion-segment-button value="computed">
        <ion-label>Computed</ion-label>
      </ion-segment-button>
      <ion-segment-button value="output">
        <ion-label>Output</ion-label>
      </ion-segment-button>
    </ion-segment>
      <div id="stage_modal_segment_${stage.stage_number}_content_output" style="display: ${stage.stage_type === 'SourceStage' ? 'block' : 'none'};">
            ${generateOutputTableHtml(stage)}
      </div>
      <div id="stage_modal_segment_${stage.stage_number}_content_computed" style="display: ${stage.stage_type !== 'SourceStage' ? 'block' : 'none'};">
            ${generateComputeTableHtml(stage)}
      </div>

    </ion-content>
    <ion-footer>
        <ion-toolbar>
            <ion-buttons slot="end" color="secondary">
                <ion-button onclick="downloadExcel(${stage.stage_number})">Download Excel</ion-button>
            </ion-buttons>
        </ion-toolbar>
    </ion-footer>
</ion-modal>`).join("")

    for (let stage of data.pipeline) {
        document.getElementById(`stage_modal_segment_${stage.stage_number}`).addEventListener('ionChange', (event) => {
            const selectedValue = event.detail.value;
            // Hide all segments initially
            document.getElementById(`stage_modal_segment_${stage.stage_number}_content_computed`).style.display = 'none';
            document.getElementById(`stage_modal_segment_${stage.stage_number}_content_output`).style.display = 'none';
            // Show the selected segment
            document.getElementById(`stage_modal_segment_${stage.stage_number}_content_${selectedValue}`).style.display = 'block';
        });
    }
</script>
</html>

    """)

    # Render the template with the pipeline data
    return template.render(pipeline=pipeline)
