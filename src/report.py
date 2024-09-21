from jinja2 import Template


def generate_html_report(pipeline):
    template = Template("""
    <html>
    <head>
        <title>Pipeline Report</title>
        <style>
            table {
                border-collapse: collapse;
                width: 100%;
            }
            table, th, td {
                border: 1px solid black;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
            }
            .clickable {
                cursor: pointer;
                color: blue;
            }
            .details {
                display: none;
            }
        </style>
        <script>
            function toggleDetails(stage_id) {
                var details = document.getElementById(stage_id);
                if (details.style.display === "none") {
                    details.style.display = "table-row";
                } else {
                    details.style.display = "none";
                }
            }
        </script>
    </head>
    <body>
        <h1>{{ pipeline.name }}</h1>
        <p>{{ pipeline.description }}</p>
        <table border="1">
            <tr>
                <th>Stage Name</th>
                <th>Description</th>
                <th>Input Size</th>
                <th>Output Size</th>
                <th>Execution Time</th>
            </tr>
            {% for stage in pipeline.report %}
            <tr class="clickable" onclick="toggleDetails('stage_{{ loop.index }}')">
                <td>{{ stage.stage_name }}</td>
                <td>{{ stage.description }}</td>
                <td>{{ stage.input_df.shape[0] }} rows, {{ stage.input_df.shape[1] }} columns</td>
                <td>{{ stage.output_df.shape[0] }} rows, {{ stage.output_df.shape[1] }} columns</td>
                <td>{{ stage.execution_time }} seconds</td>
            </tr>
            <tr id="stage_{{ loop.index }}" class="details">
                <td colspan="5">
                    <strong>Input DataFrame:</strong><br>
                    {{ stage.input_df.to_html() | safe }}<br>

                    <strong>Computation DataFrame:</strong><br>
                    {{ stage.computation_df.to_html() | safe }}<br>

                    <strong>Output DataFrame:</strong><br>
                    {{ stage.output_df.to_html() | safe }}
                </td>
            </tr>
            {% endfor %}
        </table>
    </body>
    </html>
    """)

    # Render the template with the pipeline data
    return template.render(pipeline=pipeline)
