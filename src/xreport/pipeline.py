import csv
import time
from datetime import datetime

import pandas as pd

from xreport.report import generate_html_report
from xreport.stages.source_stage import SourceStage


class DataPipeline:
    def __init__(self, name, description, source_stage: SourceStage):
        self.name = name
        self.description = description
        self.stages = [source_stage]
        self.report = []

    def add_stage(self, stage):
        self.stages.append(stage)

    def run(self):
        current_df = pd.DataFrame()

        for index, stage in enumerate(self.stages):
            start_time = time.time()  # Start timing
            current_df = stage.execute(current_df)
            execution_time = time.time() - start_time  # Calculate execution time
            self.report.append({
                'stage_number': index + 1,  # Stage number (1-based index)
                'stage_type': stage.__class__.__name__,  # Add stage type here
                'stage_name': stage.name,
                'description': stage.description,
                'input_shape': list(stage.input_df.shape),
                'output_shape': list(stage.output_df.shape),
                'execution_time': execution_time,
                'computation_data': stage.computation_df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC),
                'output_data': stage.output_df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC),
            })

        return current_df

    def to_report(self):
        return {
            'name': self.name,
            'description': self.description,
            'pipeline': self.report,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def generate_report(self):
        return generate_html_report(self.to_report())


