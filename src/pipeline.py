import pandas as pd

from src.report import generate_html_report
from src.stages.source_stage import SourceStage


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

        for stage in self.stages:
            stage.execution_time = 0  # Placeholder for execution time (you can implement timing)
            current_df = stage.execute(current_df)
            self.report.append({
                'stage_name': stage.name,
                'description': stage.description,
                'input_df': stage.input_df,
                'computation_df': stage.computation_df,
                'output_df': stage.output_df,
                'execution_time': stage.execution_time,
            })

        return current_df

    def generate_report(self):
        return generate_html_report(self)
