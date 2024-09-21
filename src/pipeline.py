from src.report import generate_html_report


class DataPipeline:
    def __init__(self, name, description, input_df):
        self.name = name
        self.description = description
        self.stages = []
        self.input_df = input_df
        self.report = []

    def add_stage(self, stage):
        self.stages.append(stage)

    def run(self):
        current_df = self.input_df.copy()

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
