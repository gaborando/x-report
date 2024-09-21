import pandas as pd

from xreport.stages.base_stage import BaseStage


class SourceStage(BaseStage):
    def __init__(self, name, description, input_df):
        super().__init__(name, description)
        self.input_df = input_df  # Set the input DataFrame
        self.output_df = input_df  # The output is the same as the input
        self.computation_df = pd.DataFrame()  # Empty computation DataFrame

    def _process_stage(self, df):
        return self.output_df  # Return the output DataFrame