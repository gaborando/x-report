import pandas as pd

from xreport.stages.base_stage import BaseStage


class SourceStage(BaseStage):
    def __init__(self, stage_id, name, description, input_df):
        """
        :param stage_id: Identifier for the stage
        :param name: Name of the stage
        :param description: Description of the stage
        :param input_df: Input DataFrame for the stage
        """
        super().__init__(stage_id, name, description)
        self.input_df = input_df  # Set the input DataFrame
        self.output_df = input_df  # The output is the same as the input
        self.computation_df = pd.DataFrame()  # Empty computation DataFrame

    def _process_stage(self, df):
        """
        :param df: The input DataFrame that needs to be processed.
        :return: The processed output DataFrame.
        """
        return self.output_df  # Return the output DataFrame