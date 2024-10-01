import pandas as pd
from .base_stage import BaseStage

class AddColumnStage(BaseStage):
    def __init__(self, stage_id, name, description, new_columns_map):
        """
        Args:
            stage_id: id of the stage.
            name: Name of the stage.
            description: Description of the stage.
            new_columns_map: A dictionary where keys are new column names and values are lambda functions
                             applied to each row to compute the new column's value.
        """
        super().__init__(stage_id, name, description)
        self.new_columns_map = new_columns_map

    def execute(self, input_df):
        self.input_df = input_df.copy()

        # Create computation DataFrame that contains all original columns
        self.computation_df = self.input_df.copy()
        self.computation_df['#'] = range(1, len(self.input_df) + 1)  # Row numbers

        # Iterate through the new columns map to compute new columns
        for new_column_name, lambda_func in self.new_columns_map.items():
            self.computation_df[new_column_name] = self.input_df.apply(lambda_func, axis=1)  # Compute the new column

        # Add the computed columns to the output DataFrame
        self.output_df = self.input_df.copy()
        for new_column_name in self.new_columns_map.keys():
            self.output_df[new_column_name] = self.computation_df[new_column_name]  # Include the new columns

        return self.output_df
