import pandas as pd
from src.stages.base_stage import BaseStage

class ProjectionStage(BaseStage):
    def __init__(self, name, description, selected_columns):
        super().__init__(name, description)
        self.selected_columns = selected_columns

    def _process_stage(self, df):
        # Selected and discarded columns
        selected_df = df[self.selected_columns]
        discarded_df = df[[col for col in df.columns if col not in self.selected_columns]]

        # Create computation_df
        # Use pd.concat but reset the index to avoid NaN rows
        self.computation_df = pd.concat(
            [selected_df.reset_index(drop=True),
             pd.DataFrame({'#': ['#'] * len(selected_df)}).reset_index(drop=True),
             discarded_df.reset_index(drop=True)],
            axis=1
        )

        self.output_df = selected_df
        return selected_df