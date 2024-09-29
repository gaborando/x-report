import pandas as pd

from xreport.stages.base_stage import BaseStage


class ProjectionStage(BaseStage):
    def __init__(self, stage_id, name, description, selected_columns):
        """
        :param stage_id: The unique identifier for the stage.
        :param name: The name of the stage.
        :param description: A brief description of the stage.
        :param selected_columns: The columns that have been selected for this stage.
        """
        super().__init__(stage_id, name, description)
        self.selected_columns = selected_columns

    def _process_stage(self, df):
        """
        :param df: The input dataframe containing the columns to be processed.
        :return: A dataframe with the selected columns.
        """
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