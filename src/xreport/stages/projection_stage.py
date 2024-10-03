import pandas as pd

from xreport.stages.base_stage import BaseStage


class ProjectionStage(BaseStage):
    def __init__(self, stage_id, name, description, selected_columns, sort_columns=None, comparator_func=None):
        """
        :param stage_id: The unique identifier for the stage.
        :param name: The name of the stage.
        :param description: A brief description of the stage.
        :param selected_columns: The columns that have been selected for this stage.
        :param sort_columns: Optional. List of columns to sort the DataFrame by.
        :param comparator_func: Optional. Lambda function for custom comparison while sorting.
        """
        super().__init__(stage_id, name, description)
        self.selected_columns = selected_columns
        self.sort_columns = sort_columns
        self.comparator_func = comparator_func

    def _process_stage(self, df):
        """
        :param df: The input DataFrame containing the columns to be processed.
        :return: A DataFrame with the selected columns.
        """
        # Apply sorting if columns are provided
        if self.sort_columns:
            if self.comparator_func:
                # Use custom comparator for sorting
                df = df.sort_values(by=self.sort_columns, key=self.comparator_func)
            else:
                # Default sorting by specified columns
                df = df.sort_values(by=self.sort_columns)

        # Selected and discarded columns
        selected_df = df[self.selected_columns]
        discarded_df = df[[col for col in df.columns if col not in self.selected_columns]]

        # Create computation_df with reset index to avoid NaN rows
        self.computation_df = pd.concat(
            [selected_df.reset_index(drop=True),
             pd.DataFrame({'#': ['#'] * len(selected_df)}).reset_index(drop=True),
             discarded_df.reset_index(drop=True)],
            axis=1
        )

        # Set the output DataFrame to the selected columns
        self.output_df = selected_df
        return selected_df