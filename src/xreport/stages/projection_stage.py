import pandas as pd

from xreport.stages.base_stage import BaseStage


class ProjectionStage(BaseStage):
    """
        ProjectionStage

        A class that represents a projection stage for data processing which selects
        specific columns from a DataFrame.

    Attributes
    ----------
    selected_columns : list
        A list of column names to be selected from the input DataFrame.

    Methods
    -------
    __init__(name, description, selected_columns)
        Initializes the ProjectionStage with a name, description, and selected columns.

    _process_stage(df)
        Processes the input DataFrame by selecting and discarding specified columns,
        and then concatenates the result to create a computation DataFrame.
    """
    def __init__(self, name, description, selected_columns):
        """
        :param name: The name of the dataset or entity.
        :param description: A brief description of the dataset or entity.
        :param selected_columns: A list of column names that are selected for processing or analysis.
        """
        super().__init__(name, description)
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