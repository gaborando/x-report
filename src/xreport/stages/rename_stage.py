from xreport.stages.base_stage import BaseStage


class RenameStage(BaseStage):

    def __init__(self, stage_id, name, description, rename_dict):
        """
        :param stage_id: Unique identifier for the stage.
        :param name: Name of the stage.
        :param description: Detailed description of the stage's functionality.
        :param rename_dict: Dictionary mapping old variable names to new ones.
        """
        super().__init__(stage_id, name, description)
        self.rename_dict = rename_dict  # Dictionary for renaming {old_name: new_name}

    def _process_stage(self, df):
        """
        :param df: DataFrame to be processed with column names to be renamed.
        :return: The DataFrame with renamed columns based on the provided rename_dict.
        """
        # Create computation_df with all data and new column names formatted
        computation_data = df.copy()
        for old, new in self.rename_dict.items():
            computation_data.rename(columns={old: f"{new} ({old})"}, inplace=True)

        self.computation_df = computation_data

        # Rename the columns in the original DataFrame
        self.output_df = df.rename(columns=self.rename_dict)

        return self.output_df