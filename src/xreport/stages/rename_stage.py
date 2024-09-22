from xreport.stages.base_stage import BaseStage


class RenameStage(BaseStage):
    """

        RenameStage is a stage in a data processing pipeline responsible for renaming DataFrame columns.

        :param name: Name of the stage
        :type name: str
        :param description: Description of the stage
        :type description: str
        :param rename_dict: Dictionary mapping old column names to new column names
        :type rename_dict: dict

        _process_stage performs the renaming of DataFrame columns.
        It creates a copy of the DataFrame with new column names formatted as "new_name (old_name)".
        The original DataFrame's columns are renamed based on the rename_dict.

        :param df: The input DataFrame to process
        :type df: pandas.DataFrame
        :return: DataFrame with renamed columns
        :rtype: pandas.DataFrame
    """
    def __init__(self, name, description, rename_dict):
        """
        :param name: The name of the object.
        :param description: A brief description of the object.
        :param rename_dict: A dictionary containing keys as old names and values as new names for renaming purposes.
        """
        super().__init__(name, description)
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