from xreport.stages.base_stage import BaseStage


class RenameStage(BaseStage):
    def __init__(self, name, description, rename_dict):
        super().__init__(name, description)
        self.rename_dict = rename_dict  # Dictionary for renaming {old_name: new_name}

    def _process_stage(self, df):
        # Create computation_df with all data and new column names formatted
        computation_data = df.copy()
        for old, new in self.rename_dict.items():
            computation_data.rename(columns={old: f"{new} ({old})"}, inplace=True)

        self.computation_df = computation_data

        # Rename the columns in the original DataFrame
        self.output_df = df.rename(columns=self.rename_dict)

        return self.output_df