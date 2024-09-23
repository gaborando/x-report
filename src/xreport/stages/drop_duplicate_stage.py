from xreport.stages.base_stage import BaseStage

class DropDuplicateStage(BaseStage):
    """
        class DropDuplicateStage(BaseStage):

    def __init__(self, name, description, subset=None, keep='first'):
        """
    def __init__(self, name, description, subset=None, keep='first'):
        """
        :param name: Name of the instance.
        :param description: A brief description of the instance.
        :param subset: Optional parameter to specify a subset. Defaults to None.
        :param keep: Parameter to specify which items to keep. Defaults to 'first'.
        """
        super().__init__(name, description)
        self.subset = subset
        self.keep = keep

    def execute(self, input_df):
        """
        :param input_df: The input DataFrame to process for duplicate removal.
        :return: A DataFrame with duplicates removed based on specified subset and keep criteria.
        """
        self.input_df = input_df.copy()

        # Identify which rows are kept
        duplicates_mask = self.input_df.duplicated(subset=self.subset, keep=self.keep)
        self.computation_df = self.input_df.copy()

        # Add a row number (#) column and a kept column
        self.computation_df['#'] = range(1, len(self.computation_df) + 1)
        self.computation_df['kept'] = ~duplicates_mask

        # Generate output DataFrame by dropping duplicates
        self.output_df = self.input_df[~duplicates_mask].reset_index(drop=True)

        return self.output_df