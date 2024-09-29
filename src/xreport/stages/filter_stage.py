import pandas as pd

from xreport.stages.base_stage import BaseStage


class FilterStage(BaseStage):
    def __init__(self, stage_id, name, description, conditions):
        """
        :param stage_id: The unique identifier for the stage.
        :param name: The name of the stage.
        :param description: A description of the stage.
        :param conditions: A dictionary containing condition names as keys and lambda functions as values.
        """
        super().__init__(stage_id, name, description)
        self.conditions = conditions  # Should be a dictionary with condition names and lambdas

    def _process_stage(self, df):
        """
        :param df: Input pandas DataFrame that is to be processed.
        :return: DataFrame filtered based on cumulative conditions.
        """
        self.computation_df = df.copy()  # Start with the input DataFrame

        # Apply each condition and create columns in computation_df
        self.computation_df['#'] = pd.DataFrame({'#': ['#'] * len(df)})
        for cond_name, cond_func in self.conditions.items():
            self.computation_df[cond_name] = cond_func(df)

        # Cumulative condition column
        self.computation_df['all_conditions'] = self.computation_df[list(self.conditions.keys())].all(axis=1)

        # Output DataFrame: Only the filtered results
        self.output_df = df[self.computation_df['all_conditions']]
        return self.output_df
