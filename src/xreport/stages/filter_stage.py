import pandas as pd

from xreport.stages.base_stage import BaseStage


class FilterStage(BaseStage):
    """
    Class representing a filter stage in a data processing pipeline.

    class FilterStage(BaseStage):

        def __init__(self, name, description, conditions):
            Initialize the FilterStage with a name, description, and filtering conditions.

            :param name: The name of the filter stage.
            :param description: A brief description of what the filter stage does.
            :param conditions: A dictionary where keys are condition names and values are lambda functions that apply the condition on a DataFrame.

        def _process_stage(self, df):
            Process the given DataFrame through all the defined conditions, filter the data, and save the results.

            :param df: The input DataFrame to process.
            :return: A DataFrame containing only rows that meet all the conditions.
    """
    def __init__(self, name, description, conditions):
        """
        :param name: The name of the instance.
        :param description: A brief description of the instance.
        :param conditions: A dictionary containing condition names as keys and lambda functions as values.
        """
        super().__init__(name, description)
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
