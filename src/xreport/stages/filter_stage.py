from xreport.stages.base_stage import BaseStage


class FilterStage(BaseStage):
    def __init__(self, name, description, conditions):
        super().__init__(name, description)
        self.conditions = conditions  # Should be a dictionary with condition names and lambdas

    def _process_stage(self, df):
        self.computation_df = df.copy()  # Start with the input DataFrame

        # Apply each condition and create columns in computation_df
        for cond_name, cond_func in self.conditions.items():
            self.computation_df[cond_name] = cond_func(df)

        # Cumulative condition column
        self.computation_df['all_conditions'] = self.computation_df[list(self.conditions.keys())].all(axis=1)

        # Output DataFrame: Only the filtered results
        self.output_df = df[self.computation_df['all_conditions']]
        return self.output_df
