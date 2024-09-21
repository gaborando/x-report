import pandas as pd

from xreport.stages.base_stage import BaseStage


class MapStage(BaseStage):
    def __init__(self, name, description, mappings):
        super().__init__(name, description)
        self.mappings = mappings  # A dictionary of column names and their mapping functions

    def _process_stage(self, input_df):
        self.computation_df = pd.DataFrame()
        df = input_df.copy()
        # Initialize computation DataFrame

        for column, mapping_func in self.mappings.items():
            # Create a copy of the original column for the computation DataFrame
            pre_mapped_column = df[column].copy()

            # Apply the mapping function
            df[column] = df[column].apply(mapping_func)

            # Concatenate the mapped column and the previous column into computation_df
            self.computation_df = pd.concat(
                [ self.computation_df,

                  pd.DataFrame({f"{column} (Prev)": input_df[column]})],
                axis=1
            )

        self.computation_df = pd.concat([df, pd.DataFrame({'#': ['#'] * len(df)}),self.computation_df], axis=1)

        self.output_df = df
        return self.output_df
