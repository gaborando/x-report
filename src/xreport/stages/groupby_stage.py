import pandas as pd
from .base_stage import BaseStage

class GroupByStage(BaseStage):
    def __init__(self, stage_id, name, description, group_by_columns, agg_funcs):
        """
        :param stage_id: Unique identifier for the stage
        :param name: Name of the stage
        :param description: Description of the stage
        :param group_by_columns: Columns used for grouping the data in this stage
        :param agg_funcs: Aggregation functions to be applied on the grouped data
        """
        super().__init__(stage_id, name, description)
        self.group_by_columns = group_by_columns
        self.agg_funcs = agg_funcs

    def execute(self, input_df):
        """
        :param input_df: A pandas DataFrame that needs to be processed.
        :return: A new DataFrame containing the aggregated results.
        """
        # Set input DataFrame
        self.input_df = input_df.copy()

        # Perform the group by operation
        grouped = self.input_df.groupby(self.group_by_columns)

        # Store aggregation results
        aggregation_result = grouped.agg(self.agg_funcs).reset_index()

        # Build the computation DataFrame
        computation_list = []
        for group_values, group_data in grouped:
            computation_list.append(group_data)

            # Build a boolean mask for each grouping key
            mask = pd.Series(True, index=aggregation_result.index)
            for col, val in zip(self.group_by_columns, group_values if isinstance(group_values, tuple) else [group_values]):
                mask &= aggregation_result[col] == val

            # Get the corresponding aggregation row for the group
            agg_row = aggregation_result[mask].copy()

            # Create a label for grouping in the first column (e.g., 'Agg for group (A)')
            agg_row.loc[:, self.group_by_columns] = f'Aggregation for group ({", ".join(map(str, group_values))})'

            # Append the aggregated row after each group's data
            computation_list.append(agg_row)

        # Concatenate the sorted data with the aggregation summary at the end of each group
        if len(computation_list) > 0:
            computation_df = pd.concat(computation_list)
        else:
            computation_df = self.input_df.sort_values(by=self.group_by_columns)

        # Set the computation DataFrame
        self.computation_df = computation_df

        # Set the output DataFrame to the aggregated result
        self.output_df = aggregation_result

        return self.output_df
