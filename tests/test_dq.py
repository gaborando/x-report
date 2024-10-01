# Create a custom warning function for duplicate checks
import pandas as pd

from tests.other.customer_integration.lib.utils import check_pipeline
from xreport.pipeline import DataPipeline
from xreport.stages.data_quality_stage import RowLevelCheck, DataFrameLevelCheck, DataQualityStage
from xreport.stages.source_stage import SourceStage


def duplicate_warning_resolution_func(df):
    duplicates = df[df.duplicated(subset=['Name', 'Surname'], keep=False)]
    warnings_resolutions = []

    if not duplicates.empty:
        # Get all duplicates as a grouped DataFrame
        duplicated_pairs = duplicates.groupby(['Name', 'Surname']).size().reset_index(name='Count')

        for _, row in duplicated_pairs.iterrows():
            # Find the indices of duplicated rows based on Name and Surname
            name = row['Name']
            surname = row['Surname']
            duplicate_indices = duplicates[(duplicates['Name'] == name) & (duplicates['Surname'] == surname)].index

            # Get the line numbers
            line_numbers = ', '.join(str(idx) for idx in duplicate_indices)
            warnings_resolutions.append((
                f"Duplicate Name-Surname, {name} + {surname} is duplicated on lines: {line_numbers}",
                "Remove duplicate Name-Surname pairs"
            ))

    return warnings_resolutions if warnings_resolutions else [("No duplicates found.", "No action needed.")]


# Example Usage in the Pipeline

# Sample DataFrame
data = pd.DataFrame({
    'Name': ['Alice', 'Bob', 'Charlie', 'Alice', 'Bob'],
    'Surname': ['Smith', 'Jones', 'Brown', 'Smith','Jones'],
    'Salary': [50000, 1000000000, 2000000000, 30000,4000],
    'Country': ['USA', 'Canada', 'USA', 'Canada','EU']
})

# Create the DataPipeline
pipeline = DataPipeline(
    'id1',
    'Data Quality Pipeline',
    'A pipeline to check data quality and transformations.',
    SourceStage('id0', "Source Data", "Initial DataFrame input", data)
)

# Row-level checks
row_checks = [
    RowLevelCheck(
        name='Salary Over 1 Billion',
        description='Check if salary exceeds 1 billion',
        check_func=lambda row: row['Salary'] <= 1_000_000_000,
        warning_func=lambda row: f"Salary too high: {row['Salary']}",
        resolution_func=lambda row: "Reduce salary below 1 billion"
    )
]

# DataFrame-level checks
df_checks = [
    DataFrameLevelCheck(
        name='Duplicate Name-Surname',
        check_func=lambda df: df.duplicated(subset=['Name', 'Surname']).sum() == 0,
        warning_resolution=duplicate_warning_resolution_func  # Use the custom function
    ),
    DataFrameLevelCheck(
        name='Country Count',
        check_func=lambda df: df['Country'].nunique() >= 2,
        warning_resolution=lambda df: [("Country count is less than 2", "Ensure at least 2 unique countries are present")]
    )
]

# Adding Data Quality Stage to the pipeline
data_quality_stage = DataQualityStage(
    'id2',
    name='Data Quality Check',
    description='Checking data quality for salary, duplicates, and country count',
    row_checks=row_checks,
    df_checks=df_checks
)
pipeline.add_stage(data_quality_stage)

# Run the pipeline
result_df = pipeline.run()

# Output the resulting DataFrame and computation DataFrame
print("Result DataFrame:")
print(result_df)

print("\nComputation DataFrame (Failed Checks):")
print(data_quality_stage.computation_df)

html_report = pipeline.generate_report()
with open('index_dq.html', 'w') as file:
    file.write(html_report)

check_pipeline(pipeline)