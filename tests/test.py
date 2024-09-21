import pandas as pd

from xreport.pipeline import DataPipeline
from xreport.stages.expand_stage import ExpandStage
from xreport.stages.filter_stage import FilterStage
from xreport.stages.map_stage import MapStage
from xreport.stages.projection_stage import ProjectionStage
from xreport.stages.rename_stage import RenameStage
from xreport.stages.source_stage import SourceStage


def get_state_and_postal_code_df(cities_df):
    return pd.DataFrame({
        'City': ['Los Angeles', 'Chicago', 'Houston'],
        'State': ['California', 'Illinois', 'Texas'],
        'PostalCode': ['90001','60601','77001']
    })

def map_city_to_country(city):
    mapping = {
        'Los Angeles': 'USA',
        'Chicago': 'USA',
        'Houston': 'USA',
        'New York': 'USA',
        'Miami': 'USA'
    }
    return mapping.get(city, 'Unknown')

def age_category(age):
    if age < 30:
        return 'Young'
    elif age < 40:
        return 'Adult'
    else:
        return 'Senior'


# Sample Data
data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Nobody'],
    'age': [25, 30, 35, 40, 31],
    'sex': ['F','M','U','X','Y'],
    'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Kreta']
})

# Define Pipeline
pipeline = DataPipeline(
    'Sample Pipeline',
    'This is a sample data pipeline',
    SourceStage(
        "Source Data",
        "Initial DataFrame input", data))

# Add stages
pipeline.add_stage(
    FilterStage(
        'Age Filter',
        'Filter age between 30 and 35 years old', {
    'age_gt_30_check': lambda df: df['age'] >= 30,
    'age_lt_35_check': lambda df: df['age'] <= 35,
}))
pipeline.add_stage(
    ProjectionStage(
        'Select Name and City',
        'Keep only name and city columns',
        ['name', 'city', 'age']))
pipeline.add_stage(
    RenameStage(
        "Rename Name Column",
        "Renaming Name to FirstName",
        {'name': 'Full Name', 'city': 'City', 'age': 'Age'}))
pipeline.add_stage(
    ExpandStage("Expand State Information",
                "Adding State information for each City",
                               join_columns=['City'],
                               lambda_func=get_state_and_postal_code_df))
pipeline.add_stage(FilterStage(
    'Drop Unknown state',
    'Filter only in state is found', {
    'state_ok': lambda df: df['State'].notnull() & (df['State'] != ''),

}))
pipeline.add_stage(
    MapStage(
        "Map City to Country and Age Category",
        "Mapping City names to their countries and categorizing ages",
        {'City': map_city_to_country, 'Age': age_category}))

# Run pipeline
result_df = pipeline.run()

# Generate and store HTML report
html_report = pipeline.generate_report()
with open('index.html', 'w') as f:
    f.write(html_report)
