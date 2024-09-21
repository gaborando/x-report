import pandas as pd

# Import necessary classes for the data pipeline and stages
from xreport.pipeline import DataPipeline
from xreport.stages.expand_stage import ExpandStage
from xreport.stages.filter_stage import FilterStage
from xreport.stages.map_stage import MapStage
from xreport.stages.projection_stage import ProjectionStage
from xreport.stages.rename_stage import RenameStage
from xreport.stages.source_stage import SourceStage
from xreport.stages.groupby_stage import GroupByStage  # Import GroupByStage

# Function to create a DataFrame mapping cities to their states and postal codes
def get_state_and_postal_code_df(cities_df):
    return pd.DataFrame({
        'City': ['Los Angeles', 'Chicago', 'Houston'],
        'State': ['California', 'Illinois', 'Texas'],
        'PostalCode': ['90001', '60601', '77001']
    })

# Function to map cities to their respective countries
def map_city_to_country(city):
    mapping = {
        'Los Angeles': 'USA',
        'Chicago': 'USA',
        'Houston': 'USA',
        'New York': 'USA',
        'Miami': 'USA'
    }
    return mapping.get(city, 'Unknown')

# Function to categorize age into groups
def age_category(age):
    if age < 30:
        return 'Young'
    elif age < 40:
        return 'Adult'
    else:
        return 'Senior'

# Sample DataFrame creation with names, ages, sexes, cities, and salaries
data = pd.DataFrame({
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Nobody'],
    'age': [25, 30, 35, 40, 31],
    'sex': ['F', 'M', 'U', 'X', 'Y'],
    'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Kreta'],
    'salary': [70000, 80000, 75000, 90000, 60000]  # Adding salary column
})

# Define the DataPipeline instance with a source stage containing the sample data
pipeline = DataPipeline(
    'Sample Pipeline',
    'This is a sample data pipeline',
    SourceStage(
        "Source Data",
        "Initial DataFrame input",
        data
    )
)

# Add a filtering stage to retain only rows where age is between 30 and 35
pipeline.add_stage(
    FilterStage(
        'Age Filter',
        'Filter age between 30 and 35 years old', {
            'age_gt_30_check': lambda df: df['age'] >= 30,
            'age_lt_35_check': lambda df: df['age'] <= 35,
        }
    )
)

# Add a projection stage to select only specific columns
pipeline.add_stage(
    ProjectionStage(
        'Select Name and City',
        'Keep only name and city columns',
        ['name', 'city', 'age', 'salary']  # Include salary in projection
    )
)

# Add a renaming stage to change column names for clarity
pipeline.add_stage(
    RenameStage(
        "Rename Name Column",
        "Renaming Name to Full Name",
        {'name': 'Full Name', 'city': 'City', 'age': 'Age', 'salary': 'Salary'}
    )
)

# Add an expand stage to include state information based on city
pipeline.add_stage(
    ExpandStage(
        "Expand State Information",
        "Adding State information for each City",
        join_columns=['City'],
        lambda_func=get_state_and_postal_code_df
    )
)

# Add a filtering stage to drop rows with unknown states
pipeline.add_stage(FilterStage(
    'Drop Unknown State',
    'Filter only if state is found', {
        'state_ok': lambda df: df['State'].notnull() & (df['State'] != ''),
    }
))

# Add a mapping stage to categorize cities and ages
pipeline.add_stage(
    MapStage(
        "Map City to Country and Age Category",
        "Mapping City names to their countries and categorizing ages",
        {'City': map_city_to_country, 'Age': age_category}
    )
)

# Add a group by stage to calculate the average salary for each unique salary value
pipeline.add_stage(
    GroupByStage(
        "Group By Salary",
        "Calculating average salary for each unique salary",
        group_by_columns=['City','Age'],  # Column to group by
        agg_funcs={'Salary': 'sum'}  # Count the number of entries for each salary
    )
)

# Run the pipeline and store the resulting DataFrame
result_df = pipeline.run()

# Display the resulting DataFrame
print(result_df)

html_report = pipeline.generate_report()
with open('index.html', 'w') as file:
    file.write(html_report)

