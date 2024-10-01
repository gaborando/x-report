import csv
import time
import traceback
from datetime import datetime

import pandas as pd

from xreport.report import generate_html_report
from xreport.stages.source_stage import SourceStage


class DataPipeline:
    """
        class DataPipeline:

        Attributes:
            pipeline_id (str): The id of the data pipeline.
            name (str): The name of the data pipeline.
            description (str): A brief description of the data pipeline.
            stages (list): A list of stages included in the data pipeline.
            report (list): A list capturing the execution details of each stage.
            debug (bool): Print debug logs

        Methods:
            __init__(name, description, source_stage):
                Initializes the DataPipeline object with a name, description, and source stage.

            add_stage(stage):
                Adds a new stage to the data pipeline.

            run():
                Executes all the stages in the pipeline sequentially. Returns the resulting DataFrame.

            to_report():
                Creates a report dictionary containing details of the pipeline execution.

            generate_report():
                Generates an HTML report of the pipeline execution details.
    """
    def __init__(self, pipeline_id, name, description, source_stage: SourceStage, debug = True):
        """
        :param pipeline_id: The unique identifier for the pipeline.
        :param name: The name of the pipeline.
        :param description: A brief description of the pipeline.
        :param source_stage: The initial stage of the pipeline, which should be an instance of SourceStage.
        :param debug: Flag to indicate whether to run the pipeline in debug mode. Defaults to True.
        """
        self.pipeline_id = pipeline_id
        self.name = name
        self.description = description
        self.stages = [source_stage]
        self.report = []
        self.error = None
        self.debug = debug

    def add_stage(self, stage):
        """
        :param stage: The stage to be added to the list of stages.
        :return: None
        """
        self.stages.append(stage)

    def run(self):
        """
        Executes a series of stages, tracking the execution status, time, and errors.
        Updates a report with detailed information for each stage.

        :return: Final DataFrame obtained after executing all the stages.
        """
        current_df = pd.DataFrame()

        pipeline_start_time = time.time()  # Start timing

        if self.debug:
            print(f"Starting pipeline {self.name} ({self.pipeline_id})")

        self.error = None

        for index, stage in enumerate(self.stages):
            start_time = time.time()  # Start timing
            status = 'SUCCESS'
            if self.debug:
                print(f"Executing stage {index} - {stage.name} ({stage.stage_id})")
                if stage.input_df is not None:
                    print(f"Input shape: {stage.input_df.shape}")
                    print(f"Input columns: {list(stage.input_df.columns)}")
            if self.error is None:
                try:
                    current_df = stage.execute(current_df)
                except Exception as e:
                    self.error = e
                    status = 'ERROR'
            else:
                status = 'NOT_DONE'

            if self.error is not None:
                stage.output_df = pd.DataFrame()
                stage.computation_df = pd.DataFrame()
                stage.input_df = pd.DataFrame()
            execution_time = time.time() - start_time  # Calculate execution time

            if self.debug:
                print(f"Stage {index} - {stage.name} ({stage.stage_id}) executed in {execution_time} seconds with status {status}")
                print(f"Output shape: {stage.output_df.shape}")
                print(f"Output columns: {list(stage.output_df.columns)}")

            self.report.append({
                'stage_number': index + 1,  # Stage number (1-based index)
                'stage_id': stage.stage_id,
                'stage_type': stage.__class__.__name__,  # Add stage type here
                'stage_name': stage.name,
                'description': stage.description,
                'input_shape': list(stage.input_df.shape),
                'output_shape': list(stage.output_df.shape),
                'execution_time': execution_time,
                'computation_data': stage.computation_df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC),
                'output_data': stage.output_df.to_csv(index=False, quoting=csv.QUOTE_NONNUMERIC),
                'status': status
            })


        pipeline_execution_time = time.time() - pipeline_start_time  # Calculate execution time
        if self.debug:
            print(f"Pipeline {self.name} ({self.pipeline_id}) ended in {pipeline_execution_time} with status {'OK' if self.error is None else 'ERROR'}")

        return current_df

    def to_report(self):
        """
        :return: Dictionary containing the report's name, description, pipeline status, error, and timestamp.
        """
        return {
            'pipeline_id': self.pipeline_id,
            'name': self.name,
            'description': self.description,
            'pipeline': self.report,
            'error': self.__exception_to_json(self.error) if self.error is not None else None,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def __exception_to_json(self, ex):
        # Capture the exception type, message, and stack trace
        exc_type = type(ex).__name__
        exc_message = str(ex)
        exc_traceback = "\n".join(traceback.format_exception(type(ex), value=ex, tb=ex.__traceback__))

        # Construct the JSON-friendly dictionary
        exc_dict = {
            "type": exc_type,
            "message": exc_message,
            "traceback": exc_traceback
        }

        # Convert the dictionary
        return exc_dict

    def generate_report(self):
        """
        Generates an HTML report by first converting the data to a report format and then generating the HTML content.

        :return: A string containing the generated HTML report.
        """
        return generate_html_report(self.to_report())


