import google.auth
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
#from apache_beam.utils.pipeline_options import SetupOptions
#from apache_beam.utils.pipeline_options import StandardOptions

options = PipelineOptions()

#Setting the Options Programmatically
options = PipelineOptions(flags=[])

#set the project to the default project in your current Google Cloud Environment
_, options.view_as(GoogleCloudOptions).project=google.auth.default()

#Set the Google Cloud Region in which Cloud Dataflow run
options.view_as(GoogleCloudOptions).region='us-west1'

#Cloud Storage Location
dataflow_gcs_location='gs://gcp-dataeng-demos-soumya/dataflow'

#Dataflow Staging Location. This location is used to stage the Dataflow Pipeline and SDK binary
options.view_as(GoogleCloudOptions).staging_location = '{}/staging'.format(dataflow_gcs_location)
#Dataflow Temp Location. This location is used to store temporary files or intermediate results before finally outputting
options.view_as(GoogleCloudOptions).temp_location = '{}/temp'.format(dataflow_gcs_location)
#The directory to store the output files of the job
output_gcs_location = '{}/output'.format(dataflow_gcs_location)




# Dataflow Runner

import apache_beam as beam
from apache_beam.runners import DataflowRunner

def toFloat(x):
    test_list = list(map(float, x))
    return test_list

def makeRow(x):
    row = dict(zip(('v1','v2','v3','v4','v5','v6'),x))
    return row

p = beam.Pipeline(options=options) #option must be given here and when dataflowrunner is called, otherwise an error will occur. To resolve that error, some mechanism also exists.


stage1 = p|"Input" >> beam.io.ReadFromText('gs://gcp-dataeng-demos-soumya/dataflow/inputs_file01.csv')
stage2 = stage1| "Split">>beam.Map(lambda x: x.split(','))
stage3 = stage2| "Conversion to Float">>beam.Map(toFloat)
stage4 = stage3| "Make a bigquery row">>beam.Map(makeRow)
stage5 = stage4| "Write to Bigquery">>beam.io.Write(
                                               beam.io.WriteToBigQuery(
                                                                        'my-table-just-now',
                                                                        dataset='gcpdataset',
                                                                        project='mimetic-parity-378803',
                                                                        schema='v1:FLOAT,v2:FLOAT,v3:FLOAT,v4:FLOAT,v5:FLOAT,v6:FLOAT',
                                                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
                                                                        
                                                                      )
                                                       )
    

pipeline_result = DataflowRunner().run_pipeline(p, options=options)
