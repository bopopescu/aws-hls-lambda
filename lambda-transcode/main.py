from __future__ import print_function
import boto3
import os
import sys
import uuid
import boto
import hashlib
import json
import boto.elastictranscoder

s3_client = boto3.client('s3')
env_dist = os.environ


def lambda_handler(event, context):
    for record in event['Records']:
        # bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        suffixArr = key.split(".")
        suffix = suffixArr[1]
        suffixList = ['avi', 'mkv', 'mp4', 'flv', 'wmv']
        if not suffix in suffixList:
            print("This format is not supported.")
            exit()
        print("fileName: ", key)
        HlsJobCreation(key)


def HlsJobCreation(key):
    # This is the ID of the Elastic Transcoder pipeline that was created when
    # setting up your AWS environment:
    # http://docs.aws.amazon.com/elastictranscoder/latest/developerguide/sample-code.html#python-pipeline
    pipeline_id = env_dist['pipeline_id']

    # This is the name of the input key that you would like to transcode.
    input_key = key

    # Region where the sample will be run
    # region = 'ap-southeast-1'
    region = env_dist['region']

    # HLS Presets that will be used to create an adaptive bitrate playlist.
    # hls_64k_audio_preset_id = '1351620000001-200071';
    hls_0400k_preset_id = '1351620000001-200050'
    hls_0600k_preset_id = '1351620000001-200040'
    # hls_1000k_preset_id = '1351620000001-200030';
    # hls_1500k_preset_id = '1351620000001-200020';
    # hls_2000k_preset_id = '1351620000001-200010';

    # HLS Segment duration that will be targeted.
    # segment_duration = '2'
    segment_duration = env_dist['segment_duration']

    # All outputs will have this prefix prepended to their output key.
    # output_key_prefix = 'elastic-transcoder-samples/output/hls/'
    output_key_prefix = env_dist['output_key_prefix']

    # Creating client for accessing elastic transcoder
    transcoder_client = boto.elastictranscoder.connect_to_region(region)

    # Setup the job input using the provided input key.
    job_input = {'Key': input_key}

    # Setup the job outputs using the HLS presets.
    output_key = hashlib.sha256(input_key.encode('utf-8')).hexdigest()
    # hls_audio = {
    #     'Key': 'hlsAudio/' + output_key,
    #     'PresetId': hls_64k_audio_preset_id,
    #     'SegmentDuration': segment_duration
    # }
    hls_400k = {
        'Key': 'hls0400k/' + output_key,
        'PresetId': hls_0400k_preset_id,
        'SegmentDuration': segment_duration
    }
    hls_600k = {
        'Key': 'hls0600k/' + output_key,
        'PresetId': hls_0600k_preset_id,
        'SegmentDuration': segment_duration
    }
    # hls_1000k = {
    #     'Key': 'hls1000k/' + output_key,
    #     'PresetId': hls_1000k_preset_id,
    #     'SegmentDuration': segment_duration
    # }
    # hls_1500k = {
    #     'Key': 'hls1500k/' + output_key,
    #     'PresetId': hls_1500k_preset_id,
    #     'SegmentDuration': segment_duration
    # }
    # hls_2000k = {
    #     'Key': 'hls2000k/' + output_key,
    #     'PresetId': hls_2000k_preset_id,
    #     'SegmentDuration': segment_duration
    # }
    job_outputs = [hls_400k, hls_600k]

    # Setup main playlist which can be used to play using adaptive bitrate.
    playlist = {
        'Name': 'hls_' + output_key,
        'Format': 'HLSv3',
        'OutputKeys': list(map(lambda x: x['Key'], job_outputs))
    }

    # Creating the job.
    create_job_request = {
        'pipeline_id': pipeline_id,
        'input_name': job_input,
        'output_key_prefix': output_key_prefix + output_key + '/',
        'outputs': job_outputs,
        'playlists': [playlist]
    }
    create_job_result = transcoder_client.create_job(**create_job_request)
    print('HLS job has been created: ', json.dumps(create_job_result['Job'], indent=4, sort_keys=True))


'''   
return    
trans = {
   "state" : "PROGRESSING|COMPLETED|WARNING|ERROR",
   "errorCode" : "the code of any error that occurred",
   "messageDetails" : "the notification message you created in Amazon SNS",
   "version" : "API version that you used to create the job",
   "jobId" : "value of Job:Id object that Elastic Transcoder returns in the response to a Create Job request",
   "pipelineId" : "value of PipelineId object in the Create Job request",
   "input" : {
      job Input settings
   },
   "outputKeyPrefix" : "prefix for file names in Amazon S3 bucket",
   "outputs": [
      {
         applicable job Outputs settings,
         "status" : "Progressing|Completed|Warning|Error"
      },
      {...}
   ],
   "playlists": [
      {
         applicable job playlists settings
      }
   ],
   "userMetadata": {
      "metadata key": "metadata value"
   }
}
'''
