import boto3
import time
import cv2
import datetime
import requests
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
import botocore.session
import pymkv
from subprocess import PIPE, run


input_stream_name = "33ba8db3-5fcb-f3f7-7ad6-f8e2c7435931"
output_stream_name = "david-test"


def log(*args):
    print(datetime.datetime.now().isoformat(), *args)


def process_mkv(name):
    video_capture = cv2.VideoCapture(name)
    width = int(video_capture.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(video_capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
    fps = int(video_capture.get(cv2.CAP_PROP_FPS))

    # result = run(["mkvinfo", name], stdout=PIPE, stderr=PIPE, universal_newlines=True)
    # result.stdout.splitlines()
    # print(result.returncode, result.stdout, result.stderr)
    # exit()

    fourcc = cv2.VideoWriter_fourcc(*"X264")
    out = cv2.VideoWriter(f"{name}-processed.mkv", fourcc, fps, (width, height))
    timestamp = f"{time.mktime(datetime.datetime.now().timetuple()):.3f}"

    while video_capture.isOpened():
        has_frames, frame = video_capture.read()
        if not has_frames:
            break
        frame = cv2.flip(frame, 0)
        out.write(frame)

    run(["mkvpropedit", name, "-t", f"all:{name}"], stdout=PIPE, stderr=PIPE, universal_newlines=True)

    video_capture.release()
    out.release()



def get_input(stream):
    mkv = b''

    start = time.time()
    while True:
        frame = stream.read(100 * 1024)
        # yield frame
        if mkv == b'':
            mkv += frame
        else:
            index = frame.find(b'\x1aE\xdf\xa3')
            if index == -1:
                mkv += frame
            else:
                mkv += frame[0:index]
                yield mkv
                # filename = f"./files/file-{time.time()}.mkv"
                # with open(filename, 'wb') as f:
                #     f.write(mkv)
                # with open(filename, 'rb') as fr:
                #     print(time.time(), 'yield')
                #     yield fr.read()
                print('read', time.time() - start)
                starte = time.time()
                # process_mkv(filename)
                # with open(f"{filename}", "rb") as o:
                #     print('sending file')
                #     yield o.read()
                # with open(f"{filename}-processed.mkv", "rb") as o:
                #     print('sending file')
                #     yield o.read()
                    # return o.read()
                print('extract', time.time() - starte)
                print('processed', time.time() - start)
                start = time.time()
                mkv = frame[index:]


log("Starting up...")
kinesis_client = boto3.client("kinesisvideo", region_name="eu-central-1")

log("Getting GET_MEDIA data endpoint...")
get_media_response = kinesis_client.get_data_endpoint(StreamName=input_stream_name, APIName="GET_MEDIA")
log(f"GET_MEDIA data endpoint loaded: {get_media_response['DataEndpoint']}")

log("Loading input stream...")
input_video_client = boto3.client("kinesis-video-media", endpoint_url=get_media_response["DataEndpoint"])
input_stream = input_video_client.get_media(StreamName=input_stream_name, StartSelector={"StartSelectorType": "NOW"})
log("Input stream loaded")

log("Loading output PUT_MEDIA data endpoint")
output_response = kinesis_client.get_data_endpoint(StreamName=output_stream_name, APIName="PUT_MEDIA")
log(f"PUT_MEDIA data endpoint loaded: {output_response['DataEndpoint']}")


log("Starting to read input stream...")

# 1498511782000L
# String.format(Locale.US, "%.3f", mBuilder.mTimestamp / MILLI_TO_SEC)


session = botocore.session.Session()
sigv4 = SigV4Auth(session.get_credentials(), 'kinesisvideo', 'eu-central-1')
endpoint = f"{output_response['DataEndpoint']}/putMedia"
put_media_headers = {
    "x-amzn-stream-name": output_stream_name,
    "x-amzn-fragment-timecode-type": "ABSOLUTE",
    #"x-amz-content-sha256": "UNSIGNED-PAYLOAD",
    # "x-amzn-producer-start-timestamp": str(int(time.mktime(datetime.datetime.now().timetuple()) * 1000)) + ".000",
    # "x-amzn-producer-start-timestamp": "1498511782.000",
    # "transfer-encoding": "chunked",
    # "Content-Type": "application/octet-stream"
}
request = AWSRequest(method='POST', url=endpoint, headers=put_media_headers)
#request.context["payload_signing_enabled"] = False  # This is mandatory since VpcLattice does not support payload signing. Not providing this will result in error.
sigv4.add_auth(request)
prepped = request.prepare()

# r = requests.post(prepped.url, headers=prepped.headers, data=get_input(input_stream["Payload"]))
# print(r.content, r.headers)

mkv = b''

while True:
    frame = input_stream["Payload"].read(1024)
    if mkv == b'':
        mkv += frame
    else:
        index = frame.find(b'\x1aE\xdf\xa3')
        if index == -1:
            mkv += frame
        else:
            mkv += frame[0:index]
            log("Processing mkv..")
            filename = f"./files/file-{time.time()}.mkv"
            with open(filename, 'wb') as f:
                f.write(mkv)
            process_mkv(filename)
            with open(f"{filename}-processed.mkv", "rb") as o:
                log("Sending mkv...")
                request = AWSRequest(method='POST', url=endpoint, headers=put_media_headers)
                sigv4.add_auth(request)
                prepped = request.prepare()
                r = requests.post(prepped.url, headers=prepped.headers, data=o.read())
                print(r.content, r.headers)
            mkv = frame[index:]


# Keep alive?