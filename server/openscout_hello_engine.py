# OpenScout
#   - Distributed Automated Situational Awareness
#
#   Author: Thomas Eiszler <teiszler@andrew.cmu.edu>
#
#   Copyright (C) 2020 Carnegie Mellon University
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
#

import base64
import time
import os
import uuid
import cv2
import numpy as np
import logging
from gabriel_server import cognitive_engine
from gabriel_protocol import gabriel_pb2
from openscout_protocol import openscout_pb2
from io import BytesIO
from PIL import Image
import asyncio
import io
import glob
import uuid
import requests
import json
import pandas
import pytesseract


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

detection_log = logging.getLogger("text-engine")
fh = logging.FileHandler('/openscout/server/openscout-text-engine.log')
fh.setLevel(logging.INFO)
formatter = logging.Formatter('%(message)s')
fh.setFormatter(formatter)
detection_log.addHandler(fh)


class HelloWorldEngine(cognitive_engine.Engine):
    ENGINE_NAME = "openscout-hello"

    def __init__(self, args):
        self.name = args.name
        logger.info("Hello World Name: {}".format(args.name))

    def handle(self, input_frame):
        if input_frame.payload_type != gabriel_pb2.PayloadType.IMAGE:
            status = gabriel_pb2.ResultWrapper.Status.WRONG_INPUT_FORMAT
            return cognitive_engine.create_result_wrapper(status)

        extras = cognitive_engine.unpack_extras(openscout_pb2.Extras, input_frame)

        status = gabriel_pb2.ResultWrapper.Status.SUCCESS
        result_wrapper = cognitive_engine.create_result_wrapper(status)
        result_wrapper.result_producer_name.value = self.ENGINE_NAME

        result = gabriel_pb2.ResultWrapper.Result()
        result.payload_type = gabriel_pb2.PayloadType.TEXT
        image = self.preprocess_image(input_frame.payloads[0])


        #detect_result = pytesseract.image_to_string(image).encode('utf-8').strip()
        text = pytesseract.image_to_data(image, output_type='data.frame')
        text = text[text.conf != -1]
        lines = text.groupby(['page_num', 'block_num', 'par_num', 'line_num'])['text'] \
                                            .apply(lambda x: ' '.join(list(x))).tolist()
        confs = text.groupby(['page_num', 'block_num', 'par_num', 'line_num'])['conf'].mean().tolist()
        line_conf = []
        for i in range(len(lines)):
            if lines[i].strip():
                line_conf.append((lines[i].strip(), round(confs[i],3)))

        for text_row in line_conf:
            logger.info("OCR text: {}".format(text_row[0]))
            result.payload = "Detected {} ({:.3f})".format(text_row[0],text_row[1]).encode(encoding="utf-8")
            detection_log.info("{},{},{},{},{:.3f},".format(extras.client_id, extras.location.latitude, extras.location.longitude, text_row[0],text_row[1]))

        '''
        if len(detect_result.strip()) > 0:
            logger.info("OCR given text: {}".format(detect_result))
            result.payload = 'Text is {}'.format(detect_result).encode(encoding="utf-8")
            detection_log.info("{},{},{},{},{:.3f},".format(extras.client_id, extras.location.latitude, extras.location.longitude, detect_result, 0))
        '''

        result_wrapper.results.append(result)
        return result_wrapper


    def preprocess_image(self, image):
        np_data = np.fromstring(image, dtype=np.uint8)
        img = cv2.imdecode(np_data, cv2.IMREAD_COLOR)
        img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
        return img
