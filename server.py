import cherrypy
import os

from girder.api import access
from girder.api.describe import Description
from girder.api.rest import Resource
from girder.constants import ROOT_DIR

DEFAULT_FILE = 'file://' + os.path.join(ROOT_DIR, 'plugins', 'sparktest',
                                        'FL_insurance_sample.csv')
DEFAULT_SCRIPT = \
"""
# Filter to get only the records from Clay county
textFile = sc.textFile(file)
clayCounty = textFile.filter(lambda line: 'clay county' in line.lower())

print('TOTAL = %d' % clayCounty.count())
import pprint
pprint.pprint(clayCounty.collect())
"""


class SparkTest(Resource):
    def __init__(self):
        self.resourceName = 'sparktest'

        self.route('POST', (), self.testSparkTask)

    def testSparkTask(self, params):
        user = self.getCurrentUser()

        apiUrl = os.path.dirname(cherrypy.url())

        script = cherrypy.request.body.read().decode('utf8') or DEFAULT_SCRIPT

        task = {
            'mode': 'spark.python',
            'script': script,
            'inputs': [{
                'id': 'file',
                'type': 'string',
                'format': 'string'
            }]
        }

        inputs = {
            'file': {
                'mode': 'inline',
                'format': 'string',
                'data': params.get('file', DEFAULT_FILE)
            }
        }

        job = self.model('job', 'jobs').createJob(
            'spark test', 'romanesco', user=user, handler='romanesco_handler',
            kwargs={
                'task': task,
                'inputs': inputs,
                'auto_convert': False,
                'validate': False
            }
        )

        jobToken = self.model('job', 'jobs').createJobToken(job)

        job['kwargs']['jobInfo'] = {
            'method': 'PUT',
            'url': '/'.join((apiUrl, 'job', str(job['_id']))),
            'headers': {'Girder-Token': jobToken['_id']},
            'logPrint': True
        }

        job = self.model('job', 'jobs').save(job)

        self.model('job', 'jobs').scheduleJob(job)

        return self.model('job', 'jobs').filter(job, user)
    testSparkTask.description = (
        Description('Spark+romanesco test')
        .param('body', 'The script to execute', required=False,
               paramType='body', default=DEFAULT_SCRIPT)
        .param('file', 'Path to the text file.', required=False,
               default=DEFAULT_FILE))

def load(info):
    info['apiRoot'].sparktest = SparkTest()
