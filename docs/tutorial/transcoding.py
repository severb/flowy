from flowy.activity import Activity
from flowy.client import activity_client


@activity_client('Transcoding', 1, 'transcoding_list')
class Transcoding(Activity):

    def some_transcoding_function(self, source):
        # Business logic goes here, but we're not interested in that
        pass

    def run(self, source_file):
        status = self.some_transcoding_function(source_file)
        return status


activity_client.start_on('VideoApp', 'transcoding_list')
