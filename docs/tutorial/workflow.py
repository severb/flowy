from flow.client import workflow_client
from flowy.workflow import Workflow, ActivityProxy


@workflowclient('UploadWorkflow', 3, 'upload_list')
class ProcessUploadedVideoWorkflow(Workflow):

    transcode = ActivityProxy('Transcoding', 1)
    generate_thumbnails = ActivityProxy('ThumbnailGenerator', 1)
    process_metadata = ActivityProxy('MetadataProcessing', 1)

    def run(self, video_id, source):
        transcodings = self.transcode(source)
        thumbnails = self.generate_thumbnails(source)
        status = process_metadata(video_id,
                                  transcodings.result(), thumbnails.result())
        return status.result()


workflow_client.start_on('VideoProcessing', 'upload_list')
