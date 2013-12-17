from flow import workflow_client, Workflow, ActivityProxy, make_config


@workflow_client('UploadWorkflow', 3, 'upload_list')
class ProcessUploadedVideoWorkflow(Workflow):

    transcode = ActivityProxy('Transcoding', 1)
    generate_thumbnails = ActivityProxy('ThumbnailGenerator', 1)
    process_metadata = ActivityProxy('MetadataProcessing', 1)
    add_subtitles = ActivityProxy('AddSubtitles', 1)

    def run(self, remote, video_id, source):
        transcodings = remote.transcode(source)
        thumbnails = remote.generate_thumbnails(source)
        status = remote.process_metadata(video_id,
                    transcodings.result(), thumbnails.result()
        )
        subtitles = remote.add_subtitles(video_id)
        return status.result()


if __name__ == '__main__':
    my_config = make_config('RolisTest')

    ParityWF = my_config.workflow_starter('MyParity', 1)
    print 'Starting: ', ParityWF(40)

    my_config.scan()
    my_config.start_workflow_loop(task_list='math_list')
