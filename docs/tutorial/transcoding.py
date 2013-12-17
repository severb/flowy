from flowy import Activity, activity_config, make_config


@activity_config('Transcoding', 1, 'transcoding_list')
class Transcoding(Activity):

    def some_transcoding_function(self, source):
        # Business logic goes here, but we're not interested in that
        pass

    def run(self, heartbeat, source_file):
        status = self.some_transcoding_method(source_file)
        return status


if __name__ == '__main__':
    my_config = make_config('RolisTest')
    my_config.scan()
    my_config.start_activity_loop(task_list='transcoding_list')
