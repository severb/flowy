from flowy.swf.scanner import workflow
from flowy.swf.task import ActivityProxy
from flowy.task import Workflow


@workflow(name='imagecateg', version=1, task_list='my_workflows')
class ImageCategorization(Workflow):
    """ Download, resize and categorize images based on color. """

    resize = ActivityProxy(name='resize', version=1)
    sum_colors = ActivityProxy(name='predominantcolor', version='v1')
    rename = ActivityProxy('renameimage', 1, start_to_close=5)

    def run(self, url):
        tmp_path = self.resize(url, width=256, height=256)
        colors = self.sum_colors(url)
        r, g, b = colors.result()
        if max(r, g, b) == r:
            dest_path = '/tmp/r.jpeg'
        elif max(r, g, b) == g:
            dest_path = '/tmp/g.jpeg'
        else:
            dest_path = '/tmp/b.jpeg'
        self.rename(tmp_path, dest_path)
        return dest_path


if __name__ == '__main__':
    from flowy.swf.boilerplate import start_workflow_worker
    start_workflow_worker(domain='SeversTest', task_list='my_workflows')
