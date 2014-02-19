import os
import StringIO
import tempfile

from PIL import Image

import requests
from flowy.swf.scanner import activity
from flowy.task import Activity


@activity(name='resize', version=1, task_list='image_processing')
class ResizeImage(Activity):
    def run(self, url, width=128, height=128):
        image_file = self.download_image(url)
        image = self.resize(image_file, width, height)
        dest_file, dest_path = self.prepare_destination()
        self.store(image, dest_file)
        return dest_path

    def download_image(self, url):
        return StringIO.StringIO(requests.get(url).content)

    def resize(self, image_file, width, height, dest_file):
        i = Image.open(image_file)
        i.thumbnail((width, height))
        return i

    def prepare_destination(self):
        fd, path = tempfile.mkstemp(prefix='flowy-', dir='/tmp/')
        return os.fdopen(fd, 'w+'), path

    def store(self, image, dest_file):
        image.save(dest_file, format='jpeg')
        dest_file.close()
