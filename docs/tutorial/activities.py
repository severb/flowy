import os
import StringIO
import uuid

from PIL import Image

import requests
from flowy.swf.scanner import activity
from flowy.task import Activity


@activity(name='resize', version=1, task_list='image_processing')
class ResizeImage(Activity):
    def run(self, url, width=128, height=128):
        image_file = self.download_image(url)
        resized_file = self.resize(image_file, width, height)
        destination = self.generate_unique_key(url)
        self.store(resized_file, destination)
        return destination

    def download_image(self, url):
        return StringIO.StringIO(requests.get(url).content)

    def resize(self, image_file, width, height):
        i = Image.open(image_file)
        i.thumbnail((width, height))
        result = StringIO.StringIO()
        i.save(result)
        return result

    def generate_unique_key(self, url):
        basename = os.path.basename(url)
        file_name = '%s-%s' % (uuid.uuid4(), basename)
        return os.path.join('/img-tmp', file_name)

    def store(self, file_object, destination):
        pass
