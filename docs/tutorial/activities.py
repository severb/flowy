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

    def resize(self, image_file, width, height):
        i = Image.open(image_file)
        i.thumbnail((width, height))
        return i

    def prepare_destination(self):
        fd, path = tempfile.mkstemp(prefix='flowy-', dir='/tmp/')
        return os.fdopen(fd, 'w+'), path

    def store(self, image, dest_file):
        image.save(dest_file, format='jpeg')
        dest_file.close()


@activity('predominantcolor', 'v1', 'image_processing', heartbeat=15)
class ComputePredominantColor(Activity):
    def run(self, url):
        image = self.download_image(url)
        if not self.heartbeat():
            return
        return self.sum_colors(image)

    def download_image(self, url):
        f_like = StringIO.StringIO(requests.get(url).content)
        return Image.open(f_like)

    def sum_colors(self, image):
        r_total, g_total, b_total = (0, 0, 0)
        pixels = 1
        for r, g, b in image.getdata():
            r_total += r
            g_total += g
            b_total += b
            pixels += 1
            if pixels % 2**20 == 0:  # about every megapixel
                if not self.heartbeat():
                    return
        return r_total, g_total, b_total


@activity('renameimage', 1, 'image_processing', start_to_close=10)
class RenameImage(Activity):
    def run(self, source, destination):
        os.rename(source, destination)


if __name__ == '__main__':
    from flowy.swf.boilerplate import start_activity_worker
    start_activity_worker(domain='SeversTest', task_list='image_processing')
