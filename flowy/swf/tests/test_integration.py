import os
import unittest


def make(file_name):
    def test(self):
        self.assertTrue(1)
    return test


class ExamplesTest(unittest.TestCase):
    pass


here = os.path.dirname(__file__)

for file_name in os.listdir(os.path.join(here, 'logs')):
    test_name = 'test_' + file_name.rsplit('.', 1)[0]
    print 'adding:', test_name
    setattr(ExamplesTest, test_name, make(file_name))
