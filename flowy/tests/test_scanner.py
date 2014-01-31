import unittest


class TestActivityDecorator(unittest.TestCase):

    def _get_uut(self):
        from flowy.scanner import activity
        return activity

    def test_detection(self):
        pass
