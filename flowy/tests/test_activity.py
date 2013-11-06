import unittest


class TestActivity(unittest.TestCase):

    def _get_uut(self):
        from flowy.activity import Activity
        return Activity

    def test_activity_input_deserialization(self):
        uut = self._get_uut()
        result = uut.serialize_activity_result([1, 'a'])
        self.assertEquals('[1, "a"]', result)

    def test_activity_result_serialization(self):
        uut = self._get_uut()
        result = uut.deserialize_activity_input(
            '{"args": [1, "a"], "kwargs": {"a": 1, "b": "b"}}'
        )
        self.assertEquals(([1, "a"], {"a": 1, "b": "b"}), result)
