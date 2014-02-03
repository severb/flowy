import unittest

from mock import Mock
from mock import sentinel as s


class TestSingleThreadedWorker(unittest.TestCase):
    def _get_uut(self, client=None):
        from flowy.worker import SingleThreadedWorker
        if client is None:
            client = Mock()
        return SingleThreadedWorker(client), client

    def test_registry(self):
        worker, client = self._get_uut()
        factory = Mock()
        worker.register(s.id, factory)
        self.assertIn(s.id, worker._registry)
        self.assertEquals(worker._registry[s.id], factory)

    def test_polling(self):
        worker, client = self._get_uut()
        client.poll_next_task.return_value = s.result
        result = worker.poll_next_task()
        self.assertEquals(result, s.result)
        client.poll_next_task.assert_called_once_with(worker)

    def test_invalid_task_factory(self):
        worker, client = self._get_uut()

        result = worker.make_task(1, "aaaa", s.sched)
        self.assertEquals(result, None)

    def test_task_factory(self):
        worker, client = self._get_uut()
        factory = Mock()
        worker.register(s.id, factory)
        factory.return_value = s.result

        result = worker.make_task(s.id, s.input, s.sched)

        self.assertEqual(result, s.result)
        factory.assert_called_once_with(input=s.input, scheduler=s.sched)
