from unittest import TestCase

class TestScanner(TestCase):

    def test_this_module_category_a(self):
        import flowy.tests.scan as t_s
        from flowy.scanner import scan_for

        registry = scan_for(categories=['cat_a'], package=t_s)

        self.assertEquals(
            registry['test1'](d=4, e=5),
            (1, 2, 3, 4, 5, 'test1')
        )
        self.assertEquals(
            registry['test2'](d=40, e=50),
            (10, 20, 30, 40, 50, 'test2')
        )
        self.assertEquals(sorted(registry.keys()), ['test1', 'test2'])

    def test_this_module_category_b(self):
        import flowy.tests.scan as t_s
        from flowy.scanner import scan_for

        registry = scan_for(package=t_s, categories=['cat_b'])

        self.assertEqual(registry['test3'](d=400, e=500),
                         (100, 200, 300, 400, 500, 'test3'))
        self.assertEquals(sorted(registry.keys()), ['test3'])

    def test_caller_package(self):
        import flowy.tests.scan as t_s
        from flowy.scanner import scan_for

        registry = scan_for()
        self.assertEquals(sorted(registry.keys()),
                          ['test1', 'test2', 'test3', 'test4', 'test5'])
