from unittest import TestCase
from loggers import LoggerFile, handler_stdout
from external.slack import SlackPusher
from datetime import datetime
from time import sleep


class TestBasic(TestCase):

    def test_init(self):
        self.assertTrue(1<2)

    def test_logger_limit(self):
        lg = LoggerFile(name="LoggerFile", level=10, handlers=[handler_stdout], limit_hourly_writes=5)
        count = 0
        for i in range(1, 7):
            lg.error(f'error-{i}')
            count += 1
        self.assertEqual(lg.counter, count-1)
        
    def test_limiter_decorator(self):
        class TestSlackPusher(SlackPusher):
            def __init__(self, interval = 300):
                super().__init__(interval)
                self.count = 0
            @SlackPusher.limiter_decorator
            def test_limit(self, text):
                self.count += 1
                print(text)
            def reset_count(self):
                self.count = 0
                self.last_push = datetime(year=2020, month=1, day=1)

        interval = 2
        n = 3
        sp = TestSlackPusher(interval)
        for i in range(0, n):
            sp.test_limit('aaa')
        self.assertEqual(sp.count, n-2)
        sp.reset_count()
        
        for i in range(0, n):
            sp.test_limit('bbb')
            sleep(interval)
        self.assertEqual(sp.count, n)
