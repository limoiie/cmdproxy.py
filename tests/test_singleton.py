import pytest

from cmdproxy.singleton import Singleton


class Test:
    class A(Singleton):
        def __init__(self, x, y):
            self.x = x
            self.y = y

    def test_singleton(self):
        a = Test.A.instantiate(10, 20)
        assert a.x == 10
        assert a.y == 20

        b = Test.A.instance()
        assert a is b

    def test_new(self):
        with pytest.raises(ValueError):
            Test.A(10, 20)
