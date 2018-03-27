# coding:utf-8
import pytest

@pytest.fixture(scope='session')
def mainEngine():
    import vtEngine
    return vtEngine.MainEngine()