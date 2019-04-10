# coding:utf-8
import pytest

@pytest.fixture(scope='session')
def mainEngine():
    from . import vtEngine
    return vtEngine.MainEngine()