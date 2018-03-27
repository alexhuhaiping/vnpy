# coding:utf-8
import logging

def test_logging(mainEngine):
    logger = logging.getLogger()
    logger.warning(u'pytest.测试内容')
    mainEngine