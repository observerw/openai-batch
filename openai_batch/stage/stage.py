from ..db import schema


def create():
    raise NotImplementedError()


def start(work: schema.Work):
    raise NotImplementedError()


def running(work: schema.Work):
    raise NotImplementedError()


def end(work: schema.Work):
    raise NotImplementedError()
