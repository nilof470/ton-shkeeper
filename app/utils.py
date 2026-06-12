from decimal import Decimal
from functools import wraps

from werkzeug.routing import BaseConverter

from .logging import logger



class DecimalConverter(BaseConverter):

    def to_python(self, value):
        return Decimal(value)

    def to_url(self, value):
        return BaseConverter.to_url(value)


def skip_if_running(f):
    task_name = f'{f.__module__}.{f.__name__}'

    def dedupe_key(args, kwargs):
        normalized_args = tuple(args)
        normalized_kwargs = dict(kwargs)
        if task_name.endswith(".drain_account"):
            normalized_args = normalized_args[:2]
            normalized_kwargs.pop("txid", None)
        return normalized_args, normalized_kwargs

    @wraps(f)
    def wrapped(self, *args, **kwargs):
        workers = self.app.control.inspect().active()
        current_args, current_kwargs = dedupe_key(args, kwargs)

        for worker, tasks in workers.items():
            for task in tasks:
                task_args, task_kwargs = dedupe_key(
                    tuple(task['args']), task['kwargs']
                )
                if (task_name == task['name'] and
                        current_args == task_args and
                        current_kwargs == task_kwargs and
                        self.request.id != task['id']):
                    logger.debug(f'task {task_name} ({args}, {kwargs}) is running on {worker}, skipping')

                    return None
        logger.debug(f'task {task_name} ({args}, {kwargs}) is allowed to run')
        return f(self, *args, **kwargs)

    return wrapped
