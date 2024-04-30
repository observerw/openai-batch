# OpenAI Batch API

example usage (`runner.py`):

```python
from opena_batch_api import OpenAIBatchAPIRunner
from typing import override

class Runner(OpenAIBatchAPIRunner):
    @staticmethod
    @override
    def download(items: Iterable[BatchOutputItem]):
        for item in items:
            ...

    @staticmethod
    @override
    def upload() -> Iterable[BatchInputItem]:
        with open("input.jsonl", "r") as f:
            data = f.readline()
            data = json.loads(data)
            ...

if __name__ == "__main__":
    runner = Runner()
    runner.cli()
```

then:

```sh
python runner.py run
```

A daemon will be started. After 24 hours, the daemon will automatically download the results and save them.