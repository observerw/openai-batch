# (WIP) OpenAI Batch

[Batch API Reference](https://platform.openai.com/docs/api-reference/batch)

# Installation

```sh
pip install openai-batch
```

or using poetry:

```sh
poetry add openai-batch
```

# Write your own `Runner` script

## Config

|         Name         |         Type         |                                       Description                                       |
| :------------------: | :------------------: | :-------------------------------------------------------------------------------------: |
|        `name`        |        `str`         |                                    Name of the work.                                    |
| `completion_window`  | `datetime.timedelta` | Time window for the work to be completed. Only support `timedelta(hours=24)` currently. |
|      `endpoint`      |        `str`         |    API endpoint. Only support `/v1/chat/completions` and `/v1/embeddings` currently.    |
| `allow_same_dataset` |        `bool`        |            Whether to allow the same dataset to be processed multiple times.            |
|      `clean_up`      |        `bool`        |                     Whether to clean up the work after completion.                      |

## Methods

- `upload()`: Transform your own dataset into OpenAI chat completions API format. Considering the dataset may be too large to be loaded into memory, it's recommended to use a generator to yield the data.
- `download(output: Iterable[BatchOutputItem])`: Download the API response.
- `download_error(output: Iterable[BatchErrorItem])`: (Optional) Download the errors.

## Example

example usage (`runner.py`):

```python
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)

class Runner(OpenAIBatchRunner):
    work_config = WorkConfig(
        name="example",
        completion_window=timedelta(hours=24),
        endpoint="/v1/chat/completions",
        allow_same_dataset=False,
        clean_up=True,
    )

    @staticmethod
    def upload() -> Iterable[BatchInputItem]:
        with open("data.jsonl", "r", encoding="utf-8") as f:
            # each line is a JSON object like {"id": "1", "content": "Hello!"}
            for line in f:
                data = json.loads(line)
                yield BatchInputItem(   # yield an item
                    id=data["id"],
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a helpful assistant.",
                        },
                        {
                            "role": "user",
                            "content": data["content"],
                        },
                    ],
                )

    @staticmethod
    def download(output: Iterable[BatchOutputItem]):
        with open("result.jsonl", "w", encoding="utf-8") as f:
            for item in output:
                if item.status == "success":
                    f.write(f"{item.model_dump_json()}\n")
                else:
                    logger.error(f"Request {item.id} failed: {item.error}")

    @staticmethod
    def download_error(output: Iterable[BatchErrorItem]):
        with open("errors.jsonl", "a", encoding="utf-8") as f:
            for item in output:
                f.write(f"{item.model_dump_json()}\n")


if __name__ == "__main__":
    Runner.run()
```

In above example:

- You need to inherit `OpenAIBatchRunner` class and define your own Runner class.
- It's recommended to configure logging in your script, since the runner will log information during the process.
- When all the configurations are done, you can run the script through `Runner.run()`.

To run the script, you can simply run:

```sh
python runner.py
```

Then a daemon will be started. Within 24 hours, the daemon will automatically download the results and save them.

 