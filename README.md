# OpenAI Batch API

example usage (`runner.py`):

```python
class Runner(OpenAIBatchRunner):
    @staticmethod
    def upload() -> Iterable[BatchInputItem]:
        with open("data.jsonl", "r", encoding="utf-8") as f:
            # each line is a JSON object like {"id": "1", "content": "Hello!"}
            for line in f:
                data = json.loads(line)
                yield BatchInputItem(
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
                    logging.error(f"Request {item.id} failed: {item.error}")

    @staticmethod
    def download_error(output: Iterable[BatchErrorItem]):
        with open("errors.jsonl", "a", encoding="utf-8") as f:
            for item in output:
                f.write(f"{item.model_dump_json()}\n")


if __name__ == "__main__":
    Runner().cli()
```

then:

```sh
python runner.py
```

A daemon will be started. Within 24 hours, the daemon will automatically download the results and save them.