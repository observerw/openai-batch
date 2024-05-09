import datetime

import pytest

from openai_batch.const import MAX_FILE_SIZE
from openai_batch.model import BatchInputItem, WorkConfig
from openai_batch.worker import transform

config = WorkConfig(
    name="test",
    completion_window=datetime.timedelta(hours=24),
    endpoint="/v1/chat/completions",
    allow_same_dataset=False,
    clean_up=True,
)


class TestWorker:
    @pytest.mark.parametrize("batch_input_num", [1919, 810])
    def test_transform(self, batch_input_num):
        batch_input = iter(
            [
                BatchInputItem(
                    id=str(id),
                    messages=[
                        {
                            "role": "user",
                            "content": f"example content{id}",
                        },
                    ],
                    user="user123",
                )
                for id in range(batch_input_num)
            ]
        )
        result = transform(config, batch_input)
        assert sum([len(f.readlines()) for f in result.files]) == batch_input_num
        for f in result.files:
            assert 0 < f.tell() <= MAX_FILE_SIZE
