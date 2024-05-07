import pytest

from openai_batch.const import MAX_FILE_SIZE
from openai_batch.model import Config, BatchInputItem
from openai_batch.worker import transform

config = Config(
    exit_on_duplicate=True
)

class TestWorker:
    @pytest.mark.parametrize("batch_input_num", [1919, 810])
    def test_transform(self, batch_input_num):
        batch_input = iter([
            BatchInputItem(
                id=str(id),
                messages=[
                    {
                        "role": "user",
                        "content": f"example content{id}",
                    },
                ],
                user="user123"
            )
            for id in range(batch_input_num)
        ])
        result = transform(config, batch_input)
        assert sum([len(f.readlines()) for f in result.files]) == batch_input_num
        for f in result.files:
            assert 0 < f.tell() <= MAX_FILE_SIZE
