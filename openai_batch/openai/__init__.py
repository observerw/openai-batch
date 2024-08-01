from openai import OpenAI

from .upload import OpenAIFile

openai_client = OpenAI()
openai_file = OpenAIFile(client=openai_client)
