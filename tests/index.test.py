import pytest
from main import Config, Client
def test_config(): cfg = Config(); assert cfg["base_url"] == "https://api.example.com"
def test_client(): c = Client(Config()); r = c.run(); assert r["status"] == "success"
