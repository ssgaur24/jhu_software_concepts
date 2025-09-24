import pytest
from src.flask_app import create_app


@pytest.fixture()
def app():
    """Create test Flask app"""
    return create_app({"TESTING": True})


@pytest.fixture()
def client(app):
    """Create test client"""
    return app.test_client()