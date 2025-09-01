# Flask application factory (lecture pattern):
# Creates and returns the app, then registers blueprints.
from flask import Flask

def create_app():
    app = Flask(__name__)

    # Routes are organized as a blueprint in app/views.py.
    try:
        from .views import main_bp
        app.register_blueprint(main_bp)
    except Exception as e:
        print(f"[init] Failed to register blueprint: {e}")

    return app

__all__ = ["create_app"]