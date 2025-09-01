# app/__init__.py
from flask import Flask

def create_app():
    app = Flask(__name__)

    # Register blueprints (routes)
    try:
        from .views import main_bp
        app.register_blueprint(main_bp)
    except Exception as e:
        # Helpful during setup; you can remove later
        print(f"[init] Failed to register blueprint: {e}")

    return app

__all__ = ["create_app"]