# Entry point script required to launch with python run.py
from app import create_app

# Expose the WSGI app object for tools
app = create_app()

if __name__ == "__main__":
    # Host 0.0.0.0 to run locally with the port 8080
    app.run(host="0.0.0.0", port=8080, debug=False)
