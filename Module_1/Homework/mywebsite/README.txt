How to run
1) Create venv:    py -3.13 -m venv .venv
2) Activate:       .venv\Scripts\activate   (Git Bash: source .venv/Scripts/activate)
3) Install deps:   pip install -r requirements.txt
4) a: Launch:         python run.py
   b: If running on pycharm, set environment variables - FLASK_APP=run:app;FLASK_RUN_HOST=0.0.0.0;FLASK_RUN_PORT=8080;FLASK_DEBUG=0
     Set script as run.py
   - Expected: app listens on http://localhost:8080