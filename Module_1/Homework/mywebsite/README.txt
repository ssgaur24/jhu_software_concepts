Project Location
- Please evaluate this project at github path:
  jhu_software_concepts\Module_1\Homework\mywebsite
  https://github.com/ssgaur24/jhu_software_concepts/tree/main/Module_1/Homework/mywebsite

How to Run
1) Create venv in project root:
   - Git Bash (Windows):  python -m venv .venv && source .venv/Scripts/activate
   - PowerShell:          py -3.13 -m venv .venv && .\.venv\Scripts\Activate.ps1
2) Install deps:          pip install -r requirements.txt
3) Launch:                python run.py
   Expected: http://localhost:8080

How to Run in PyCharm
    Python Run Configuration
   - Script path:        <project>\run.py
   - Working directory:  <project>
   - Interpreter:        <project>\.venv\Scripts\python.exe
    - Environment vars:   FLASK_APP=run:app;FLASK_RUN_HOST=0.0.0.0;FLASK_RUN_PORT=8080;FLASK_DEBUG=0
