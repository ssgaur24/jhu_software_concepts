# Routes (views) grouped in a 'main' blueprint per lecture structure.
from flask import Blueprint, render_template

# Blueprint name 'main' is used in url_for('main.home') etc.
main_bp = Blueprint("main", __name__)

@main_bp.route("/")
# 'page' is passed to set the active tab in the navbar.

# Home page initialization
def home():
    return render_template("home.html", page="home")

# Contact page initialization
@main_bp.route("/contact")
def contact():
    return render_template("contact.html", page="contact")

# Projects page initialization
@main_bp.route("/projects")
def projects():
    return render_template("projects.html", page="projects")
