import os

from celery import Celery
from flask import Flask
from flask_moment import Moment
from flask_pagedown import PageDown
import flask_sijax

moment = Moment()

pagedown = PageDown()


def create_app(config_name):
    app = Flask(__name__)
    app.debug = True
    app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


    moment.init_app(app)

    pagedown.init_app(app)
    
    if not app.debug and not app.testing and not app.config['SSL_DISABLE']:
        from flask_sslify import SSLify
        sslify = SSLify(app)


    app.config['SIJAX_STATIC_PATH'] = os.path.join('.', os.path.dirname(__file__), 'static/js/sijax/')
    app.config['SIJAX_JSON_URI'] = '/static/js/sijax/json2.js'
    flask_sijax.Sijax(app)

    return app

dsl = create_app(os.getenv('FLASK_CONFIG') or 'default')
