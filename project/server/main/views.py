import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from project.server.main.tasks import create_task_harvest, is_valid_issn, chunks, create_task_collect, create_task_enrich
import pandas as pd

default_timeout = 4320000

main_blueprint = Blueprint('main', __name__, )

@main_blueprint.route('/', methods=['GET'])
def home():
    return render_template('home.html')


@main_blueprint.route('/harvest', methods=['POST'])
def run_task_harvest():
    """
    Harvest data
    """
    args = request.get_json(force=True)
    issns = args.get('issns', [])
    if len(issns) == 0:
        df = pd.read_json('/upw_data/issn_l', lines=True)
        issns = [e['journal_issn_l'] for e in df.dropna().to_dict(orient='records') if is_valid_issn(e['journal_issn_l'])]

    issn_chunks = list(chunks(issns, 1000))
    for ix, issn_chunk in enumerate(issn_chunks):
        new_args = args.copy()
        new_args['issns'] = issn_chunk
        new_args['ix'] = ix
        with Connection(redis.from_url(current_app.config['REDIS_URL'])):
            q = Queue(name='harvest-issn', default_timeout=default_timeout)
            task = q.enqueue(create_task_harvest, new_args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/collect', methods=['POST'])
def run_task_collect():
    """
    Collect data
    """
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='harvest-issn', default_timeout=default_timeout)
        task = q.enqueue(create_task_collect, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/enrich', methods=['POST'])
def run_task_enrich():
    """
    Enrich data
    """
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue(name='harvest-issn', default_timeout=default_timeout)
        task = q.enqueue(create_task_enrich, args)
    response_object = {'status': 'success', 'data': {'task_id': task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route('/tasks/<task_id>', methods=['GET'])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config['REDIS_URL'])):
        q = Queue('harvest-issn')
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            'status': 'success',
            'data': {
                'task_id': task.get_id(),
                'task_status': task.get_status(),
                'task_result': task.result,
            }
        }
    else:
        response_object = {'status': 'error'}
    return jsonify(response_object)
