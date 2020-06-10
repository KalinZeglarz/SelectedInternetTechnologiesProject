import json
import sys

from flask import Flask, request, Response
from StoresUpdater import StoresUpdater

app = Flask(__name__)
su = StoresUpdater('localhost', '9042', 'localhost', '9200')


@app.route('/rating/<int:user_id>/<int:movie_id>', methods=['GET'])
def get(user_id, movie_id):
    try:
        jsonRow = su.get(user_id, movie_id)
        return Response(response=json.dumps(jsonRow, indent=3), status=200, mimetype='application/json')
    except:
        Response(status=404)


@app.route("/user/document/<id>", methods=["GET"])
def get_user_likes(id):
    try:
        index = request.args.get('index', default='users')
        result = su.user_likes(id, index)
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except:
        Response(status=404)


@app.route("/movie/document/<id>", methods=["GET"])
def get_movie_likes(id):
    try:
        index = request.args.get('index', default='movies')
        result = su.movie_likes(id, index)
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except:
        Response(status=404)


@app.route("/index/all", methods=["GET"])
def get_all_index():
    try:
        result = su.get_indexes()
        result = {
            "indexFound": result
        }
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except:
        Response(status=404)


@app.route('/rating', methods=['POST'])
def add_row():
    newRow = request.get_json()
    su.append_row(newRow)
    return Response(response="Ok", status=201, mimetype='application/json')


# ------ Preselection ------
@app.route("/user/preselection/<id>", methods=["GET"])
def use_preselection(id):
    try:
        index = request.args.get('index', default='users')
        result = su.movie_preselection(int(id), index)
        result = {
            "moviesFound": result
        }
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except:
        Response(status=404)


@app.route("/movie/preselection/<id>", methods=["GET"])
def movies_preselection(id):
    try:
        index = request.args.get('index', default='movies')
        result = su.user_preselection(int(id), index)
        result = {
            "usersFound": result
        }
        return Response(response=json.dumps(result), status=200, mimetype='application/json')
    except:
        Response(status=404)


if __name__ == '__main__':
    print(sys.path)
    app.run(
        host='127.0.0.1',
        port=9875
    )
