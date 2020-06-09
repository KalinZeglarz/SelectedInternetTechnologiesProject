import json

from flask import Flask, jsonify, abort, request, Response
from StoresUpdater import StoresUpdater

app = Flask(__name__)
su = StoresUpdater(20, 'localhost', '9042', 'localhost', '1000')


@app.route('/rating/<int:user_id>/<int:movie_id>', methods=['GET'])
def get(user_id, movie_id):
    try:
        jsonRow = su.get(user_id, movie_id)
        return Response(response=json.dumps(jsonRow, indent=3), status=200, mimetype='application/json')
    except:
        abort(404)


@app.route("/user/document/<id>", methods=["GET"])
def get_user_likes(id):
    try:
        index = request.args.get('index', default='users')
        result = su.user_likes(id, index)
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/document/<id>", methods=["GET"])
def get_movie_likes(id):
    try:
        index = request.args.get('index', default='movies')
        result = su.movie_likes(id, index)
        return jsonify(result)
    except:
        abort(404)


@app.route("/index/all", methods=["GET"])
def get_all_index():
    try:
        result = su.get_indexes()
        result = {
            "indexFound": result
        }
        return jsonify(result)
    except:
        abort(404)


@app.route('/rating', methods=['POST'])
def add_row(user_id):
    newRow = request.get_json()
    su.append_row(newRow)
    return Response(status=201, mimetype='application/json')


# ------ Preselection ------
@app.route("/user/preselection/<id>", methods=["GET"])
def use_preselection(id):
    try:
        index = request.args.get('index', default='users')
        result = su.movie_preselection(int(id), index)
        result = {
            "moviesFound": result
        }
        return jsonify(result)
    except:
        abort(404)


@app.route("/movie/preselection/<id>", methods=["GET"])
def movies_preselection(id):
    try:
        index = request.args.get('index', default='movies')
        result = su.user_preselection(int(id), index)
        result = {
            "usersFound": result
        }
        return jsonify(result)
    except:
        abort(404)


if __name__ == '__main__':
    app.run(
        host='127.0.0.1',
        port=9875
    )
