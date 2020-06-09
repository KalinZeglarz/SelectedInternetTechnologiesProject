import pandas as pd
from elasticsearch import Elasticsearch, helpers
import numpy as np


class PreselectionServer:
    def __init__(self, address):
        self.es = Elasticsearch(address)

    def index_documents(self, dfUsr):
        df = dfUsr

        means = df.groupby(['userID'], as_index=False, sort=False) \
            .mean() \
            .loc[:, ['userID', 'rating']] \
            .rename(columns={'rating': 'ratingMean'})

        df = pd.merge(df, means, on='userID', how="left", sort=False)
        df['ratingNormal'] = df['rating'] - df['ratingMean']

        ratings = df.loc[:, ['userID', 'movieID', 'ratingNormal']] \
            .rename(columns={'ratingNormal': 'rating'}) \
            .pivot_table(index='userID', columns='movieID', values='rating') \
            .fillna(0)

        print("Indexing users...")
        index_users = [{
            "_index": "users",
            "_type": "user",
            "_id": index,
            "_source": {
                'ratings': row[row > 0] \
                .sort_values(ascending=False) \
                .index.values.tolist()
            }
        } for index, row in ratings.iterrows()]
        helpers.bulk(self.es, index_users)
        print("Done")

        print("Indexing movies...")
        index_movies = [{
            "_index": "movies",
            "_type": "movie",
            "_id": column,
            "_source": {
                "whoRated": ratings[column][ratings[column] > 0] \
                        .sort_values(ascending=False) \
                        .index.values.tolist()
                        }
        } for column in ratings]
        helpers.bulk(self.es, index_movies)
        print("Done")

    def get_movies_liked_by_user(self, user_id, index='users'):
        user_id = int(user_id)
        return self.es.get(index=index, doc_type="user", id=user_id)["_source"]

    def get_users_that_like_movie(self, movie_id, index='movies'):
        movie_id = int(movie_id)
        return self.es.get(index=index, doc_type="movie", id=movie_id)["_source"]

    def get_movie_recommendations(self, user_id, index='users'):
        movies_liked_by_user = self.get_movies_liked_by_user(user_id)
        user_id = int(user_id)

        users_who_rated_at_least_one = self.es.search(index=index, body={
            'query': {
                'terms': movies_liked_by_user
            }
        })["hits"]["hits"]

        unique_movies = set()
        for ratings in users_who_rated_at_least_one:
            if ratings["_id"] != user_id:
                ratings = ratings["_source"]["ratings"]
                for rating in ratings:
                    if rating not in movies_liked_by_user["ratings"]:
                        unique_movies.add(rating)

        return list(unique_movies)

    def get_user_recommendations(self, movie_id, index='movies'):
        users_who_liked_the_movie = self.get_users_that_like_movie(movie_id)
        movie_id = int(movie_id)

        movies_rated_by_at_least_one = self.es.search(index=index, body={
            'query': {
                'terms': users_who_liked_the_movie
            }
        })["hits"]["hits"]

        unique_users = set()
        for ratings in movies_rated_by_at_least_one:
            if ratings["_id"] != movie_id:
                ratings = ratings["_source"]["whoRated"]
                for rating in ratings:
                    if rating not in users_who_liked_the_movie["whoRated"]:
                        unique_users.add(rating)

        return list(unique_users)

    def add_user_document(self, user_id, movies, user_index='users', movie_index='movies'):
        user_id = int(user_id)
        movies = list(set(movies))
        to_update = [self.es.get(index=movie_index, id=movie_id, doc_type='movie') for movie_id in movies]

        if len(to_update) != len(movies):
            raise Exception("One or more movies unknown")

        for movie_document in to_update:
            users = movie_document["_source"]["whoRated"]
            users.append(user_id)
            users = list(set(users))
            self.es.update(index=movie_index, id=movie_document["_id"], doc_type='movie', body={
                "doc": {
                    "whoRated": users
                }
            })

        self.es.create(index=user_index, id=user_id, body={
            "ratings": movies
        },
                       doc_type='user')

    def add_movie_document(self, movie_id, users, user_index='users', movie_index='movies'):
        movie_id = int(movie_id)
        users = list(set(users))
        to_update = [self.es.get(index=user_index, id=user_id, doc_type='user') for user_id in users]

        if len(to_update) != len(users):
            raise Exception("One or more users unknown")

        for user_document in to_update:
            movies = user_document["_source"]["ratings"]
            movies.append(movie_id)
            movies = list(set(movies))
            self.es.update(index=user_index, id=user_document["_id"], doc_type='user', body={
                "doc": {
                    "ratings": movies
                }
            })

        self.es.create(index=movie_index, id=movie_id, body={
            "whoRated": users
        },
                       doc_type='movie')

    def update_user_document(self, user_id, movies, user_index='users', movie_index='movies'):
        user_id = int(user_id)

        movies = list(set(movies))
        to_update = self.es.get(index=user_index, id=user_id, doc_type='user')
        old_movies = to_update['_source']['ratings']

        movies_to_add_user = np.setdiff1d(movies, old_movies)
        movies_to_remove_user = np.setdiff1d(old_movies, movies)

        for movie_to_remove_user in movies_to_remove_user:
            movie_document = self.es.get(index=movie_index, id=movie_to_remove_user, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.remove(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,
                           id=movie_to_remove_user, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        for movie_to_add_user in movies_to_add_user:
            movie_document = self.es.get(index=movie_index, id=movie_to_add_user, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.append(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,
                           id=movie_to_add_user, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        self.es.update(index=user_index, id=user_id,
                       body={"doc": {"ratings": movies}}, doc_type="user")

    def update_movie_document(self, movie_id, users, user_index='users', movie_index='movies'):
        movie_id = int(movie_id)

        users = list(set(users))
        to_update = self.es.get(index=movie_index, id=movie_id, doc_type='movie')
        old_users = to_update['_source']['whoRated']

        users_to_add_movie = np.setdiff1d(users, old_users)
        users_to_remove_movie = np.setdiff1d(old_users, users)

        for user_to_remove_movie in users_to_remove_movie:
            user_document = self.es.get(index=user_index, id=user_to_remove_movie, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.remove(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,
                           id=user_to_remove_movie, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        for user_to_remove_movie in users_to_add_movie:
            user_document = self.es.get(index=user_index, id=user_to_remove_movie, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.append(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,
                           id=user_to_remove_movie, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        self.es.update(index=movie_index, id=movie_id,
                       body={"doc": {"whoRated": users}}, doc_type="movie")

    def delete_user_document(self, user_id, user_index="users", movie_index="movies"):
        user_id = int(user_id)

        to_delete = self.es.get(index=user_index, id=user_id, doc_type='user')["_source"]["ratings"]

        for movie_id_to_update in to_delete:
            movie_document = self.es.get(index=movie_index, id=movie_id_to_update, doc_type='movie')
            users_who_liked_movie = movie_document["_source"]["whoRated"]
            users_who_liked_movie.remove(user_id)
            users_who_liked_movie = list(set(users_who_liked_movie))
            self.es.update(index=movie_index,
                           id=movie_id_to_update, doc_type='movie',
                           body={"doc": {"whoRated": users_who_liked_movie}})

        self.es.delete(index=user_index, id=user_id, doc_type="user")

    def delete_movie_document(self, movie_id, user_index="users", movie_index="movies"):
        movie_id = int(movie_id)

        to_delete = self.es.get(index=movie_index, id=movie_id, doc_type='movie')["_source"]["whoRated"]

        for user_id_to_update in to_delete:
            user_document = self.es.get(index=user_index, id=user_id_to_update, doc_type='user')
            movies_liked_by_user = user_document["_source"]["ratings"]
            movies_liked_by_user.remove(movie_id)
            movies_liked_by_user = list(set(movies_liked_by_user))
            self.es.update(index=user_index,
                           id=user_id_to_update, doc_type='user',
                           body={"doc": {"ratings": movies_liked_by_user}})

        self.es.delete(index=movie_index, id=movie_id, doc_type="movie")

    def bulk_user_update(self, data, user_index='users', movie_index='movies'):
        for item in data:
            self.update_user_document(item["user_id"],
                                      item["liked_movies"], user_index=user_index, movie_index=movie_index)

    def bulk_movie_update(self, data, user_index='users', movie_index='movies'):
        for item in data:
            self.update_movie_document(item["movie_id"],
                                       item["users_who_liked_movie"], user_index=user_index, movie_index=movie_index)

    def reindex(self, source_index, target_index):
        helpers.reindex(self.es, source_index, target_index)

    def delete_index(self, index):
        self.es.indices.delete(index=index, ignore=[400, 404])

    def add_index(self, index):
        request_body = {
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 1
            }
        }
        self.es.indices.create(index=index, body=request_body)

    def get_all_index(self):
        return self.es.indices.get_alias("*")


if __name__ == "__main__":
    ec = PreselectionServer('localhost:1000')
    ec.index_documents()

    # ------ Index operations ------
    print()

    ec.reindex('users', 'test')
    ec.delete_index('test123')
    print(ec.get_all_index())
    ec.add_index('test123')
    print(ec.get_all_index())
