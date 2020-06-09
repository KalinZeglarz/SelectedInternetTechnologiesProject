import json
import pandas as pd
from ProfileStore import ProfileStore
from PreselectionServer import PreselectionServer


class StoresUpdater:
    def __init__(self, cassandraHost, cassandraPort, elasticsearchHost, elasticsearchPort):
        self.dfData: pd.DataFrame
        self.dict_list: list
        self.cassandraClient = ProfileStore(cassandraHost, cassandraPort)
        self.cassandraClient.clear_table()
        self.es = PreselectionServer(elasticsearchHost + ":" + elasticsearchPort)

        if len(self.cassandraClient.get_data_table()) == 0:
            print("Cassandra empty")
            self.dfData = self.load_data_from_files()
            self.push_to_cassandra()
            self.es.index_documents(self.dfData)
            self.dict_list = self.dfData.to_dict(orient='records')
        else:
            self.dfData = dict_list_to_df(self.cassandraClient.get_data_table())
            self.es.index_documents(self.dfData)
            self.dict_list = self.cassandraClient.get_data_table()

    # ------ Interior functions ------
    def df_to_dict_list(self):
        return self.dict_list

    def load_data_from_files(self):
        dfUsr = pd.read_csv("C:\\GitHub\\SelectedInternetTechnologiesProject\\data\\user_ratedmovies.dat", header=0,
                            delimiter="\t", usecols=['userID', 'movieID', 'rating'], nrows=1000)
        dfMov = pd.read_csv("C:\\GitHub\\SelectedInternetTechnologiesProject\\data\\movie_genres.dat",
                            header=0, delimiter="\t")

        dfMov['dummyColumn'] = 1
        dfMovPivoted = dfMov.pivot_table(index="movieID", columns="genre", values="dummyColumn")
        dfMovPivoted = dfMovPivoted.fillna(0)
        dfData = pd.merge(dfUsr, dfMovPivoted, on="movieID")

        return dfData

    def push_to_cassandra(self):
        for index, row in self.dfData.iterrows():
            jsoned = json.loads(row.to_json(orient='columns'))
            self.cassandraClient.push_data_table(index, int(jsoned["userID"]), int(jsoned["movieID"]),
                                                 float(jsoned["rating"]), float(jsoned["Action"]),
                                                 float(jsoned["Adventure"]), float(jsoned["Animation"]),
                                                 float(jsoned["Children"]), float(jsoned["Comedy"]),
                                                 float(jsoned["Crime"]), float(jsoned["Documentary"]),
                                                 float(jsoned["Drama"]), float(jsoned["Fantasy"]),
                                                 float(jsoned["Film-Noir"]), float(jsoned["Horror"]),
                                                 float(jsoned["IMAX"]), float(jsoned["Musical"]),
                                                 float(jsoned["Mystery"]), float(jsoned["Romance"]),
                                                 float(jsoned["Sci-Fi"]), float(jsoned["Short"]),
                                                 float(jsoned["Thriller"]), float(jsoned["War"]),
                                                 float(jsoned["Western"]))

    # ------ Get/Add/Update ------
    def get(self, user_id, movie_id):
        dfRow = self.dfData.loc[(self.dfData['userID'] == user_id) & (self.dfData['movieID'] == movie_id)]
        jsonRow = data_to_json(dfRow)
        return jsonRow

    def user_likes(self, user_id, index):
        self.es.get_movies_liked_by_user(user_id, index)

    def movie_likes(self, movie_id, index):
        self.es.get_users_that_like_movie(movie_id, index)

    def get_indexes(self):
        return self.es.get_all_index()

    def data_to_json(self):
        jsonData = data_to_json(self.dfData)
        return jsonData

    def append_row(self, newRow):
        user_id = int(newRow['userID'])
        movie_id = int(newRow['movieID'])
        movies = [movie_id]
        if (self.dfData.loc[(self.dfData['userID'] == newRow['userID'])]).empty:
            self.es.add_user_document(user_id, movies, 'users', 'movies')
        else:
            self.es.update_user_document(newRow['userID'], [newRow['movieID']], 'users', 'movies')
        self.dfData = self.dfData.append(newRow, ignore_index=True)
        self.push_to_cassandra()
        self.dict_list = self.cassandraClient.get_data_table()

    # ------ Preselection ------
    def movie_preselection(self, user_id, index):
        self.es.get_movie_recommendations(user_id, index)

    def user_preselection(self, movie_id, index):
        self.es.get_user_recommendations(movie_id, index)


def data_to_json(data):
    jsonData = []
    for index, row in data.iterrows():
        jsonData.append(json.loads(row.to_json()))
    return jsonData


def dict_list_to_df(dict_list):
    return pd.DataFrame.from_dict(dict_list, orient="columns")


if __name__ == "__main__":
    su = StoresUpdater('localhost', '9042', 'localhost', '9200')
    print(su.get(75, 3))

