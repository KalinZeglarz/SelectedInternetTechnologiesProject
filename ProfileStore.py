from cassandra.cluster import Cluster


class ProfileStore:
    def __init__(self, cassandraHost, cassandraPort):
        self.keyspace = "user_ratings"
        self.table = "user_ratedmovies"
        self.cluster = Cluster([cassandraHost], port=cassandraPort)
        self.session = self.cluster.connect()

        self.create_keyspace()
        self.create_table()

        self.lastindex = 0

    def create_keyspace(self):
        self.session.execute("""
            CREATE KEYSPACE IF NOT EXISTS """ + self.keyspace + """
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
            """)

    def create_table(self):
        self.session.execute("CREATE TABLE IF NOT EXISTS " + self.keyspace + "." + self.table + "(rowID int, "
                                                                                                "userID int ,"
                                                                                                "movieID int, "
                                                                                                "rating float, "
                                                                                                "Action float, "
                                                                                                "Adventure float, "
                                                                                                "Animation float, "
                                                                                                "Children float, "
                                                                                                "Comedy float, "
                                                                                                "Crime float, "
                                                                                                "Documentary float, "
                                                                                                "Drama float,"
                                                                                                "Fantasy float,"
                                                                                                "FilmNoir float,"
                                                                                                "Horror float,"
                                                                                                "IMAX float,"
                                                                                                "Musical float, "
                                                                                                "Mystery float,"
                                                                                                "Romance float,"
                                                                                                "SciFi float,"
                                                                                                "Short float,"
                                                                                                "Thriller float,"
                                                                                                "War float,"
                                                                                                "Western float, "
                                                                                                "PRIMARY KEY(rowID))")

    def push_data_table(self, rowID, userID, movieID, rating, Action, Adventure, Animation, Children, Comedy, Crime,
                        Documentary, Drama, Fantasy, FilmNoir, Horror, IMAX, Musical, Mystery, Romance, SciFi, Short,
                        Thriller, War, Western):
        self.session.execute(
            "INSERT INTO " + self.keyspace + "." + self.table + "(rowID, userID, movieID, rating, "
                                                                "Action, Adventure, Animation, Children, "
                                                                "Comedy, Crime, Documentary, Drama, "
                                                                "Fantasy, FilmNoir, Horror, IMAX, "
                                                                "Musical, Mystery, Romance, SciFi, "
                                                                "Short, Thriller, War, Western)"
                                                                "VALUES (%(rowID)s, %(userID)s, "
                                                                "%(movieID)s, %(rating)s, "
                                                                "%(Action)s, %(Adventure)s, "
                                                                "%(Animation)s, %(Children)s, "
                                                                "%(Comedy)s, %(Crime)s, "
                                                                "%(Documentary)s, %(Drama)s, "
                                                                "%(Fantasy)s, %(FilmNoir)s, "
                                                                "%(Horror)s, %(IMAX)s, "
                                                                "%(Musical)s, %(Mystery)s, "
                                                                "%(Romance)s, %(SciFi)s, "
                                                                "%(Short)s, %(Thriller)s, "
                                                                "%(War)s, %(Western)s)",
            {
                "rowID": rowID,
                'userID': userID,
                'movieID': movieID,
                'rating': rating,
                'Action': Action,
                'Adventure': Adventure,
                'Animation': Animation,
                'Children': Children,
                'Comedy': Comedy,
                'Crime': Crime,
                'Documentary': Documentary,
                'Drama': Drama,
                'Fantasy': Fantasy,
                'FilmNoir': FilmNoir,
                'Horror': Horror,
                'IMAX': IMAX,
                'Musical': Musical,
                'Mystery': Mystery,
                'Romance': Romance,
                'SciFi': SciFi,
                'Short': Short,
                'Thriller': Thriller,
                'War': War,
                'Western': Western
            }
        )
        self.lastindex = len(self.get_data_table())

    def get_data_table(self):
        rows = self.session.execute("SELECT * FROM "+self.keyspace+"."+self.table+";")
        data = []
        for row in rows:
            data_row = {"userID": row.userid, "movieID": row.movieid, "rating": row.rating, "Action": row.action,
                        "Adventure": row.adventure, "Animation": row.animation, "Children": row.children,
                        "Comedy": row.comedy, "Crime": row.crime, "Documentary": row.documentary, "Drama": row.drama,
                        "Fantasy": row.fantasy, "Film-Noir": row.filmnoir, "Horror": row.horror, "IMAX": row.imax,
                        "Musical": row.musical, "Mystery": row.mystery, "Romance": row.romance, "Sci-Fi": row.scifi,
                        "Short": row.short, "Thriller": row.thriller, "War": row.war, "Western": row.western}
            data.append(data_row)
        return data

    def clear_table(self):
        self.session.execute("TRUNCATE " + self.keyspace + "." + self.table + ";")
        self.lastindex = 0


if __name__ == "__main__":
    cassandraClient = ProfileStore('127.0.0.1', '9042')
    dict = {
        "userID": 73,
        "movieID": 1,
        "rating": 6,
        "Action": 1,
        "Adventure": 1,
        "Animation": 0,
        "Children": 1,
        "Comedy": 1,
        "Crime": 0,
        "Documentary": 0,
        "Drama": 0,
        "Fantasy": 0,
        "Film-Noir": 0,
        "Horror": 0,
        "IMAX": 0,
        "Musical": 0,
        "Mystery": 0,
        "Romance": 1,
        "Sci-Fi": 0,
        "Short": 0,
        "Thriller": 0,
        "War": 0,
        "Western": 0,
    }
    cassandraClient.push_data_table(dict)
    cassandraClient.get_data_table()
