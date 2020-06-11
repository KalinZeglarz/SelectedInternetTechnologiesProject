# SelectedInternetTechnologiesProject
Poznan University of Technology Studies project of microservice for movie ratings and preselections

You can use API_client.py to test endpoints

### Docker
- Cassandra - `sudo docker run -it --link main_cass:cassandra --rm cassandra:3 cqlsh cassandra`
- Elasticsearch - `sudo docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.6.2`

### Endpoints
- `POST` `/rating`                                    - add rating to database (ex. of body below)
- `GET` `/rating/<int:user_id>/<int:movie_id>`        - single rating (userID, movieID, rating, [genres of movie])
- `GET` `/user/document/<id>`                         - all movies rated by user
- `GET` `/movie/document/<id>`                        - all users that rated movie
- `GET` `/index/all`                                  - all Elasticsearch indexes
- `GET` `/user/preselection/<id>`                     - movies preselected for user
- `GET` `/movie/preselection/<id>`                    - users preselected for movie
- `GET` `/generator`                                  - generates random rating and adds it to database

### POST body ex.
```
{
      "userID": 75.0,
      "movieID": 2.0,
      "rating": 1.0,
      "Action": 0.0,
      "Adventure": 0.0,
      "Animation": 0.0,
      "Children": 0.0,
      "Comedy": 1.0,
      "Crime": 0.0,
      "Documentary": 0.0,
      "Drama": 0.0,
      "Fantasy": 0.0,
      "Film-Noir": 0.0,
      "Horror": 0.0,
      "IMAX": 0.0,
      "Musical": 0.0,
      "Mystery": 0.0,
      "Romance": 1.0,
      "Sci-Fi": 0.0,
      "Short": 0.0,
      "Thriller": 0.0,
      "War": 0.0,
      "Western": 0.0
   }
```
   
   
