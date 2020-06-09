# SelectedInternetTechnologiesProject
Poznan University of Technology Studies project of microservice for movie ratings and preselections

### Docker
- Cassandra - `sudo docker run -it --link main_cass:cassandra --rm cassandra:3 cqlsh cassandra`
- Elasticsearch - `sudo docker run -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:6.6.2`

### Endpoints
- `GET` `/rating/<int:user_id>/<int:movie_id>` 
- `GET` `/user/document/<id>` 
- `GET` `/movie/document/<id>`
- `GET` `/index/all`
- `POST` `/rating` 
- `GET` `/user/preselection/<id>`
- `GET` `/movie/preselection/<id>`
