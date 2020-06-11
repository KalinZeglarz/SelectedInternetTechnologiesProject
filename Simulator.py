import json
import random


class Simulator:
    @staticmethod
    def generate_random_user_rating(unique_movies):
        newRow = {
            "userID": random.randint(1, 2000),
            "movieID": unique_movies[random.randint(0, (len(unique_movies) - 1))],
            "rating": (random.randint(1, 10) / 2),
            "Action": random.randint(0, 1),
            "Adventure": random.randint(0, 1),
            "Animation": random.randint(0, 1),
            "Children": random.randint(0, 1),
            "Comedy": random.randint(0, 1),
            "Crime": random.randint(0, 1),
            "Documentary": random.randint(0, 1),
            "Drama": random.randint(0, 1),
            "Fantasy": random.randint(0, 1),
            "Film-Noir": random.randint(0, 1),
            "Horror": random.randint(0, 1),
            "IMAX": random.randint(0, 1),
            "Musical": random.randint(0, 1),
            "Mystery": random.randint(0, 1),
            "Romance": random.randint(0, 1),
            "Sci-Fi": random.randint(0, 1),
            "Short": random.randint(0, 1),
            "Thriller": random.randint(0, 1),
            "War": random.randint(0, 1),
            "Western": random.randint(0, 1),
        }
        return newRow
