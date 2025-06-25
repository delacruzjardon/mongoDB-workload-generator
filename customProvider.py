from faker.providers import BaseProvider
import random
import string
from datetime import datetime

class CustomProvider(BaseProvider):
    aircraft_seat_map = {
        "Airbus A320": 170,
        "Boeing 737": 160,
        "CRJ 1000": 100,
        "Embraer E190": 90,
        "Dash 8-400": 85,
        "ATR-72": 75,
        "ERJ-145": 50
    }

    def flight_id(self):
        # Generates a random flight_id (integer)
        return random.randint(0, 9999999)

    def random_string(self, length=5):
        # Helper method to create random uppercase string
        letters = string.ascii_uppercase
        return ''.join(random.choice(letters) for _ in range(length))

    def gate(self):
        # Gate as a letter + number 1-10
        return random.choice(string.ascii_uppercase) + str(random.randint(1, 10))
    
    def ticket_number(self):
        # PNR-like ticket: 10 uppercase letters/digits (e.g., M8Y3KQ)
        return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

    def aircraft_and_seats(self):
        # Select plane_type and calculate seats_available and num_passengers
        plane_type = random.choice(list(self.aircraft_seat_map.keys()))
        total_seats = self.aircraft_seat_map[plane_type]
        num_passengers = random.randint(1, min(70, total_seats))
        seats_available = total_seats - num_passengers
        return plane_type, total_seats, num_passengers, seats_available

    def passengers(self, total_seats, num_passengers, fake):
        # Generate passenger list
        seat_letters = ["A","B","C","D","E","F"]
        passengers_list = []
        for idx in range(1, num_passengers + 1):
            seat_number = str(random.randint(1, total_seats // 3)) + random.choice(seat_letters)
            passengers_list.append({
                "passenger_id": idx,
                "name": fake.name(),
                "seat_number": seat_number,
                "ticket_number": self.ticket_number()
            })
        return passengers_list

    def equip(self, plane_type, total_seats):
        return {
            "plane_type": plane_type,
            "total_seats": total_seats,
            "amenities": ["WiFi", "TV", "Power outlets"]
        }

    def flight_code(self):
        return "FLT-" + str(random.randint(100, 999))

