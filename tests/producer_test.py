
# import sys
# import os

# Add the src directory to the Python path. I don't think I actually need this.
# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

import unittest
from unittest.mock import patch, MagicMock
import time
from src.producer import format_data, run_kafka_producer

get_random_user_return_value = {
            'gender': 'male',
            'name': {'title': 'Mr', 'first': 'Charlie', 'last': 'Lopez'},
            'location': {
                'street': {'number': 6068, 'name': 'Rue Denfert-Rochereau'},
                'city': 'Toulon',
                'state': 'Maine-et-Loire',
                'country': 'France',
                'postcode': 77255,
                'coordinates': {'latitude': '-16.4120', 'longitude': '-34.9002'},
                'timezone': {'offset': '+4:00', 'description': 'Abu Dhabi, Muscat, Baku, Tbilisi'}
            },
            'email': 'charlie.lopez@example.com',
            'login': {'uuid': '2999ae9b-8cfa-4785-9714-232712a44672'},
            'dob': {'date': '1955-05-15T19:08:12.996Z', 'age': 69},
            'registered': {'date': '2005-06-26T19:37:47.991Z', 'age': 19},
            'phone': '01-16-32-34-16',
            'cell': '06-53-78-57-70',
            'id': {'name': 'INSEE', 'value': '1550421276800 88'},
            'picture': {
                'large': 'https://randomuser.me/api/portraits/men/49.jpg',
                'medium': 'https://randomuser.me/api/portraits/med/men/49.jpg',
                'thumbnail': 'https://randomuser.me/api/portraits/thumb/men/49.jpg'
            },
            'nat': 'FR'
        }

format_data_return_value = {
            'id': '2999ae9b-8cfa-4785-9714-232712a44672',
            'first_name': 'Charlie',
            'last_name': 'Lopez',
            'gender': 'male',
            'address': '6068 Rue Denfert-Rochereau',
            'city': 'Toulon',
            'state': 'Maine-et-Loire',
            'country': 'France',
            'postcode': 77255,
            'email': 'charlie.lopez@example.com',
            'dob': '1955-05-15T19:08:12.996Z',
            'age': 69,
            'registered_date': '2005-06-26T19:37:47.991Z',
            'phone': '01-16-32-34-16',
            'picture': 'https://randomuser.me/api/portraits/med/men/49.jpg'
        }

class TestFormatData(unittest.TestCase):
    def setUp(self):
        # Example input and expected output
        self.input_data = get_random_user_return_value

        self.expected_output = format_data_return_value

    def test_format_data(self):
        # Allow full diff to be displayed
        self.maxDiff = None
        
        # Call the function with the input data
        result = format_data(self.input_data)

        # Assert that the result matches the expected output
        self.assertEqual(result, self.expected_output)

class TestKafkaProducer(unittest.TestCase):
    @patch('src.producer.KafkaProducer')
    @patch('src.producer.get_random_user')
    @patch('src.producer.format_data')
    def test_run_kafka_producer(self, mock_format_data, mock_get_random_user, mock_kafka_producer):
        # Mock the KafkaProducer
        mock_producer_instance = MagicMock()
        mock_kafka_producer.return_value = mock_producer_instance

        # Mock get_random_user and format_data
        mock_get_random_user.return_value = get_random_user_return_value
        mock_format_data.return_value = format_data_return_value

        # Run the producer
        run_kafka_producer()

        # Assertions
        mock_get_random_user.assert_called()  # Ensure data was fetched
        mock_format_data.assert_called_with(get_random_user_return_value)  # Ensure format_data was called with correct data
        mock_producer_instance.send.assert_called()  # Ensure messages were sent
        mock_producer_instance.flush.assert_called_once()  # Ensure flush was called
        mock_producer_instance.close.assert_called_once()  # Ensure producer was closed


if __name__ == '__main__':
    unittest.main()