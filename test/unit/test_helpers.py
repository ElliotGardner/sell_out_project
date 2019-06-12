import os
import sys
sys.path.append(os.environ.get('PYTHONPATH'))
import pytest

from sqlalchemy.engine import Engine
import datetime
import json

from src.helpers import helpers



def test_headers():
    # expected results for setting headers with a token
    expectedHeaders = {
        'Authorization': ('Bearer ' + '123456'),
    }

    # assert that the returned object from the set_headers function is a dictionary
    assert isinstance(helpers.set_headers('123456'), dict)

    # assert that the resulting dict from the function is equal to the expected answer
    assert expectedHeaders == helpers.set_headers('123456')

    # assert that if a non-string (an int, for example) is passed to the function, an error will occur
    try:
        helpers.set_headers(123456)
        assert(False)
    except TypeError as e:
        assert(True)


def test_create_db_engine():
    # assert that the results from a good request are an engine instance
    assert isinstance(helpers.create_db_engine('test.db','sqlite'), Engine)

    # assert that if a non-supported type is specified, that a type error is raised
    try:
        helpers.create_db_engine('test.db','postgres')
        assert(False)
    except TypeError as e:
        assert(True)


def test_event_to_event_dict():
    example = '{"name": {"text": "Sounds of Summer \u2013 Havana Night with Pandemonium Steel Band", "html": "Sounds of Summer \u2013 Havana Night with Pandemonium Steel Band"}, "description": {"text": "Enjoy traditional Caribbean music, and themed food and beverage specials, with Pandemonium Steel Band on the Cantigny clubhouse patio.", "html": "Enjoy traditional Caribbean music, and themed food and beverage specials, with Pandemonium Steel Band on the Cantigny clubhouse patio."}, "id": "59111762874", "url": "https://www.eventbrite.com/e/sounds-of-summer-havana-night-with-pandemonium-steel-band-tickets-59111762874?aff=ebapi", "start": {"timezone": "America/Chicago", "local": "2019-06-08T18:00:00", "utc": "2019-06-08T23:00:00Z"}, "end": {"timezone": "America/Chicago", "local": "2019-06-08T21:00:00", "utc": "2019-06-09T02:00:00Z"}, "organization_id": "298709505518", "created": "2019-03-20T14:29:59Z", "changed": "2019-03-20T14:33:35Z", "published": "2019-03-20T14:33:34Z", "capacity": null, "capacity_is_custom": null, "status": "live", "currency": "USD", "listed": true, "shareable": false, "online_event": false, "tx_time_limit": 480, "hide_start_date": false, "hide_end_date": false, "locale": "en_US", "is_locked": false, "privacy_setting": "unlocked", "is_series": false, "is_series_parent": false, "inventory_type": "limited", "is_reserved_seating": false, "show_pick_a_seat": false, "show_seatmap_thumbnail": false, "show_colors_in_seatmap_thumbnail": false, "source": "coyote", "is_free": true, "version": "3.7.0", "summary": "Enjoy traditional Caribbean music, and themed food and beverage specials, with Pandemonium Steel Band on the Cantigny clubhouse patio.", "logo_id": "58811313", "organizer_id": "19827544012", "venue_id": "31002373", "category_id": "103", "subcategory_id": null, "format_id": "6", "resource_uri": "https://www.eventbriteapi.com/v3/events/59111762874/", "is_externally_ticketed": false, "music_properties": {"resource_uri": "https://www.eventbriteapi.com/v3/events/59111762874/music_properties/", "age_restriction": null, "presented_by": null, "door_time": null}, "ticket_availability": {"has_available_tickets": true, "minimum_ticket_price": {"currency": "USD", "value": 0, "major_value": "0.00", "display": "0.00 USD"}, "maximum_ticket_price": {"currency": "USD", "value": 0, "major_value": "0.00", "display": "0.00 USD"}, "is_sold_out": false, "start_sales_date": {"timezone": "America/Chicago", "local": "2019-03-20T00:00:00", "utc": "2019-03-20T05:00:00Z"}, "waitlist_available": false}, "format": {"resource_uri": "https://www.eventbriteapi.com/v3/formats/6/", "id": "6", "name": "Concert or Performance", "name_localized": "Concert or Performance", "short_name": "Performance", "short_name_localized": "Performance"}, "venue": {"address": {"address_1": "27w270 Mack Road", "address_2": null, "city": "Wheaton", "region": "IL", "postal_code": "60189", "country": "US", "latitude": "41.8471004", "longitude": "-88.15528819999997", "localized_address_display": "27w270 Mack Road, Wheaton, IL 60189", "localized_area_display": "Wheaton, IL", "localized_multi_line_address_display": ["27w270 Mack Road", "Wheaton, IL 60189"]}, "resource_uri": "https://www.eventbriteapi.com/v3/venues/31002373/", "id": "31002373", "age_restriction": null, "capacity": null, "name": "Cantigny Golf Course Club House", "latitude": "41.8471004", "longitude": "-88.15528819999997"}, "basic_inventory_info": {"has_ticket_classes": true, "has_inventory_tiers": false, "has_ticket_rules": false, "has_add_ons": false, "has_donations": false}, "bookmark_info": {"bookmarked": false}, "logo": {"crop_mask": {"top_left": {"x": 0, "y": 1446}, "width": 2574, "height": 1287}, "original": {"url": "https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F58811313%2F298709505518%2F1%2Foriginal.20190320-143250?auto=compress&s=8dc615282f4f7a2c83f8da7c734bd2e9", "width": 2574, "height": 3861}, "id": "58811313", "url": "https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F58811313%2F298709505518%2F1%2Foriginal.20190320-143250?h=200&w=450&auto=compress&rect=0%2C1446%2C2574%2C1287&s=05b9340cab33f3561c59212169ec5998", "aspect_ratio": "2", "edge_color": "#172636", "edge_color_set": true}}'

    expected = {'id': '59111762874', 'name': 'Sounds of Summer – Havana Night with Pandemonium Steel Band', 'startDate': datetime.datetime(2019, 6, 8, 18, 0), 'endDate': datetime.datetime(2019, 6, 8, 21, 0), 'publishedDate': datetime.datetime(2019, 3, 20, 14, 33, 34), 'onSaleDate': datetime.datetime(2019, 3, 20, 0, 0),'venueId': 31002373, 'categoryId': 3999, 'formatId': 6, 'inventoryType': 'limited', 'isFree': 1, 'isReservedSeating': 0, 'isAvailable': 1, 'isSoldOut': 0, 'hasWaitList': 0, 'minPrice': 0.0, 'maxPrice': 0.0, 'capacity': 10000, 'ageRestriction': None, 'doorTime': None, 'presentedBy': None, 'isOnline': 0}

    # assert that the result is a dictionary
    assert isinstance(helpers.event_to_event_dict(json.loads(example)), dict)

    # assert that the expected output matches
    assert helpers.event_to_event_dict(json.loads(example)) == expected

    # assert that bad input yields a bad response
    try:
        helpers.event_to_event_dict('1235')
        assert(False)
    except Exception as e:
        assert(True)


def test_event_to_venue_dict():
    example = '{"name": {"text": "Sounds of Summer \u2013 Havana Night with Pandemonium Steel Band", "html": "Sounds of Summer \u2013 Havana Night with Pandemonium Steel Band"}, "description": {"text": "Enjoy traditional Caribbean music, and themed food and beverage specials, with Pandemonium Steel Band on the Cantigny clubhouse patio.", "html": "Enjoy traditional Caribbean music, and themed food and beverage specials, with Pandemonium Steel Band on the Cantigny clubhouse patio."}, "id": "59111762874", "url": "https://www.eventbrite.com/e/sounds-of-summer-havana-night-with-pandemonium-steel-band-tickets-59111762874?aff=ebapi", "start": {"timezone": "America/Chicago", "local": "2019-06-08T18:00:00", "utc": "2019-06-08T23:00:00Z"}, "end": {"timezone": "America/Chicago", "local": "2019-06-08T21:00:00", "utc": "2019-06-09T02:00:00Z"}, "organization_id": "298709505518", "created": "2019-03-20T14:29:59Z", "changed": "2019-03-20T14:33:35Z", "published": "2019-03-20T14:33:34Z", "capacity": null, "capacity_is_custom": null, "status": "live", "currency": "USD", "listed": true, "shareable": false, "online_event": false, "tx_time_limit": 480, "hide_start_date": false, "hide_end_date": false, "locale": "en_US", "is_locked": false, "privacy_setting": "unlocked", "is_series": false, "is_series_parent": false, "inventory_type": "limited", "is_reserved_seating": false, "show_pick_a_seat": false, "show_seatmap_thumbnail": false, "show_colors_in_seatmap_thumbnail": false, "source": "coyote", "is_free": true, "version": "3.7.0", "summary": "Enjoy traditional Caribbean music, and themed food and beverage specials, with Pandemonium Steel Band on the Cantigny clubhouse patio.", "logo_id": "58811313", "organizer_id": "19827544012", "venue_id": "31002373", "category_id": "103", "subcategory_id": null, "format_id": "6", "resource_uri": "https://www.eventbriteapi.com/v3/events/59111762874/", "is_externally_ticketed": false, "music_properties": {"resource_uri": "https://www.eventbriteapi.com/v3/events/59111762874/music_properties/", "age_restriction": null, "presented_by": null, "door_time": null}, "ticket_availability": {"has_available_tickets": true, "minimum_ticket_price": {"currency": "USD", "value": 0, "major_value": "0.00", "display": "0.00 USD"}, "maximum_ticket_price": {"currency": "USD", "value": 0, "major_value": "0.00", "display": "0.00 USD"}, "is_sold_out": false, "start_sales_date": {"timezone": "America/Chicago", "local": "2019-03-20T00:00:00", "utc": "2019-03-20T05:00:00Z"}, "waitlist_available": false}, "format": {"resource_uri": "https://www.eventbriteapi.com/v3/formats/6/", "id": "6", "name": "Concert or Performance", "name_localized": "Concert or Performance", "short_name": "Performance", "short_name_localized": "Performance"}, "venue": {"address": {"address_1": "27w270 Mack Road", "address_2": null, "city": "Wheaton", "region": "IL", "postal_code": "60189", "country": "US", "latitude": "41.8471004", "longitude": "-88.15528819999997", "localized_address_display": "27w270 Mack Road, Wheaton, IL 60189", "localized_area_display": "Wheaton, IL", "localized_multi_line_address_display": ["27w270 Mack Road", "Wheaton, IL 60189"]}, "resource_uri": "https://www.eventbriteapi.com/v3/venues/31002373/", "id": "31002373", "age_restriction": null, "capacity": null, "name": "Cantigny Golf Course Club House", "latitude": "41.8471004", "longitude": "-88.15528819999997"}, "basic_inventory_info": {"has_ticket_classes": true, "has_inventory_tiers": false, "has_ticket_rules": false, "has_add_ons": false, "has_donations": false}, "bookmark_info": {"bookmarked": false}, "logo": {"crop_mask": {"top_left": {"x": 0, "y": 1446}, "width": 2574, "height": 1287}, "original": {"url": "https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F58811313%2F298709505518%2F1%2Foriginal.20190320-143250?auto=compress&s=8dc615282f4f7a2c83f8da7c734bd2e9", "width": 2574, "height": 3861}, "id": "58811313", "url": "https://img.evbuc.com/https%3A%2F%2Fcdn.evbuc.com%2Fimages%2F58811313%2F298709505518%2F1%2Foriginal.20190320-143250?h=200&w=450&auto=compress&rect=0%2C1446%2C2574%2C1287&s=05b9340cab33f3561c59212169ec5998", "aspect_ratio": "2", "edge_color": "#172636", "edge_color_set": true}}'

    expected = {'id': 31002373, 'name': 'Cantigny Golf Course Club House', 'city': 'Wheaton', 'capacity': 10000, 'ageRestriction': None}

    # assert that the result is a dictionary
    assert isinstance(helpers.event_to_venue_dict(json.loads(example)), dict)

    # assert that the expected output matches
    assert helpers.event_to_venue_dict(json.loads(example)) == expected

    # assert that bad input yields a bad response
    try:
        helpers.event_to_venue_dict('1235')
        assert(False)
    except Exception as e:
        assert(True)


