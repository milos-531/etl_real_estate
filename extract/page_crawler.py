css_selectors = {
    "title": "#plh1",
    "type": "#plh10",
    "area": "#plh11",
    "num_rooms": "#plh12",
    "posted_by": "#plh13",
    "age": "#plh14",
    "condition": "#plh15",
    "equipment": "#plh16",
    "heating": "#plh17",
    "floor": "#plh18",
    "total_floors": "#plh19",
    "utilities_cost": "#plh20",
    "payment_period": "#plh21",
    "rent_cost": ".offer-price-value",
    "rent_currency": ".offer-price-unit",
    "city": "#plh2",
    "location": "#plh3",
    "micro_location": "#plh4",
    "street": "#plh5",
    "description": "#plh52",
    "deposit": "#plh26",
    "not_in_house": "#plh25",
    "pet_friendly": "#plh31",
    "immediately_available": "#plh23",
    "balcony": "#plh36",
    "air_conditioning": "#plh39",
    "garrage": "#plh49",
    "elevator": "#plh40",
    "internet": "#plh45",
    "intercom": "#plh46",
    "video_surveillance": "#plh47",
    "parking": "#plh50",
    "air_conditioning": "#plh39",
    "telephone": "#plh43",
    "cable_tv": "#plh44",
    "not_final_floor": "#plh24",
    "no_smoking": "#plh28",
    "allowed_smoking": "#plh27",
    "for_students": "#plh29",
    "basement": "#plh41",
    "garden": "#plh51",
    "date_posted": "#plh82",
}


def get_text_data(response, output):
    fields = [
        "title",
        "type",
        "area",
        "num_rooms",
        "posted_by",
        "age",
        "condition",
        "equipment",
        "heating",
        "floor",
        "total_floors",
        "utilities_cost",
        "payment_period",
        "rent_cost",
        "rent_currency",
        "city",
        "location",
        "micro_location",
        "street",
        "date_posted",
    ]
    for field in fields:
        output[field] = get_text(response, field)

    return output


def get_bool_data(response, output):
    fields = [
        "deposit",
        "not_in_house",
        "pet_friendly",
        "immediately_available",
        "balcony",
        "air_conditioning",
        "garrage",
        "elevator",
        "internet",
        "intercom",
        "video_surveillance",
        "parking",
        "telephone",
        "cable_tv",
        "not_final_floor",
        "no_smoking",
        "allowed_smoking",
        "for_students",
        "basement",
        "garden",
    ]
    for field in fields:
        output[field] = get_bool(response, field)

    return output


def get_description(response):
    description_selector = css_selectors["description"]
    description_paragraphs = response.css(description_selector + " *::text").getall()
    description = " ".join(description_paragraphs)
    return description


def parse_page(response):
    output = {}
    output = get_text_data(response, output)
    output = get_bool_data(response, output)
    output["description"] = get_description(response)
    output["url"] = response.url
    return output


def get_text(response, item):
    item_selector = css_selectors[item]
    item_text = response.css(item_selector + "::text").get()
    return item_text


def get_bool(response, item):
    item_selector = css_selectors[item]
    item = response.css(item_selector).get()
    item_exists = bool(item)
    return item_exists
