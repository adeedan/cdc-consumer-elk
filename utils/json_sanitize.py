def sanitize_doc(doc):
    ALLOWED_FIELDS = {
        "parkId",
        "parkCode",
        "parkname",
        "fullName",
        "url",
        "description",
        "designation",
        "latitude",
        "longitude",
        "states",
        "directionURL",
        "directionInfo",
        "weatherInfo",
        "country",
        "lastUpdatedDate",
        "activities",
        "contactDetails"
    }

    clean = {}
    for key, value in doc.items():
        if key in ALLOWED_FIELDS:
            clean[key] = value
    return clean