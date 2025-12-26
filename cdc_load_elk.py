import json
from elasticsearch import Elasticsearch
from utils import json_sanitize as js
import configparser

def cdc_elk_refresh(message):

    json_str=json.loads(message)

    config = configparser.ConfigParser()
    config.read('config/config.ini')

    elk_host = config.get('Elastic', 'host')
    api_key = config.get('Elastic', 'api_key')
    index = config.get('Elastic', 'index')

    client = Elasticsearch(
    elk_host,
      api_key=api_key,
      verify_certs=False,
      ssl_show_warn=False
    )

    event = json_str.get("event")
    if event =="delete":
        doc_id = json_str["documentKey"]
        cdc_delete_elk(client, doc_id.get("_id"), index)

    elif event in ("insert", "update", "replace"):
        doc = json_str["document"]
        cdc_upsert_elk(client, doc, index)

def cdc_delete_elk(es, doc_id, index):
    #print(f"Deleting document: {doc_id}")
    es.options(ignore_status=[404]).delete(
        index=index,
        id=doc_id
    )

def cdc_upsert_elk(es, doc, index):
    doc_id = doc["_id"]
    #print(f"Upserting document: {doc_id}")
    doc = js.sanitize_doc(doc)

    es.update(
        index=index,
        id=doc_id,
        doc=doc,
        doc_as_upsert=True
    )


"""
doc={"event": "insert", "document": {"_id": "6923ae31cdaac97078b0cd01", "parkId": "B7944940-3FE5-4F9B-80AB-2FD78A4CDD48", "parkCode": "wrst", "parkname": "Wrangell - St Elias", "fullName": "Wrangell - St Elias National Park & Preserve", "url": "https://www.nps.gov/wrst/index.htm", "description": "Wrangell-St. Elias is a vast national park that rises from the ocean all the way up to 18,008 ft. At 13.2 million acres, the park is the same size as Yellowstone National Park, Yosemite National Park, and Switzerland combined! Within this wild landscape, people continue to live off the land as they have done for centuries. This rugged, beautiful land is filled with opportunities for adventure.", "designation": "National Park and Preserve", "latitude": "61.4182147", "longitude": "-142.6028439", "states": "AK", "directionURL": "http://www.nps.gov/wrst/planyourvisit/wrangell-st-elias-visitor-center.htm", "directionInfo": "The administrative building and main park visitor center are located along the Richardson Highway (Hwy 4), which is a paved state highway that runs through Copper Center, AK. The buildings are 8 miles south of the Glenn Highway and Richardson Highway intersection near Glennallen, Alaska. This is approximately 200 miles east of Anchorage, AK and 250 miles south of Fairbanks, AK.", "weatherInfo": "Varies widely depending on location in park and time of year. Visit https://www.nps.gov/wrst/planyourvisit/weather.htm for detailed information.", "country": "USA", "lastUpdatedDate": "2025-12-25T22:17:53.110Z", "activities": [{"id": "01D717BC-18BB-4FE4-95BA-6B13AD702038", "activity": "Snowshoeing"}, {"id": "071BA73C-1D3C-46D4-A53C-00D5602F7F0E", "activity": "Boating"}, {"id": "07CBCA6A-46B8-413F-8B6C-ABEDEBF9853E", "activity": "Canyoneering"}, {"id": "09DF0950-D319-4557-A57E-04CD2F63FF42", "activity": "Arts and Culture"}, {"id": "0B685688-3405-4E2A-ABBA-E3069492EC50", "activity": "Wildlife Watching"}, {"id": "0C0D142F-06B5-4BE1-8B44-491B90F93DEB", "activity": "Park Film"}, {"id": "1DFACD97-1B9C-4F5A-80F2-05593604799E", "activity": "Food"}, {"id": "24380E3F-AD9D-4E38-BF13-C8EEB21893E7", "activity": "Shopping"}, {"id": "4D224BCA-C127-408B-AC75-A51563C42411", "activity": "Paddling"}, {"id": "5F723BAD-7359-48FC-98FA-631592256E35", "activity": "Auto and ATV"}, {"id": "7C912B83-1B1B-4807-9B66-97C12211E48E", "activity": "Snowmobiling"}, {"id": "7CE6E935-F839-4FEC-A63E-052B1DEF39D2", "activity": "Biking"}, {"id": "8386EEAF-985F-4DE8-9037-CCF91975AC94", "activity": "Hunting and Gathering"}, {"id": "A59947B7-3376-49B4-AD02-C0423E08C5F7", "activity": "Camping"}, {"id": "AE42B46C-E4B7-4889-A122-08FE180371AE", "activity": "Fishing"}, {"id": "B12FAAB9-713F-4B38-83E4-A273F5A43C77", "activity": "Climbing"}, {"id": "B33DC9B6-0B7D-4322-BAD7-A13A34C584A3", "activity": "Guided Tours"}, {"id": "BFF8C027-7C8F-480B-A5F8-CD8CE490BFBA", "activity": "Hiking"}, {"id": "C8F98B28-3C10-41AE-AA99-092B3B398C43", "activity": "Museum Exhibits"}, {"id": "D72206E4-6CD1-4441-A355-F8F1827466B1", "activity": "Flying"}, {"id": "DF4A35E0-7983-4A3E-BC47-F37B872B0F25", "activity": "Junior Ranger Program"}], "contactDetails": [{"contactId": "1120", "contact": "+19078225234", "contactTypeId": "2", "contactType": "Voice"}, {"contactId": "1121", "contact": "+19078223281", "contactTypeId": "3", "contactType": "Fax"}, {"contactId": "1122", "contact": "wrst_info@nps.gov", "contactTypeId": "1", "contactType": "Email"}]}}
cdc_elk_refresh(doc)

doc={"event": "insert", "document": {"_id": "6923ae31cdaac97078b0cd01", "parkId": "B7944940-3FE5-4F9B-80AB-2FD78A4CDD48", "parkCode": "wrst", "parkname": "Wrangell - St Elias", "fullName": "Wrangell - St Elias National Park & Preserve", "url": "https://www.nps.gov/wrst/index.htm", "description": "Wrangell-St. Elias is a vast national park that rises from the ocean all the way up to 18,008 ft. At 13.2 million acres, the park is the same size as Yellowstone National Park, Yosemite National Park, and Switzerland combined! Within this wild landscape, people continue to live off the land as they have done for centuries. This rugged, beautiful land is filled with opportunities for adventure.", "designation": "National Park and Preserve", "latitude": "61.4182147", "longitude": "-142.6028439", "states": "AK", "directionURL": "http://www.nps.gov/wrst/planyourvisit/wrangell-st-elias-visitor-center.htm", "directionInfo": "The administrative building and main park visitor center are located along the Richardson Highway (Hwy 4), which is a paved state highway that runs through Copper Center, AK. The buildings are 8 miles south of the Glenn Highway and Richardson Highway intersection near Glennallen, Alaska. This is approximately 200 miles east of Anchorage, AK and 250 miles south of Fairbanks, AK.", "weatherInfo": "Varies widely depending on location in park and time of year. Visit https://www.nps.gov/wrst/planyourvisit/weather.htm for detailed information.", "country": "USA", "lastUpdatedDate": "2025-12-25T22:17:53.110Z", "activities": [{"id": "01D717BC-18BB-4FE4-95BA-6B13AD702038", "activity": "Snowshoeing"}, {"id": "071BA73C-1D3C-46D4-A53C-00D5602F7F0E", "activity": "Boating"}, {"id": "07CBCA6A-46B8-413F-8B6C-ABEDEBF9853E", "activity": "Canyoneering"}, {"id": "09DF0950-D319-4557-A57E-04CD2F63FF42", "activity": "Arts and Culture"}, {"id": "0B685688-3405-4E2A-ABBA-E3069492EC50", "activity": "Wildlife Watching"}, {"id": "0C0D142F-06B5-4BE1-8B44-491B90F93DEB", "activity": "Park Film"}, {"id": "1DFACD97-1B9C-4F5A-80F2-05593604799E", "activity": "Food"}, {"id": "24380E3F-AD9D-4E38-BF13-C8EEB21893E7", "activity": "Shopping"}, {"id": "4D224BCA-C127-408B-AC75-A51563C42411", "activity": "Paddling"}, {"id": "5F723BAD-7359-48FC-98FA-631592256E35", "activity": "Auto and ATV"}, {"id": "7C912B83-1B1B-4807-9B66-97C12211E48E", "activity": "Snowmobiling"}, {"id": "7CE6E935-F839-4FEC-A63E-052B1DEF39D2", "activity": "Biking"}, {"id": "8386EEAF-985F-4DE8-9037-CCF91975AC94", "activity": "Hunting and Gathering"}, {"id": "A59947B7-3376-49B4-AD02-C0423E08C5F7", "activity": "Camping"}, {"id": "AE42B46C-E4B7-4889-A122-08FE180371AE", "activity": "Fishing"}, {"id": "B12FAAB9-713F-4B38-83E4-A273F5A43C77", "activity": "Climbing"}, {"id": "B33DC9B6-0B7D-4322-BAD7-A13A34C584A3", "activity": "Guided Tours"}, {"id": "BFF8C027-7C8F-480B-A5F8-CD8CE490BFBA", "activity": "Hiking"}, {"id": "C8F98B28-3C10-41AE-AA99-092B3B398C43", "activity": "Museum Exhibits"}, {"id": "D72206E4-6CD1-4441-A355-F8F1827466B1", "activity": "Flying"}, {"id": "DF4A35E0-7983-4A3E-BC47-F37B872B0F25", "activity": "Junior Ranger Program"}], "contactDetails": [{"contactId": "1120", "contact": "+19078225234", "contactTypeId": "2", "contactType": "Voice"}, {"contactId": "1121", "contact": "+19078223281", "contactTypeId": "3", "contactType": "Fax"}, {"contactId": "1122", "contact": "wrst_info@nps.gov", "contactTypeId": "1", "contactType": "Email"}]}}
cdc_elk_refresh(doc)

doc={"event": "update", "document": {"_id": "6923ae31cdaac97078b0cb57", "parkId": "3262234F-A56A-447D-AE6C-F5C734ABD3A4", "parkCode": "biho", "parkname": "Big Holes", "fullName": "Big Hole National Battlefield", "url": "https://www.nps.gov/biho/index.htm", "description": "On August 9, 1877, gun shots shattered a chilly dawn on a sleeping camp of Nez Perce. By the time the smoke cleared on August 10, almost 90 Nez Perce were dead along with 31 soldiers and volunteers. Big Hole National Battlefield was created to honor all who were there.", "designation": "National Battlefield", "latitude": "45.64647324", "longitude": "-113.6458443", "states": "MT", "directionURL": "http://www.nps.gov/biho/planyourvisit/directions.htm", "directionInfo": "Big Hole National Battlefield is located on Highway 43 ten miles west of the town of Wisdom in southwestern Montana. Bear Paw Battlefield is located on Route 240 sixteen miles south of the town of Chinook in north-central Montana.", "weatherInfo": "This climatic region is typified by large seasonal temperature differences, with warm to hot (and often humid) summers and cold (sometimes severely cold) winters. Wisdom has a humid continental climate.", "country": "USA", "lastUpdatedDate": "2025-11-27T23:56:33.486Z", "activities": [{"id": "01D717BC-18BB-4FE4-95BA-6B13AD702038", "activity": "Snowshoeing"}, {"id": "09DF0950-D319-4557-A57E-04CD2F63FF42", "activity": "Arts and Culture"}, {"id": "0B685688-3405-4E2A-ABBA-E3069492EC50", "activity": "Wildlife Watching"}, {"id": "0C0D142F-06B5-4BE1-8B44-491B90F93DEB", "activity": "Park Film"}, {"id": "1DFACD97-1B9C-4F5A-80F2-05593604799E", "activity": "Food"}, {"id": "24380E3F-AD9D-4E38-BF13-C8EEB21893E7", "activity": "Shopping"}, {"id": "AE42B46C-E4B7-4889-A122-08FE180371AE", "activity": "Fishing"}, {"id": "B33DC9B6-0B7D-4322-BAD7-A13A34C584A3", "activity": "Guided Tours"}, {"id": "BFF8C027-7C8F-480B-A5F8-CD8CE490BFBA", "activity": "Hiking"}, {"id": "C8F98B28-3C10-41AE-AA99-092B3B398C43", "activity": "Museum Exhibits"}, {"id": "DF4A35E0-7983-4A3E-BC47-F37B872B0F25", "activity": "Junior Ranger Program"}, {"id": "F9B1D433-6B86-4804-AED7-B50A519A3B7C", "activity": "Skiing"}], "contactDetails": [{"contactId": "89", "contact": "+14066893155", "contactTypeId": "2", "contactType": "Voice"}, {"contactId": "90", "contact": "biho_visitor_information@nps.gov", "contactTypeId": "1", "contactType": "Email"}]}, "updateDescription": {"updatedFields": {"parkname": "Big Holes"}, "removedFields": [], "truncatedArrays": []}}
cdc_elk_refresh(doc)
"""
