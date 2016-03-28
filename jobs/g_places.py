from googleplaces import GooglePlaces, types, lang

YOUR_API_KEY = 'AIzaSyC1D8hoFRIH1Mw_YArJUK2VO9B9LSoxs24'

google_places = GooglePlaces(YOUR_API_KEY)

# You may prefer to use the text_search API, instead.
print google_places.get_place("ChIJ7z5P2ikbdkgRJbLXb5y1uWs")
# print query_result
# if query_result.has_attributions:
#     print query_result.html_attributions

# You may prefer to use the text_search API, instead.
query_result = google_places.nearby_search(
        location='London, England', keyword='Fish and Chips',
        radius=20000, types=[types.TYPE_FOOD])
print  query_result
if query_result.has_attributions:
    print query_result.html_attributions
    
for place in query_result.places:
    # Returned places from a query are place summaries.
    print place.name
    print place.geo_location
    print place.place_id
