import requests

print("Starting the Bronze pipeline...")
r = requests.get("https://api.openbrewerydb.org/v1/breweries?per_page=20&page=20&sort=id:asc")
print(r.json())