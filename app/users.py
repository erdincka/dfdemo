import json
import httpx
import argparse
import mysql.connector


parser = argparse.ArgumentParser()
parser.add_argument("-s", "--server", default="127.0.0.1")
parser.add_argument("-u", "--user", default="root")
parser.add_argument("-p", "--password", default="Admin123.")
parser.add_argument("-d", "--database", default="demodb")
parser.add_argument("-c", "--count", default=2)

args = parser.parse_args()

URL = f"https://randomuser.me/api/?results={args.count}&format=json&dl&noinfo"

def get_users_from_url():
    res = httpx.get(URL)
    res.raise_for_status()
    users = res.json()
    return users["results"] if "results" in users else []


def get_users_from_file():
    with open("users.json", "r") as f:
        users = json.load(f)
    return users


users = get_users_from_url()

print(f"Connecting to {args.server} with {args.user} for {args.database}")

mydb = mysql.connector.connect(
    host=args.server, user=args.user, password=args.password, database=args.database
)

mycursor = mydb.cursor()
sql = "INSERT INTO users (title, first, last, street, city, state, postcode, country, gender, " \
"email, uuid, username, password, phone, cell, dob, registered, large, medium, thumbnail, nat) " \
"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

vals = []
for user in users:
    # pprint(user)
    vals.append(
        (
            user["name"]["title"],
            user["name"]["first"],
            user["name"]["last"],
            str(user["location"]["street"]["number"])
            + " "
            + user["location"]["street"]["name"],
            user["location"]["city"],
            user["location"]["state"],
            user["location"]["postcode"],
            user["location"]["country"],
            user["gender"],
            user["email"],
            user["login"]["uuid"],
            user["login"]["username"],
            user["login"]["password"],
            user["phone"],
            user["cell"],
            user["dob"]["date"],
            user["registered"]["date"],
            # datetime.fromisoformat(user["dob"]["date"].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
            # datetime.fromisoformat(user["registered"]["date"].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M:%S'),
            user["picture"]["large"],
            user["picture"]["medium"],
            user["picture"]["thumbnail"],
            user["nat"],
        )
    )

# print(vals)

mycursor.executemany(sql, vals)
mydb.commit()

print(mycursor.rowcount, "row(s) inserted.")
