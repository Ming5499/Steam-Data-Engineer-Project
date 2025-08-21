import requests, csv
apps = requests.get("https://api.steampowered.com/ISteamApps/GetAppList/v2/").json()["applist"]["apps"]
with open("steam_appid_name.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["appid","name"])
    for a in apps:
        w.writerow([a.get("appid",""), a.get("name","")])
