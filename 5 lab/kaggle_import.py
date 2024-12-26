import time

start_time = time.time()

credentials = {
    'host': 'localhost',
    'database': 'football',
    'user': 'Onyshchenko',
    'password': 'Onyshchenko',
    'port': '5432',
}

import pandas as pd

def read_csv():
    inputted = pd.read_csv("data.csv")
    inputted.dropna(inplace=True)
    inputted.reset_index(inplace=True)
    inputted.drop(['index', 'xG Per Avg Match', 'Shots Per Avg Match', 'On Target Per Avg Match'], axis=1, inplace=True)
    inputted.reset_index(inplace=True)
    inputted.rename(inplace=True,columns={'index': 'id', 'Player Names': 'name', 'League': 'league_name', 'Substitution ': 'Substitution'})
    return inputted

import psycopg2

def db_connect():
    return psycopg2.connect(**credentials)

def create_league_table(connection):
    with connection:
        curs = connection.cursor()
        curs.execute("DROP TABLE IF EXISTS league CASCADE;")
        curs.execute("""
CREATE TABLE league
(
  league_name VARCHAR NOT NULL,
  league_id SERIAL NOT NULL,
  PRIMARY KEY (league_id)
);""")

def create_club_table(connection):
    with connection:
        curs = connection.cursor()
        curs.execute("DROP TABLE IF EXISTS club CASCADE;")
        curs.execute("""
CREATE TABLE club
(
  Name VARCHAR NOT NULL,
  Country VARCHAR NOT NULL,
  club_id SERIAL NOT NULL,
  league_id INT NOT NULL,
  PRIMARY KEY (club_id),
  FOREIGN KEY (league_id) REFERENCES league(league_id)
);""")

def create_Player_table(connection):
    with connection:
        curs = connection.cursor()
        curs.execute("DROP TABLE IF EXISTS Player CASCADE;")
        curs.execute("""
CREATE TABLE Player
(
  Name VARCHAR NOT NULL,
  player_id SERIAL NOT NULL,
  club_id INT NOT NULL,
  PRIMARY KEY (player_id),
  FOREIGN KEY (club_id) REFERENCES club(club_id)
);""")

def create_Stats_table(connection):
    with connection:
        curs = connection.cursor()
        curs.execute("DROP TABLE IF EXISTS Stats CASCADE;")
        curs.execute("""
CREATE TABLE Stats
(
  year INT NOT NULL,
  Matches_Played INT NOT NULL,
  Substitution INT NOT NULL,
  Mins INT NOT NULL,
  Goals INT NOT NULL,
  xG FLOAT NOT NULL,
  Shots INT NOT NULL,
  OnTarget INT NOT NULL,
  id SERIAL NOT NULL,
  player_id INT NOT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (player_id) REFERENCES Player(player_id)
);""")

def insert_league(curs, league_name):
    curs.execute(
        "SELECT COUNT(*) FROM league WHERE league_name = '{}'".format(league_name.replace("'", '')))
    count = curs.fetchone()[0]

    if count == 0:
        curs.execute("SELECT COUNT(*) FROM league")
        count = curs.fetchone()[0]
        curs.execute(
            "INSERT INTO league (league_id, league_name) VALUES (%s, %s)",
            (count, league_name))

def insert_club(curs, country, name, league_name):
    curs.execute("SELECT COUNT(*) FROM club WHERE country = %s AND name = %s", (country, name))
    count = curs.fetchone()[0]
    if count == 0:
        curs.execute("SELECT league_id FROM league WHERE league_name = '{}'".format(league_name))
        league_id = curs.fetchone()[0]
        curs.execute("SELECT COUNT(*) FROM club")
        count = curs.fetchone()[0]
        curs.execute(
            "INSERT INTO club (club_id, country, name, league_id) VALUES (%s, %s, %s, %s)",
            (count, country, name, league_id))

def insert_Stats(curs,id, player_name, Year, Matches_Played, Substitution, Mins, Goals, xG, Shots, OnTarget):
    curs.execute("SELECT player_id FROM Player WHERE Name = '{}'".format(player_name.replace("'",'')))
    player_id = curs.fetchone()[0]
    curs.execute("SELECT COUNT(*) FROM Stats WHERE player_id = %s AND year = %s",
                (player_id, Year))
    count = curs.fetchone()[0]
    if count == 0:
        curs.execute("INSERT INTO Stats (id, Year, Matches_Played, Substitution, Mins, Goals, xG, Shots, OnTarget, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (id, Year, Matches_Played, Substitution, Mins, Goals, xG, Shots, OnTarget, player_id))

def insert_Player(curs, name, club_name):
    curs.execute("SELECT COUNT(*) FROM Player WHERE name = '{}'".format(name.replace("'",'')))
    count = curs.fetchone()[0]
    if count == 0:
        curs.execute("SELECT COUNT(*) FROM Player")
        player_id = curs.fetchone()[0]
        curs.execute("SELECT club_id FROM club WHERE Name = '{}'".format(club_name))
        club_id = curs.fetchone()[0]
        curs.execute("INSERT INTO Player (player_id, name, club_id) VALUES (%s, %s, %s)",
                    (player_id, name.replace("'",''), club_id))

if __name__ == "__main__":
    connection = db_connect()
    create_league_table(connection)
    create_club_table(connection)
    create_Player_table(connection)
    create_Stats_table(connection)
    start_time = time.time()
    num = 0
    for index, row in read_csv().iterrows():
        with connection:cur = connection.cursor()
        if int(index) % 100 == 0:
            elapsed_time = time.time() - start_time
            print('import of '+str(index)+' took '+str(round(elapsed_time, 2))+' seconds')
            start_time = time.time()
        insert_league(cur, row['league_name'])
        insert_club(cur, row['Country'], row['Club'], row['league_name'])
        insert_Player(cur, row['name'], row['Club'])
        insert_Stats(cur, row['id'], row['name'], row['Year'], row['Matches_Played'], row['Substitution'],
                     row['Mins'], row['Goals'], row['xG'], row['Shots'], row['OnTarget'])
        num = index
    elapsed_time = time.time() - start_time
    print(f"Imported {num}, Elapsed Time: {round(elapsed_time, 2)} seconds")
