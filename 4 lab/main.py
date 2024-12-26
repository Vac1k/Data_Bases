import psycopg2
import matplotlib.pyplot as plt
import numpy as np

username = 'Onyshchenko'
password = 'Onyshchenko'
database = 'football'
host = 'localhost'
port = '5432'

query_1 = '''
-- 1. Кількість ударів в залежності від країни
SELECT country, sum(shots) as shots_per_country
FROM club as cl JOIN player as pl on cl.club_id = pl.club_id
	 JOIN stats as s on pl.player_id = s.player_id
GROUP BY country
ORDER BY shots_per_country DESC;
'''
query_2 = '''
-- 2. Частка футболістів у кожній лізі
SELECT league_name, count(pl.name) as shots_per_country
FROM league as l JOIN club as cl on l.league_id = cl.league_id 
	 JOIN player as pl on cl.club_id = pl.club_id
GROUP BY league_name
ORDER BY shots_per_country DESC;
'''
query_3 = '''
-- 3. Кількість голів кожного футболіста
SELECT name, goals
FROM player as pl JOIN stats as s on pl.player_id = s.player_id
ORDER BY goals DESC;
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    for i, query in enumerate([query_1, query_2, query_3]):
        if i != 0:
            print()
            print('_'*20)
            print()
        print(f'Запит {i+1}')
        cur.execute(query_1)

        for row in cur:
            print(row)
