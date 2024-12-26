import psycopg2
import matplotlib.pyplot as plt

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

    cur.execute(query_1)
    names = []
    total = []

    for row in cur:
        names.append(row[0])
        total.append(row[1])

    x_range = range(len(names))

    figure, (bar_ax, pie_ax, graph_ax) = plt.subplots(1, 3)
    bar = bar_ax.bar(x_range, total)
    bar_ax.bar_label(bar, label_type='center')  # потрібен новий matplotlib
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(names, rotation=45, ha='right')
    bar_ax.set_xlabel('Країна')
    bar_ax.set_ylabel('Кількість разів')
    bar_ax.set_title('Кількість ударів в залежності від країни')
    cur.execute(query_2)
    names = []
    total = []

    for row in cur:
        names.append(row[0])
        total.append(row[1])

    x_range = range(len(names))
    pie_ax.pie(total, labels=names, autopct='%1.1f%%')
    pie_ax.set_title('Частка футболістів у кожній лізі')

    cur.execute(query_3)
    names = []
    total = []

    for row in cur:
        names.append(row[0])
        total.append(row[1])

    mark_color = 'blue'
    graph_ax.plot(names, total, color=mark_color, marker='o')

    for qnt, price in zip(names, total):
        graph_ax.annotate(price, xy=(qnt, price), color=mark_color,
                          xytext=(7, 2), textcoords='offset points')

    graph_ax.set_xlabel('Імена футболістів')
    graph_ax.set_ylabel('Кількість голів')
    graph_ax.set_xticklabels(names, rotation=45, ha="right")
    graph_ax.plot(names, total, color='blue', marker='o')
    graph_ax.set_title('Кількість голів кожного футболіста')

mng = plt.get_current_fig_manager()
mng.full_screen_toggle()
# mng.resize(1400, 600)

plt.show()
