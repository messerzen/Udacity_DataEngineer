# Project - Data Modeling with Postgres

## Case - Sparkfy 

#### Paulo Henrique Zen Messerschmidt

______

# Data analytics overview

Considering all the data collected through the ETL pipeline for Sparkfify Startup, there are some interesting analysis that can be performed in order to provide a better data-driven decisions that can lead to sales performance improvements and marketing performance improvements. The ETL process gets data from songs, that are registered in the sparkify platform and users and their respective subscription type (level). Also, data from user interaction with the plataform are  collected and registered in the DBRMS (Postgres). Considering the available data, the following analytical goals can be achieved:

- **To Identify the TOP (n) songs listened for third part advertising pricing**: If the platform plays third part advertising between musics (or interrupt the music to play it) such youtube and spotify, these information can be used to "sell" this spaces for a higher price (e.g.: if a third part wants its advertising playing in the top 100 musics from the 1st position, the sparkify platform could be charge a price of 1.5 dollars for each time the advertising plays. But if the advertising plays in the top 50 starting from the 100th position, the charge applied could be 1.00 dollars). Or, even to set a dynamic pricing considering the music goes to the top or down position of the top listening songs.
- **Identify regions with lower volume of customers:** Considering that the songplays table registers the log of user activity by region, this information could be used to identify what are the regions with less or more interactions and create a geomarketing strategies to  guide digital marketing investments in the regions with less quantity of users.
- **Analyze customer profile**: Informations about the user behavior could be used to improve the user experience in the platform applying recommendation algorithms to recommend new songs or artists. Also, the data about the user level could be used to identify what is the behavior of the user that uses the free level, and create strategies to convert the user to a paid plan.

In summary, the database can be used to guide product strategies in order to improve user experience, better marketing and sales strategies in order to get more more customers and increase the marketshare.

# Database schema and pipeline

The database schema chosen was the star schema model, an it is illustrated in the image below. The model consists of the following tables:

- songplays (Fact Table): contains all the registers of user activity, describing which songs have listened by the user.
- users (Dimension Table): contains the data that describe the users;
- songs(Dimension Table): contains the data that describes artists;
- time (Dimension Table): contains the data that describe the time of each activity, and allows to identify activity by different time attributes (hour, day, weekday, month, week)
- artists (Dimension Table): contains the data about that describe the artist.

![](/home/zen/Documentos/Python Projects/Udacity_DataEngineer/2_datamodeling/project1_datamodeling_with_postgres/db_schema.png)

The schema was chosen due to its ability to user more simple queries with a great aggregation performance. In this case, artist table was linked direct to songplays because it allows to use a direct query to get information about how many songs of a specific artist was played by all the users for example. The other option would be use the snow flake schema, linking artists table to song tables instead linking to songplays, but it would require a more complex query to get the information cited in the example above, because it would be necessary link artists table to songs table first, to get songplays registers by artist.



## Query examples

`-- Query to select top active users
SELECT count(songplay_id) total_songplay, users.first_name, users.last_name as name, users.level
FROM songplays JOIN users ON users.user_id = songplays.user_id 
GROUP BY users.first_name, users.last_name, users.level
ORDER BY total_songplay DESC LIMIT(10);`

Query result:

| total_songplay | name                   | level    |
| -------------- | ---------------------- | -------- |
| 689            | Chloe Cuevas           | paid     |
| 665            | Tegan Levine           | paid     |
| 557            | Kate Harrell           | paid     |
| 463            | Lily Koch              | paid     |
| 397            | Aleena Kirby           | paid     |
| 346            | Jacqueline Lynch       | paid     |
| 321            | Layla Griffin          | paid     |
| 289            | Jacob Klein            | paid     |
| **270**        | **Mohammad Rodriguez** | **free** |
| 248            | Matthew Jones          | paid     |

There is one user in the top 10 uses a free plan.

`-- Query to select sessions by plan
SELECT count(songplay_id) total_songplay, users.level as plan
FROM songplays JOIN users ON users.user_id = songplays.user_id
GROUP BY plan
ORDER BY total_songplay DESC;`

2

| total_songplay | plan |
| -------------- | ---- |
| 5435           | paid |
| 1385           | free |

**The most active users have a paid subscription.**

