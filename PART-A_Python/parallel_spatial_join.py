#
# Assignment2 Interface
#

import psycopg2
import threading
import os
import sys

class SpatialJoinThread(threading.Thread):
    def __init__(self, threadId, name, fragmentNumber, openConnection):
        threading.Thread.__init__(self)
        self.threadId = threadId
        self.name = name
        self.fragmentNumber = fragmentNumber
        self.openConnection = openConnection
    
    def run(self):
        # print(f"Spatial Join thread on fragment {self.fragmentNumber}")
        spatialJoin(fragmentNumber=self.fragmentNumber, openConnection=self.openConnection)

def spatialJoin(fragmentNumber, openConnection):
    # Join points and rectangles on respective fragments
    curs = openConnection.cursor()
    curs.execute(f"DROP TABLE IF EXISTS f{fragmentNumber}_join")
    curs.execute(f"create table f{fragmentNumber}_join as (select count(*) as points_count, f{fragmentNumber}_rectangles.geom as rect_geom from f{fragmentNumber}_rectangles, f{fragmentNumber}_points where ST_Contains(f{fragmentNumber}_rectangles.geom, f{fragmentNumber}_points.geom) group by rect_geom order by points_count);")


def spatialPartition (openConnection):
    # Implement Spatial Partitioning
    cursor = openConnection.cursor()
    cursor.execute("select least(min(r.latitude1), min(r.latitude2), min(p.latitude)) min_lat, least(min(r.longitude1), min(r.longitude2), min(p.longitude)) min_long, greatest(max(r.latitude1), max(r.latitude2), max(p.latitude)) max_lat, greatest(max(r.longitude1), max(r.longitude2), max(p.longitude)) max_long from rectangles r, points p")
    boundaries = cursor.fetchone()
    
    # Co-ordinates for skyline (minimum, maximum) and middle spatial points
    min_lat = boundaries[0]
    min_long = boundaries[1]
    max_lat = boundaries[2]
    max_long = boundaries[3]
    mid_lat = (min_lat + max_lat) / 2
    mid_long = (min_long + max_long) / 2

    # CREATE Quadrants Table
    quadtablename = "quadrants_geom"
    cursor.execute("DROP TABLE IF EXISTS " + quadtablename)
    cursor.execute("CREATE TABLE " + quadtablename +" (quadrant INT, longitude1 REAL,  latitude1 REAL, longitude2 REAL,  latitude2 REAL, geom geometry)")
    # print(f"Created table {quadtablename}")

    cursor.execute(f"INSERT INTO {quadtablename} VALUES (1, {min_long}, {min_lat}, {mid_long}, {mid_lat})" )
    cursor.execute(f"INSERT INTO {quadtablename} VALUES (2, {mid_long}, {min_lat}, {max_long}, {mid_lat})" )
    cursor.execute(f"INSERT INTO {quadtablename} VALUES (3, {min_long}, {mid_lat}, {mid_long}, {max_lat})" )
    cursor.execute(f"INSERT INTO {quadtablename} VALUES (4, {mid_long}, {mid_lat}, {max_long}, {max_lat})" )

    # print(f"Inserted Values to table {quadtablename}")
    cursor.execute("UPDATE " + quadtablename + " SET geom = ST_MakeEnvelope(longitude1, latitude1, longitude2, latitude2, 4326);")

    print(f"Table {quadtablename} created Successfully")

    # Fragmentation
    for quad in range(1, 5):
        ## Rectangles
        cursor.execute(f"DROP TABLE IF EXISTS f{quad}_rectangles")
        cursor.execute(f"create table f{quad}_rectangles as (with quadrants as (select * from quadrants_geom where quadrant={quad}) select rectangles.latitude1, rectangles.longitude1, rectangles.latitude2, rectangles.longitude2, rectangles.geom from rectangles,quadrants where ST_Intersects(quadrants.geom, rectangles.geom));")
        
        ## Points
        cursor.execute(f"DROP TABLE IF EXISTS f{quad}_points")

        # Oh man, the level of debugging I had to go through to figure out this one conditional statement :o
        if (quad == 2) or (quad == 3):
            cursor.execute(f"create table f{quad}_points as (with quadrants as (select * from quadrants_geom where quadrant={quad}) select points.longitude, points.latitude, points.geom from points,quadrants where ST_Contains(quadrants.geom, points.geom));")
        else:
            cursor.execute(f"create table f{quad}_points as (with quadrants as (select * from quadrants_geom where quadrant={quad}) select points.longitude, points.latitude, points.geom from points,quadrants where ST_Intersects(quadrants.geom, points.geom));")
        
    print(f"Spatial Partitions created successfully")
    
    openConnection.commit()

# Do not close the connection inside this file i.e. do not perform openConnection.close()

def parallelJoin (pointsTable, rectsTable, outputTable, outputPath, openConnection):
    #Implement ParallelJoin Here.

    print("Performing Spatial Partition")
    spatialPartition(openConnection)

    print("Starting spatial join threads")

    thread1 = SpatialJoinThread(1, "Thread1", 1, openConnection)
    thread2 = SpatialJoinThread(2, "Thread2", 2, openConnection)
    thread3 = SpatialJoinThread(3, "Thread3", 3, openConnection)
    thread4 = SpatialJoinThread(4, "Thread4", 4, openConnection)

    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
    thread1.join()
    thread2.join()
    thread3.join()
    thread4.join()
    print("ALL Threads are Done")

    ## Reconstruction
    cursor = openConnection.cursor()
    
    # Join all fragment joins
    joinedTableName = "finalJoined"
    cursor.execute(f"DROP TABLE IF EXISTS {joinedTableName}")
    cursor.execute(f"create table {joinedTableName} as ((select * from f1_join) UNION ALL (select * from f2_join) UNION ALL (select * from f3_join) UNION ALL (select * from f4_join))")
    
    # Aggregate the joined results and write to outputTable with points_count
    cursor.execute(f"DROP TABLE IF EXISTS {outputTable}")
    cursor.execute(f"create table {outputTable} as (select SUM(points_count) as points_count from finalJoined group by rect_geom order by points_count)")

    print(f"Output Table {outputTable} created successfully")
    # Write to outputPath
    outputQuery = f"select * from {outputTable}"
    exportQuery = f"COPY ({outputQuery}) TO STDOUT WITH CSV"

    with open(outputPath, 'w') as f:
        cursor.copy_expert(exportQuery, f)
    
    print(f"Output written to {outputPath} successfully")

    openConnection.commit()



################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='12345', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(tablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if tablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (tablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()


