import csv
import argparse
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# create a base class for our models
Base = declarative_base()

# define a Leaderboard model
class Leaderboard(Base):
    __tablename__ = 'leaderboard'
    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer)
    first_name = Column(String(120))
    last_name = Column(String(120))
    messages = Column(Integer)

def main(db_uri, csv_path):
    # create a database engine
    engine = create_engine(db_uri, pool_pre_ping=True)

    # create the table
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    # open the CSV file
    with open(csv_path, newline='') as csvfile:
        # create a CSV reader object
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        # iterate over each row in the file
        next(reader, None)
        for row in reader:
            # do something with the data in the row
            # print(row[0], row[1], row[2], row[3])
            # create a new leaderboard entry
            new_leaderboard = Leaderboard(user_id=row[0], first_name=row[1], last_name=row[2], messages=row[3])
            session.add(new_leaderboard)
            session.commit()
        print("Données enregistrées avec succès")

    session.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Import data from a CSV file into a database.')
    parser.add_argument('db_uri', help='URI of the database')
    parser.add_argument('csv_path', help='Path to the CSV file')
    args = parser.parse_args()

    main(args.db_uri, args.csv_path)


# python feed_database.py sqlite:///database/data.db samples/pipeline_result.csv