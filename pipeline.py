import abc
import pandas as pd
import sqlalchemy
from authentication import DB_CONNECTION_STRING


engine = sqlalchemy.create_engine(DB_CONNECTION_STRING)
DATA_REPO = r'https://raw.githubusercontent.com/dangvohiep/tennis_atp/master/'


# abstract class
class DataSource:

    # abstract class variable
    @property
    @abc.abstractmethod
    def file_prefix(self):
        pass    

    # concrete constructor
    def __init__(self, season: int) -> None:
        # assign instance variables
        self.season = season
        # cached table
        self._table = None

    # read-only abstract property
    @property
    @abc.abstractmethod
    def table(self) -> pd.DataFrame:
        pass


# abstract class
class ETL:

    # concrete constructor
    def __init__(self, source: DataSource) -> None:
        # assign instance variables
        self.source = source
        # internal table
        self._table = None

    @abc.abstractmethod
    def extract(self):
        pass

    @abc.abstractmethod
    def transform(self):
        pass

    @abc.abstractmethod
    def load(self):
        pass


class ATP(DataSource):

    file_prefix = 'atp_matches_'
    type_mapping = {
        'tourney_id'        : str,
        'tourney_name'      : str,
        'surface'           : str,
        'draw_size'         : float,
        'tourney_level'     : str,
        'tourney_date'      : str,
        'match_num'         : int,
        'winner_id'         : str,
        'winner_seed'       : float,
        'winner_entry'      : str,
        'winner_name'       : str,
        'winner_hand'       : str,
        'winner_ht'         : float,
        'winner_ioc'        : str,
        'winner_age'        : float,
        'loser_id'          : str,
        'loser_seed'        : float,
        'loser_entry'       : str,
        'loser_name'        : str,
        'loser_hand'        : str,
        'loser_ht'          : float,
        'loser_ioc'         : str,
        'loser_age'         : float,
        'score'             : str,
        'best_of'           : float,
        'round'             : str,
        'minutes'           : float,
        'w_ace'             : float,
        'w_df'              : float,
        'w_svpt'            : float,
        'w_1stIn'           : float,
        'w_1stWon'          : float,
        'w_2ndWon'          : float,
        'w_SvGms'           : float,
        'w_bpSaved'         : float,
        'w_bpFaced'         : float,
        'l_ace'             : float,
        'l_df'              : float,
        'l_svpt'            : float,
        'l_1stIn'           : float,
        'l_1stWon'          : float,
        'l_2ndWon'          : float,
        'l_SvGms'           : float,
        'l_bpSaved'         : float,
        'l_bpFaced'         : float,
        'winner_rank'       : float,        
        'winner_rank_points': float,
        'loser_rank'        : float,
        'loser_rank_points' : float,
    }

    # implement
    @property
    def table(self):
        # return the cached table
        if self._table is not None:
            return self._table
        # read data file from data repository
        self._table: pd.DataFrame = pd.read_csv(
            f"{DATA_REPO}{type(self).file_prefix}{self.season}.csv",
            header=0,
            parse_dates=['tourney_date'],
            date_format=r'YYYYmmdd',
            dtype=type(self).type_mapping,
        )
        return self._table


class Tournaments(ETL):

    # implement
    def extract(self):
        self._table = self.source.table[[
            'tourney_id',
            'tourney_name',
            'tourney_level',
            'tourney_date',
            'surface',
            'draw_size',
        ]].copy()
        return self
    
    # implement
    def transform(self):
        self._table = self._table.drop_duplicates().reset_index(drop=True)
        return self

    # implement
    def load(self):
        # delete all records in the internal table from db table
        inserting_ids = self._table['tourney_id'].to_list()
        query = sqlalchemy.sql.text(
            f"""delete from tournaments where tourney_id = any(:ids)"""
        )
        with engine.begin() as db_connection:
            db_connection.execute(
                statement=query, 
                parameters={'ids': inserting_ids}
            )
        # insert all records in the internal table to db
        self._table.to_sql(name="tournaments", con=engine, if_exists='append', index=False)


class Players(ETL):

    # implement
    def extract(self):
        winner_table: pd.DataFrame = self.source.table[[
            'winner_id',
            'winner_name',
            'winner_hand',
            'winner_ht',
            'winner_ioc',
            'winner_age',
        ]].rename(mapper=type(self).__rename_column, axis=1)
        loser_table: pd.DataFrame = self.source.table[[
            'loser_id',
            'loser_name',
            'loser_hand',
            'loser_ht',
            'loser_ioc',
            'loser_age',
        ]].rename(mapper=type(self).__rename_column, axis=1)
        self._table = pd.concat(
            [winner_table, loser_table],
            ignore_index=True,
            axis=0,
        )
        return self
    
    # implement
    def transform(self):
        self._table['birth_year'] = self.source.season - self._table['age'].floordiv(1)
        self._table['birth_year'] = self._table['birth_year'].mode().squeeze()
        self._table = self._table.drop(labels=['age'], axis=1)
        self._table = self._table.drop_duplicates().reset_index(drop=True)
        return self

    # implement
    def load(self):
        # delete all records in the internal table from db table
        inserting_ids = self._table['player_id'].to_list()
        query = sqlalchemy.sql.text(
            f"""delete from players where player_id = any(:ids)"""
        )
        with engine.begin() as db_connection:
            db_connection.execute(
                statement=query, 
                parameters={'ids': inserting_ids}
            )
        # insert all records in the internal table to db
        self._table.to_sql(name="players", con=engine, if_exists='append', index=False)

    @staticmethod
    def __rename_column(raw_name: str):
        if raw_name.endswith('_id'):
            return 'player_id'
        if raw_name.endswith('_ht'):
            return 'height'
        return raw_name.split('_')[1]


class Matches(ETL):

    # implement
    def extract(self):
        self._table = self.source.table[[
            'tourney_id', 'match_num', 'winner_id', 'loser_id',
            'score', 'best_of', 'round', 'minutes', 'w_ace', 
            'w_df', 'w_svpt', 'w_1stIn', 'w_1stWon', 'w_2ndWon',
            'w_SvGms', 'w_bpSaved', 'w_bpFaced', 'l_ace', 'l_df', 
            'l_svpt', 'l_1stIn', 'l_1stWon', 'l_2ndWon', 'l_SvGms', 
            'l_bpSaved', 'l_bpFaced',
        ]].rename(type(self).__rename_column, axis=1)
        return self

    # implement
    def transform(self):
        self._table = self._table.drop_duplicates().reset_index(drop=True)
        self._table.insert(loc=0, column='season', value=self.source.season)
        return self

    # implement
    def load(self):
        # delete all records in the internal table from db table
        inserting_season = self.source.season
        query = sqlalchemy.sql.text(
            f"""delete from matches where season = :season"""
        )
        with engine.begin() as db_connection:
            db_connection.execute(
                statement=query, 
                parameters={'season': inserting_season}
            )
        # insert all records in the internal table to db
        self._table.to_sql(name="matches", con=engine, if_exists='append', index=False)

    # private static method for explicit naming
    @staticmethod
    def __rename_column(raw_name: str):
        if not raw_name.startswith(('l_','w_')):
            return raw_name
        prefix, surfix = raw_name.split('_', maxsplit=1)
        prefix_mapping = {
            'w'     : 'winner',
            'l'     : 'loser',
        }
        surfix_mapping = {
            'id'    : 'player_id',
            'ace'   : 'aces',
            'df'    : 'double_faults',
            'svpt'  : 'serve_points',
            '1stIn' : 'first_serve_in',
            '1stWon': 'first_serve_won',
            '2ndWon': 'second_serve_won',
            'SvGms' : 'service_games',
            'bpSaved': 'break_points_saved',
            'bpFaced': 'break_points_faced',
        }
        return prefix_mapping[prefix] + '_' + surfix_mapping[surfix]


class Rankings(ETL):

    # implement
    def extract(self):
        winner_table = self.source.table[[
            'tourney_id',
            'winner_id',
            'winner_rank',
            'winner_rank_points'
        ]].rename(mapper=type(self).__rename_column, axis=1)
        loser_table = self.source.table[[
            'tourney_id',
            'loser_id',
            'loser_rank',
            'loser_rank_points'
        ]].rename(mapper=type(self).__rename_column, axis=1)
        self._table = pd.concat(
            [winner_table, loser_table],
            ignore_index=True,
            axis=0,
        )
        return self

    # implement
    def transform(self):
        self._table = self._table.drop_duplicates().reset_index(drop=True)
        self._table.insert(loc=0, column='season', value=self.source.season)
        return self

    # implement
    def load(self):
        # delete all records in the internal table from db table
        inserting_season = self.source.season
        query = sqlalchemy.sql.text(
            f"""delete from rankings where season = :season""",
        )
        with engine.begin() as db_connection:
            db_connection.execute(
                statement=query, 
                parameters={'season': inserting_season}
            )
        # insert all records in the internal table to db
        self._table.to_sql(name="rankings", con=engine, if_exists='append', index=False)

    @staticmethod
    def __rename_column(raw_name: str):
        name_mapping = {
            'tourney_id'    : 'tourney_id',
            'winner_id'     : 'player_id',
            'loser_id'      : 'player_id',
            'winner_rank'   : 'rank',
            'loser_rank'    : 'rank',
            'winner_rank_points': 'points',
            'loser_rank_points': 'points',
        }
        return name_mapping[raw_name]

if __name__ == '__main__':

    start_season    = 1985
    end_season      = 2023

    for season in range(start_season, end_season+1):
        atp_source = ATP(season=season)
        Tournaments(source=atp_source).extract().transform().load()
    for season in range(start_season, end_season+1):
        atp_source = ATP(season=season)
        Players(source=atp_source).extract().transform().load()
    for season in range(start_season, end_season+1):
        atp_source = ATP(season=season)
        Rankings(source=atp_source).extract().transform().load()
    for season in range(start_season, end_season+1):
        atp_source = ATP(season=season)
        Matches(source=atp_source).extract().transform().load()


    """
    INSERTING ORDER:
    FOR EACH SEASON, SEPARATELY INSERT TABLES BY THE FOLLOWING ORDER:
        1. tournaments      : dimension table
        2. players          : dimension table
        3. rankings         : fact table
        4. matches          : fact table
    """


