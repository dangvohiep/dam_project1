import abc
import pandas as pd


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
        ...


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
        self._table = self._table.drop_duplicates().reset_index(drop=True)
        self._table['birth'] = self.season - self._table['age'].round()
        self._table = self._table.drop(labels=['age'], axis=1)
        return self

    # implement
    def load(self):
        ...

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
            'match_num', 
            'tourney_id',
            'winner_id', 
            'loser_id',
            'score', 
            'best_of',
            'round', 
            'minutes',
        ]].copy()
        return self

    # implement
    def transform(self):
        self._table = self._table.drop_duplicates().reset_index(drop=True)
        self._table.insert(loc=0, column='season', value=self.season)
        return self

    # implement
    def load(self):
        ...


class MatchStats(ETL):

    def extract(self):
        winner_table = self.source.table[[
            'player_id', 'w_ace', 'w_df', 'w_svpt'
            'w_1stIn', 'w_1stWon', 'w_2ndWon',
            'w_SvGms', 'w_bpSaved', 'w_bpFaced',
        ]].rename(mapper=type(self).__rename_column, axis=1)
        loser_table = self.source.table[[
            'l_ace', 'l_df', 'l_svpt'
            'l_1stIn', 'l_1stWon', 'l_2ndWon',
            'l_SvGms', 'l_bpSaved', 'l_bpFaced',
        ]].rename(mapper=type(self).__rename_column, axis=1)
        self._table = pd.concat(
            [winner_table, loser_table],
            axis=0,
        )
        return self

    # implement
    def transform(self):
        # Retrieve "matches"."match_id" from database
        ...
        return self

    # implement
    def load(self):
        ...

    # private static method for explicit naming
    @staticmethod
    def __rename_column(raw_name):
        raw_name = raw_name.split('_')[1]
        name_mapping = {
            'ace'   : 'aces',
            'df'    : 'double_faults',
            'svpt'  : 'serve_points',
            '1stIn' : 'first_serve_in',
            '1stWon': 'first_serve_won',
            '2ndWon': 'second_serve_won',
            'SvGms' : 'service_games',
            'bpSaved': 'break_points_saved',
            'bpFaced': 'break_points_faced'
        }
        return name_mapping[raw_name]

class Rankings(ETL):

    # implement
    def extract(self):
        winner_table = self.source.table[[
            'winner_id',
            'winner_rank',
            'winner_rank_points'
        ]].rename(mapper=type(self).__rename_column, axis=1)
        loser_table = self.source.table[[
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
        self._table.insert(loc=0, column='season', value=self.season)
        return self

    # implement
    def load(self):
        ...

    @staticmethod
    def __rename_column(raw_name: str):
        raw_name = raw_name.split("_", maxsplit=1)[1]
        return 'player_' + raw_name.replace('rank','')


if __name__ == '__main__':

    atp_source = ATP(season=1985)

    tournaments = Tournaments(source=atp_source).extract().transform().load()
    players = Players(source=atp_source).extract().transform().load()
    matches = Matches(source=atp_source).extract().transform().load()
    match_stats = MatchStats(source=atp_source).extract().transform().load()







