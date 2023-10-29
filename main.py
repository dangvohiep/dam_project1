import pandas as pd


DATA_REPO = r'https://raw.githubusercontent.com/dangvohiep/tennis_atp/master/'


class ATPMatches:

    file_prefix = 'atp_matches_'

    def __init__(
        self,
        season: float,
    ) -> None:

        # assign instance variables
        self.season = season

        # cached tables
        self.__raw_table        = None
        self.__tournaments      = None
        self.__players          = None
        self.__matches          = None
        self.__rankings         = None
        self.__match_stats      = None

    # read-only
    @property
    def raw_table(self):
        
        # return the cached table
        if self.__raw_table is not None:
            return self.__raw_table

        # read data file from data repository
        self.__raw_table: pd.DataFrame = pd.read_csv(
            f"{DATA_REPO}{type(self).file_prefix}{self.season}.csv",
            header=0,
            parse_dates=['tourney_date'],
            date_format=r'YYYYmmdd',
            dtype={
                'tourney_id'            : str,
                'tourney_name'          : str,
                'surface'               : str,
                'draw_size'             : float,
                'tourney_level'         : str,
                'tourney_date'          : str,
                'match_num'             : int,
                'winner_id'             : str,
                'winner_seed'           : float,
                'winner_entry'          : str,
                'winner_name'           : str,
                'winner_hand'           : str,
                'winner_ht'             : float,
                'winner_ioc'            : str,
                'winner_age'            : float,
                'loser_id'              : str,
                'loser_seed'            : float,
                'loser_entry'           : str,
                'loser_name'            : str,
                'loser_hand'            : str,
                'loser_ht'              : float,
                'loser_ioc'             : str,
                'loser_age'             : float,
                'score'                 : str,
                'best_of'               : float,
                'round'                 : str,
                'minutes'               : float,
                'w_ace'                 : float,
                'w_df'                  : float,
                'w_svpt'                : float,
                'w_1stIn'               : float,
                'w_1stWon'              : float,
                'w_2ndWon'              : float,
                'w_SvGms'               : float,
                'w_bpSaved'             : float,
                'w_bpFaced'             : float,
                'l_ace'                 : float,
                'l_df'                  : float,
                'l_svpt'                : float,
                'l_1stIn'               : float,
                'l_1stWon'              : float,
                'l_2ndWon'              : float,
                'l_SvGms'               : float,
                'l_bpSaved'             : float,
                'l_bpFaced'             : float,
                'winner_rank'           : float,        
                'winner_rank_points'    : float,
                'loser_rank'            : float,
                'loser_rank_points'     : float,
            },
        )
        return self.__raw_table

    # read-only
    @property
    def tournaments(self):

        # return cached table
        if self.__tournaments is not None:
            return self.__tournaments
        
        # normalize data from raw_table
        self.__tournaments = self.raw_table[[
            'tourney_id',
            'tourney_name',
            'tourney_level',
            'tourney_date',
            'surface',
            'draw_size',
        ]].drop_duplicates().reset_index(drop=True)
        return self.__tournaments

    # read-only
    @property
    def players(self):
        
        # return cached table
        if self.__players is not None:
            return self.__players

        # normalize data from raw_table
        map_name = lambda x: x.split('_')[1] if not x.endswith('_id') else 'player_id'
        winner_table: pd.DataFrame = self.raw_table[[
            'winner_id',
            'winner_name',
            'winner_hand',
            'winner_ht',
            'winner_ioc',
            'winner_age',
        ]].rename(mapper=map_name, axis=1)
        loser_table: pd.DataFrame = self.raw_table[[
            'loser_id',
            'loser_name',
            'loser_hand',
            'loser_ht',
            'loser_ioc',
            'loser_age',
        ]].rename(mapper=map_name, axis=1)
        self.__players = pd.concat(
            [winner_table, loser_table],
            ignore_index=True,
            axis=0,
        ).drop_duplicates().reset_index(drop=True)
        self.__players['birth'] = self.season - self.__players['age'].round()
        self.__players = self.__players.drop(labels=['age'], axis=1)
        self.__players = self.__players.rename({'ht': 'height'}, axis=1)
        return self.__players

    # read-only
    @property
    def matches(self):

        # return cached table
        if self.__matches is not None:
            return self.__matches

        # normalize data from raw_table
        self.__matches = self.raw_table[[
            'match_num', 
            'tourney_id',
            'winner_id', 
            'loser_id',
            'score', 
            'best_of',
            'round', 
            'minutes',
        ]].drop_duplicates().reset_index(drop=True)
        self.__matches.insert(loc=0, column='season', value=self.season)
        return self.__matches

    # read-only
    @property
    def match_stats(self):

        # return cached table
        if self.__match_stats is not None:
            return self.__match_stats

        # inner function for explicit naming
        def map_name(raw_name):
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

        # normalize data from raw_table
        winner_table = self.raw_table[[
            'player_id', 'w_ace', 'w_df', 'w_svpt'
            'w_1stIn', 'w_1stWon', 'w_2ndWon',
            'w_SvGms', 'w_bpSaved', 'w_bpFaced',
        ]].rename(mapper=map_name, axis=1)
        loser_table = self.raw_table[[
            'l_ace', 'l_df', 'l_svpt'
            'l_1stIn', 'l_1stWon', 'l_2ndWon',
            'l_SvGms', 'l_bpSaved', 'l_bpFaced',
        ]].rename(mapper=map_name, axis=1)
        self.__match_stats = pd.concat(
            [winner_table, loser_table],
            axis=0,
        )
        # Retrieve "matches"."match_id" from database
        ...
        return self.__match_stats

    # read-only
    @property
    def rankings(self):

        # return cached table
        if self.__rankings is not None:
            return self.__rankings

        # normalize data from raw_table
        map_name = lambda x: 'player_' + x.split('_', maxsplit=1)[1].replace('rank_','')
        winner_table = self.raw_table[[
            'winner_id',
            'winner_rank',
            'winner_rank_points'
        ]].rename(mapper=map_name, axis=1)
        loser_table = self.raw_table[[
            'loser_id',
            'loser_rank',
            'loser_rank_points'
        ]].rename(mapper=map_name, axis=1)
        self.__rankings = pd.concat(
            [winner_table, loser_table],
            ignore_index=True,
            axis=0,
        ).drop_duplicates().reset_index(drop=True)
        self.__rankings.insert(loc=0, column='season', value=self.season)
        return self.__rankings

    # yet implement
    def insert_db(self):
        ...


if __name__ == '__main__':
    self = ATPMatches(season=1985)








