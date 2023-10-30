-- create database named "atp"
create database "atp";

-- switch to "atp" database
\c atp

-- tournaments table
create table if not exists "tournaments" (
    "tourney_id"        varchar(255)    primary key
    , "tourney_name"    varchar(255)    not null
    , "tourney_level"   varchar(255)
    , "tourney_date"    date
    , "surface"         varchar(255)
    , "draw_size"       integer
);

-- players table
create table if not exists "players" (
    "player_id"         varchar(15)     primary key
    , "name"            varchar(255)    not null
    , "hand"            varchar(5)      check ("hand" in ('R', 'L', 'A', 'U'))
    , "height"          float
    , "ioc"             varchar(5)      not null
    , "birth_year"      integer
);

-- matches table with surrogate key
create table if not exists "matches" (
    "match_id"          serial          primary key
    , "season"          integer         not null
    , "tourney_id"      varchar(255)    not null references "tournaments" ("tourney_id") on delete cascade
    , "match_num"       integer         not null
    , "winner_id"       varchar(15)     not null references "players" ("player_id") on delete cascade
    , "loser_id"        varchar(15)     not null references "players" ("player_id") on delete cascade
    , "score"           varchar(255)
    , "best_of"         integer
    , "round"           varchar(50)
    , "minutes"         integer
    , "winner_aces"            integer
    , "winner_double_faults"   integer
    , "winner_serve_points"    integer
    , "winner_first_serve_in"  integer
    , "winner_first_serve_won" integer
    , "winner_second_serve_won" integer
    , "winner_service_games"   integer
    , "winner_break_points_saved" integer
    , "winner_break_points_faced" integer
    , "loser_aces"            integer
    , "loser_double_faults"   integer
    , "loser_serve_points"    integer
    , "loser_first_serve_in"  integer
    , "loser_first_serve_won" integer
    , "loser_second_serve_won" integer
    , "loser_service_games"   integer
    , "loser_break_points_saved" integer
    , "loser_break_points_faced" integer
    , unique("season", "tourney_id", "match_num")   -- ensures combination is unique
);

-- rankings table
create table if not exists "rankings" (
    "ranking_id"        serial          primary key
    , "season"          integer         not null
    , "tourney_id"      varchar(255)    not null references "tournaments" ("tourney_id") on delete cascade
    , "player_id"       varchar(15)     not null references "players" ("player_id") on delete cascade
    , "rank"            float
    , "points"          float
    , unique("season", "tourney_id", "player_id")   -- ensures combination of season and player_id is unique
);
