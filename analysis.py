import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import sqlalchemy
from authentication import DB_CONNECTION_STRING

engine = sqlalchemy.create_engine(DB_CONNECTION_STRING)

# PART 4: EDA
query = """
    SELECT 
        t."tourney_name",
        t."tourney_level",
        t."tourney_date",
        t."surface",
        t."draw_size",
        m."season",
        pw."name"       AS "winner_name",
        pw."hand"       AS "winner_hand",
        pw."height"     AS "winner_height",
        pw."ioc"        AS "winner_ioc",
        pw."birth_year" AS "winner_birth_year",
        pl."name"       AS "loser_name",
        pl."hand"       AS "loser_hand",
        pl."height"     AS "loser_height",
        pl."ioc"        AS "loser_ioc",
        pl."birth_year" AS "loser_birth_year",
        m."score",
        m."best_of",
        m."round",
        m."minutes",
        m."winner_aces",
        m."winner_double_faults",
        m."winner_serve_points",
        m."winner_first_serve_in",
        m."winner_first_serve_won",
        m."winner_second_serve_won",
        m."winner_service_games",
        m."winner_break_points_saved",
        m."winner_break_points_faced",
        m."loser_aces",
        m."loser_double_faults",
        m."loser_serve_points",
        m."loser_first_serve_in",
        m."loser_first_serve_won",
        m."loser_second_serve_won",
        m."loser_service_games",
        m."loser_break_points_saved",
        m."loser_break_points_faced",
        rw."rank"       AS "winner_rank",
        rw."points"     AS "winner_points",
        rl."rank"       AS "loser_rank",
        rl."points"     AS "loser_points"
    FROM "tournaments" t
    JOIN "matches" m ON t."tourney_id" = m."tourney_id"
    JOIN "players" pw ON m."winner_id" = pw."player_id"
    JOIN "players" pl ON m."loser_id" = pl."player_id"
    LEFT JOIN "rankings" rw 
        ON rw."player_id" = pw."player_id" 
        AND rw."tourney_id" = t."tourney_id" 
        AND rw."season" = m."season"
    LEFT JOIN "rankings" rl 
        ON rl."player_id" = pl."player_id" 
        AND rl."tourney_id" = t."tourney_id" 
        AND rl."season" = m."season"
    ORDER BY t."tourney_date", m."match_num";

"""

table = pd.read_sql(sql=query, con=engine)
table.info()

nummerical_variables = table.select_dtypes(exclude=object)
nummerical_variables.describe()
categorical_variables = table.select_dtypes(include=object)
categorical_variables.describe()

plt.figure(figsize=(25, 15))
correlation_matrix = nummerical_variables.corr()
sns.heatmap(correlation_matrix, cmap="YlGnBu", annot=True) 
plt.savefig('heatmap.png')
plt.close()

"""
Here are some key insights and interpretations from the correlation matrix of various tennis game statistics:

Tournament Draw Size and Match Length:
The size of the tournament draw (draw_size) has a strong positive correlation with the length of the match in minutes (minutes, 0.4) and best_of (0.56). This implies that larger tournaments tend to have longer matches, and they might typically be best-of-5 sets instead of best-of-3.
Player Age and Tennis Season:
There's a very strong correlation between the birth year of both winners (winner_birth_year, 0.91) and losers (loser_birth_year, 0.92) with the tennis season. This indicates that as the years progress, newer generations of players emerge.
Height and Aces:
There's a moderate positive correlation between the height of the winner (winner_height) and the number of aces they hit (winner_aces, 0.42). This suggests that taller players might have an advantage in serving aces.
Match Length and Serving Statistics:
The length of the match (minutes) has very strong correlations with several serving statistics such as winner_serve_points (0.9), winner_first_serve_in (0.85), and winner_service_games (0.89). This means that longer matches naturally have more serve points, and the quality of serving (first serves in, service games) is crucial in such matches.
Serving and Winning:
winner_first_serve_in and winner_first_serve_won have a very strong positive correlation (0.96). This indicates the importance of getting the first serve in and then winning those points. The same pattern is seen for losers, but it's slightly less pronounced.
Break Points and Match Length:
There's a strong positive correlation between the length of the match (minutes) and the number of break points faced by the winner (winner_break_points_faced, 0.61). Longer matches often have more critical moments where players face break points.
Birth Year and Ranking:
The birth year of the winner (winner_birth_year) has a moderate positive correlation with the winner's rank (winner_rank, 0.28). This might indicate that younger players, on average, have a higher (worse) ranking, which makes sense as they're still climbing up the ladder.
Serve Points and Double Faults:
There's a moderate positive correlation between the number of serve points by the winner (winner_serve_points) and the number of double faults they commit (winner_double_faults, 0.49). This suggests that, in games where players serve more, they might also risk more, leading to more double faults.
These insights provide a glimpse into the dynamics of tennis matches and the factors that influence match outcomes. However, correlation does not imply causation, and these observations would need more detailed analyses and domain-specific knowledge for a comprehensive understanding.
"""

# PART 5a: 
"""
Historically, there has been a noticeable trend in professional tennis: taller players tend to serve more aces than their shorter counterparts. This observation suggests several interconnected factors:

Serve Angle: Taller players have a height advantage, allowing them to serve the ball from a higher point. This height gives them a better angle to hit the ball downwards into the service box, making it more challenging for opponents to return, potentially leading to more aces.
Leverage: The arm length, combined with height, allows taller players to achieve greater racquet speed at the point of contact. This leverage can result in faster and more powerful serves.
Ball Toss: Taller players can toss the ball higher, giving them more time to position and generate power for the serve.
Evolution of the Game: Over the years, the game of tennis has seen changes in racquet technology, training techniques, and playing surfaces. These changes might have favored powerful serving, and taller players could have benefited more in this regard.
Challenges for the Returner: A powerful serve coming from a high release point can be challenging to read and return. The trajectory and bounce are different from serves of shorter players, making it trickier for the returner to anticipate and position themselves.
"""

# Lambda function to rename columns: It takes the last portion of a string after the underscore
rename_function = lambda x: x.split('_')[-1]

# Extract relevant columns for winners and rename the columns
winner_table = table[['season','winner_height','winner_aces']]
winner_table = winner_table.rename(rename_function, axis=1)
winner_table['type'] = 'winner'

# Extract relevant columns for losers and rename the columns
loser_table = table[['season','loser_height','loser_aces']]
loser_table = loser_table.rename(rename_function, axis=1)
loser_table['type'] = 'loser'

# Merge both winner and loser tables vertically and drop any rows with missing data
analysis_table = pd.concat(
    [winner_table, loser_table], 
    axis=0
).dropna(how='any')

# Initialize a grid of plots with an Axes for each season.
# This FacetGrid will allow us to create a multi-plot grid for 
# visualizing relationships across many levels of a categorical variable
g = sns.FacetGrid(
    data=analysis_table, 
    col="season", 
    hue="type", 
    height=3, 
    aspect=1.5, 
    col_wrap=4,     # number of columns before wrapping to the next row
    sharey=False,   # each facet/plot will have its own y-axis scale
)
# Apply the regression plot onto the FacetGrid to show the relationship 
# between height and aces for each type
g = g.map_dataframe(
    func=sns.regplot, 
    x="height", 
    y="aces", 
    scatter_kws={'s': 30},      # specify size of scatter points
    line_kws={'linewidth': 3}   # specify linewidth of the regression line
)

# Set x and y axis labels with increased font size
g.set_axis_labels(x_var="Height", y_var="Aces", fontsize=20)

# Set title for each subplot based on the season, with specified font style and size
g.set_titles(template="{col_name}", fontweight="bold", fontsize=20)

# Adjust legend title and label sizes
plt.setp(g._legend.get_title(), fontsize=20)
plt.setp(g._legend.get_texts(), fontsize=20)

# Adjust font sizes for tick labels on x and y axes
g.set_xticklabels(fontsize=15)
g.set_yticklabels(fontsize=15)

# Add the legend without a title and specify the order of labels
g.add_legend(title="", fontsize=20, label_order=["winner", "loser"])

# Set the y-axis to start from 0
g.set(ylim=(0, None))

# Display the plots
plt.savefig('analysis1.png')
plt.close()



# PART 5b:

"""
In tennis, the serve is a foundational stroke that can set the tone for a point. The data shows a compelling correlation between the percentage of first serves that land in (first_serve_in) and the percentage of those serves that result in points won (first_serve_won). With a correlation coefficient nearing 0.96, this relationship underscores the significance of not only getting the first serve into play but also capitalizing on it. Players who consistently land their first serves and then dominate in the ensuing rally often find themselves with an edge.

This pattern suggests that in professional tennis, it's not just about serving to start the rally; it's about using the serve as a potent weapon. The ability to consistently get the first serve in and then effectively handle the subsequent play can be a decisive factor in the outcome of a match. The data emphasizes the strategic importance of an effective first serve in the high-stakes environment of professional tennis.
"""

# Define a function to split a string at the first underscore and return the second portion; 
# if there's no underscore, return the string as it is.
rename_function = lambda x: x.split('_', maxsplit=1)[1] if '_' in x else x

# Extract relevant columns for winners and rename them using the previously defined function
winner_table = table[[
    'season', 
    'winner_first_serve_in', 
    'winner_first_serve_won'
]]
winner_table = winner_table.rename(rename_function, axis=1)
# Add a column to label the data as 'winner'
winner_table['type'] = 'winner'

# Extract relevant columns for losers and rename them similarly
loser_table = table[[
    'season',
    'loser_first_serve_in',
    'loser_first_serve_won'
]]
loser_table = loser_table.rename(rename_function, axis=1)
# Add a column to label the data as 'loser'
loser_table['type'] = 'loser'

# Merge both winner and loser tables vertically and drop any rows with missing data
analysis_table = pd.concat(
    [winner_table, loser_table], 
    axis=0
).dropna(how='any')

# Set the Seaborn plot style to "whitegrid"
sns.set_style("whitegrid")
# Set Seaborn plot context to "talk" which is optimal for presentations;
sns.set_context("talk")

# Create a jointplot to visualize the relationship between 'first_serve_in' and 'first_serve_won' 
jp = sns.jointplot(
    x="first_serve_in", 
    y="first_serve_won", 
    data=analysis_table,
    kind="reg", 
    truncate=False,
    height=7,
    scatter_kws={'s': 50, 'alpha': 0.7},
    line_kws={'linewidth': 2.5}
)

# Adjust plot labels
jp.ax_joint.set_xlabel("First Serve In", fontsize=14)
jp.ax_joint.set_ylabel("First Serve Won", fontsize=14)
# Adds dashed gridlines to the histograms on the top and right side of the jointplot
jp.ax_marg_x.grid(True, which="both", linestyle="--", linewidth=0.5)
jp.ax_marg_y.grid(True, which="both", linestyle="--", linewidth=0.5)


# Display the plots
plt.tight_layout()
plt.savefig('analysis2.png', dpi=300, bbox_inches="tight")

