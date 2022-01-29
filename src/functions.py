import pandas as pd
import json
import re
import sys

from logger import get_logger

my_logger = get_logger("English Premier League ETL")


def get_data(dir_files) -> pd.DataFrame:
    seasons_df = pd.DataFrame()

    try:
        for file in dir_files:
            season = re.search("season-(.*)_json.json", file).group(1)
            with open(f"./data/{file}") as json_file:
                jsonObject = json.load(json_file)
                df = pd.json_normalize(jsonObject)[
                    ["Div", "HomeTeam", "AwayTeam", "FTHG", "FTAG", "Referee"]
                ]
                df["Season"] = season
                json_file.close()
                seasons_df = pd.concat([seasons_df, df])
        my_logger.info("Data loaded")

    except Exception as error:
        my_logger.exception(f"Data extraction Error:. {error}")
        sys.exit(1)

    return seasons_df


def get_positions_tables(seasons) -> pd.DataFrame:
    try:
        seasons["WinnerPoints"] = 3
        seasons["TiePoint"] = 1

        positions_table = pd.DataFrame()

        for season in seasons["Season"].unique():

            season_df = seasons.query(f"Season == '{season}'")

            home_winners = season_df.query("FTHG > FTAG")
            home_winners = home_winners[
                ["Season", "Div", "HomeTeam", "WinnerPoints"]
            ].rename(columns={"HomeTeam": "Team", "WinnerPoints": "Points"})

            visitor_swinners = season_df.query("FTAG > FTHG")
            visitor_swinners = visitor_swinners[
                ["Season", "Div", "AwayTeam", "WinnerPoints"]
            ].rename(columns={"AwayTeam": "Team", "WinnerPoints": "Points"})

            home_tie = season_df.query("FTHG == FTAG")
            home_tie = home_tie[["Season", "Div", "HomeTeam", "TiePoint"]].rename(
                columns={"HomeTeam": "Team", "TiePoint": "Points"}
            )

            visitors_tie = season_df.query("FTHG == FTAG")
            visitors_tie = visitors_tie[
                ["Season", "Div", "AwayTeam", "TiePoint"]
            ].rename(columns={"AwayTeam": "Team", "TiePoint": "Points"})

            final_scores = (
                pd.concat([home_winners, visitor_swinners, home_tie, visitors_tie])
                .groupby(by=["Season", "Div", "Team"])
                .sum()
                .sort_values(by=["Div", "Points"], ascending=False)
            )
            final_scores["Position"] = final_scores.rank(ascending=False).astype(
                "int64"
            )

            positions_table = pd.concat([positions_table, final_scores])
        my_logger.info("Positions tables loaded")

    except Exception as error:
        my_logger.exception(f"Data tranforming Error:. {error}")
        sys.exit(1)

    return positions_table


def get_top_referee(seasons) -> pd.DataFrame:
    try:
        top_referees = pd.DataFrame()

        for season in seasons["Season"].unique():

            season_df = seasons.query(f"Season == '{season}'")

            most_repeated_referee = season_df["Referee"].mode().to_frame()
            most_repeated_referee["Season"] = season

            top_referees = pd.concat([top_referees, most_repeated_referee])

        top_referees = top_referees.sort_values(by=["Season"], ascending=True)
        my_logger.info("Top referees loaded")

    except Exception as error:
        my_logger.exception(f"Data tranforming Error:. {error}")
        sys.exit(1)

    return top_referees


def get_most_thrashed(seasons) -> pd.DataFrame:
    try:
        most_thrashed = pd.DataFrame()

        for season in seasons["Season"].unique():
            season_df = seasons.query(f"Season == '{season}'")

            visitors_received_golas = season_df[
                ["Season", "Div", "AwayTeam", "FTHG"]
            ].rename(columns={"AwayTeam": "Team", "FTHG": "GoalsReceived"})
            locals_received_golas = season_df[
                ["Season", "Div", "HomeTeam", "FTAG"]
            ].rename(columns={"HomeTeam": "Team", "FTAG": "GoalsReceived"})

            total_goals_received = (
                pd.concat([visitors_received_golas, locals_received_golas])
                .groupby(by=["Season", "Div", "Team"])
                .sum()
                .sort_values(by=["Div", "GoalsReceived"], ascending=False)
            )

            thrashed_team = total_goals_received[
                total_goals_received.GoalsReceived
                == total_goals_received.GoalsReceived.max()
            ]

            most_thrashed = pd.concat([most_thrashed, thrashed_team])

        most_thrashed = most_thrashed.sort_values(by=["Season"], ascending=True)
        my_logger.info("Most thrashed teams loaded")

    except Exception as error:
        my_logger.exception(f"Data tranforming Error:. {error}")
        sys.exit(1)

    return most_thrashed
