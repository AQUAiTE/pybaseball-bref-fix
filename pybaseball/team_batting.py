from typing import List, Optional

import pandas as pd
from bs4 import BeautifulSoup

from . import cache
from .datasources.fangraphs import fg_team_batting_data
from .datasources.bref import BRefSession

session = BRefSession()

# This is just a pass through for the new, more configurable function
team_batting = fg_team_batting_data


@cache.df_cache()
def team_batting_bref(team: str, start_season: int, end_season: Optional[int]=None) -> pd.DataFrame:
    """
    Get season-level Batting Statistics for Specific Team (from Baseball-Reference)

    ARGUMENTS:
    team : str : The Team Abbreviation (i.e. 'NYY' for Yankees) of the Team you want data for
    start_season : int : first season you want data for (or the only season if you do not specify an end_season)
    end_season : int : final season you want data for
    """
    if start_season is None:
        raise ValueError(
            "You need to provide at least one season to collect data for. Try team_batting_bref(season) or team_batting_bref(start_season, end_season)."
        )
    if end_season is None:
        end_season = start_season

    url = "https://www.baseball-reference.com/teams/{}".format(team)

    raw_data = []
    headings: Optional[List[str]] = None
    for season in range(start_season, end_season+1):
        print("Getting Batting Data: {} {}".format(season, team))
        stats_url = "{}/{}.shtml".format(url, season)
        response = session.get(stats_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find_all('table', {'class': 'sortable stats_table'})[0]

        if headings is None:
            headings = [row.text.strip() for row in table.find_all('th')[1:28]]

        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            cols = [ele.text.strip() for ele in cols]
            cols = [col.replace('*', '').replace('#', '') for col in cols]  # Removes '*' and '#' from some names
            cols = [col for col in cols if 'Totals' not in col and 'NL teams' not in col and 'AL teams' not in col]  # Removes Team Totals and other rows
            cols.insert(2, season)
            raw_data.append([ele for ele in cols[0:]])

    assert headings is not None
    headings.insert(2, "Year")
    data = pd.DataFrame(data=raw_data, columns=headings) # [:-5]  # -5 to remove Team Totals and other rows
    data = data.dropna()  # Removes Row of All Nones

    return data

@cache.df_cache()
def season_batting_bref(start_season: int, end_season: Optional[int]=None) -> pd.DataFrame:
    """
    Get per-team season Batting Statistics from Baseball-Reference's Standard Batting page

    ARGUMENTS:
    start_season : int : The first season you want data for (or only season if end_season unspecified)
    end_season: int : The final season you want data for
    """
    if start_season is None:
        raise ValueError(
            "You need to provide at least one season to collect data for. Try season_batting_bref(season) or season_batting_bref(start_season, end_season)."
        )
    if end_season is None:
        end_season = start_season
    
    url = "https://www.baseball-reference.com/leagues/majors/{}-standard-batting.shtml"

    raw_data = []
    headings: Optional[List[str]] = None

    for season in range(start_season, end_season + 1):
        print("Getting per-team batting data: {}".format(season))
        season_url = url.format(season)
        response = session.get(season_url)
        soup = BeautifulSoup(response.content, 'html.parser')

        table = soup.find('table', id = 'teams_standard_batting')
        if not table:
            print('Table not found for {}'.format(season))
            continue
        
        if headings is None:
            headings = [th.get('data-stat') for th in table.find('thead').find_all('th')]
        
        rows  = table.find('tbody').find_all('tr')
        for row in rows:
            cols = []

            th = row.find('th', {'data-stat': 'team_name'})
            team_name = th.text.strip()

            # Skip the league average
            if team_name == 'League Average':
                continue

            a_tag = th.find('a')
            team_abbrev = a_tag.get('href').split('/')[2]
    
            cols.extend([season, team_abbrev, team_name])
            cols.extend([td.text.strip() for td in row.find_all('td')])
            raw_data.append(cols)

        
    assert headings is not None
    # Adjust for the additional columns
    headings = ['season', 'team_abbrev', 'team_name'] + headings[1:]
    data = pd.DataFrame(data=raw_data, columns=headings)
    data.dropna()

    return data