#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import csv
import json
import pandas as pd
import numpy as np
from zoomtools import solr
from tld import get_fld


def csv_to_df(file):
    """
    :param file: csv file
    :return: returns csv data to df and prints df.info
    """
    df = pd.read_csv(file)
    # print information about columns in dataframe
    print(df.info())
    return df


# def expand_list(df, list_column, new_column):
#     # takes len of size of lists in columns
#     lens_of_lists = df[list_column].apply(len)
#     # takes len of df
#     origin_rows = range(df.shape[0])
#
#     destination_rows = np.repeat(origin_rows, lens_of_lists)
#     non_list_cols = (
#       [idx for idx, col in enumerate(df.columns)
#        if col != list_column]
#     )
#     expanded_df = df.iloc[destination_rows, non_list_cols].copy()
#     expanded_df[new_column] = (
#       [item for items in df[list_column] for item in items]
#       )
#     expanded_df.reset_index(inplace=True, drop=True)
#     return expanded_df


def clean_url(url_str):
    """
    :param url_str:
    :return: returns a url str w/o wwww or http/s in front
    """
    temp = ''
    # checks for http/s & www. in url and remove
    if 'https://www.' in url_str:
        temp += url_str.replace('https://www.', '')
    elif 'http://www.' in url_str:
        temp += url_str.replace('http://www.', '')
    elif 'https://' in url_str:
        temp += url_str.replace('https://', '')
    elif 'http://' in url_str:
        temp += url_str.replace('http://', '')
    elif 'www.' in url_str:
        temp += url_str.replace('www.', '')
    else:
        temp += url_str.split('/')[0]

    return temp


def clean_df(df_1, df_2):
    # dropping empty column at the end of df_1
    df_1.drop([''], axis=1)
    # join people records to new df_1 from df_2
    clean = pd.merge(df_1, df_2[['c_id', 'number_of_silver_records_in_person_index', 'number_of_records_in_person_index'
                                 ]], on='c_id', how='left')
    # sort df by people records
    clean = clean.sort_values(by=['number_of_silver_records_in_person_index', 'number_of_records_in_person_index'],
                              ascending=False)
    return clean


def proper_company_name(m_df, d_url_column, d_company_column, d_company_ext_names):
    suggested_name_lst = []
    for index, row in m_df.iterrows():

        co_url = clean_url(row[d_url_column])
        display_name = row[d_company_column]
        # if stripped domain and d_name are the same
        if co_url == display_name.lower():
            other_names = row[d_company_ext_names]
            # removes the domain from temp list of suggested names
            temp_lst = [json.loads(obj)["value"] for obj in other_names if co_url != json.loads(obj)["value"]]
            # check for empty list - no other name's to chose from returns an empty string
            if len(temp_lst) is not 0:
                current_suggested_name = temp_lst[0]
                # refine
                suggested_name = refine_naming_suggestion(current_suggested_name, temp_lst)
            else:
                suggested_name = ''
        else:
            suggested_name = 'NO NAME SUGGESTED'
        suggested_name_lst.append(suggested_name)
    return suggested_name_lst


def refine_naming_suggestion(current_suggestion, other_suggestion_lst):
    sugg = ''
    # if current suggestion contains www
    if 'www.' in current_suggestion:
        other_suggestion_lst.remove(current_suggestion)
        if len(other_suggestion_lst) > 0:
            new_sugg = other_suggestion_lst[0]
            return new_sugg
        else:
            return sugg
    else:
        return current_suggestion


# reading in csv as df
domain_df = csv_to_df('stored_snowflake_query.csv')

# solr join to additional fields
all_fields_df = solr.join('c', 'c_id', domain_df['c_id'],
                          fl='c_id,d_company,company,display_name,d_url,cpy_status, d_company_ext_names:[protobuf]')

# clean and process df
main_df = clean_df(all_fields_df, domain_df)


proper_company_name(main_df, 'd_url', 'd_company', 'd_company_ext_names')
# adding new column with suggested name
main_df['suggested_name'] = proper_company_name(main_df, 'd_url', 'd_company', 'd_company_ext_names')

# write results to csv
# main_df.to_csv('suggested_name_1st_pass.csv', index=False)

# write smaller file
# cpy_results = main_df[['c_id', 'd_company', 'display_name', 'suggested_name']]
# cpy_results.to_csv('trimmed_results_2pass.csv', index=False)

# cpy_results[cpy_results['suggested_name'] == ''].index

# index df to see how many rows have a domain in
main_df[main_df['suggested_name'] == '']
# main_df[main_df['suggested_name'].str.contains('www.') | main_df['suggested_name'].str.contains('\.') &  ~main_df['suggested_name'].str.contains('\ ')]
