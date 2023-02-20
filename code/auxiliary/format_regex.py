# Databricks notebook source
# Converts a standard comma separated list string into regex union, formatting requirements safely allow for incomplete code stems to be used, i.e. "C1" for all codes starting with those two characters
def convert_list_to_regex_string(input_string, sep=","):
  return f"'{'|'.join(['^' + x for x in input_string.split(sep)] + [',' + x for x in input_string.split(sep)])}'"
