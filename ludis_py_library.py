import pandas as pd
import os
import re
import requests
from io import BytesIO
#' Add together two numbers
#'
#' @param url url corresponding to datasets s3 location
#' @param headers headers containing a ludis api token
#' @param data_frame the dataframe to be written to ludis dataset
#' @returns return a httr::PUT reponse object
#' @export
def write_ludis_csv(df, url, headers) :
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    res = requests.put(url, headers=headers)
    res2 = requests.put(res.text, data=csv_buffer)
    return(res2)




#' Add together two numbers
#'
#' @param url url corresponding to datasets s3 location
#' @param headers headers containing a ludis api token
#' @returns a dataframe `df`
#' @export
def read_ludis_csv(url, headers):
    df = pd.read_csv(url, storage_options=headers)
    return(df)


#' Add together two numbers
#'
#' @param url url corresponding to datasets s3 location
#' @param headers headers containing a ludis api token
#' @returns a dataframe `df`
#' @export
def list_ludis_datasets(url, headers) :
    url = re.sub("ludisurl","listfiles", url)
    res = requests.get(url, headers=headers)
    data = res.json()

    return(data["contents"])

