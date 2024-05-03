#' Add together two numbers
#'
#' @param url url corresponding to datasets s3 location
#' @param headers headers containing a ludis api token
#' @param data_frame the dataframe to be written to ludis dataset
#' @returns return a httr::PUT reponse object
#' @export
write_ludis_csv <- function(data_frame, url, headers) {
  csv_data <- capture.output(write.csv(data_frame, row.names = F))

  response <- httr::PUT(url = url,
                        httr::add_headers(.headers=headers))
  res_text <- content(response, "text")
  response <- httr::PUT(url = res_text, body = csv_data)

  return(response)
}

#' Add together two numbers
#'
#' @param url url corresponding to datasets s3 location
#' @param headers headers containing a ludis api token
#' @returns a dataframe `df`
#' @export
read_ludis_csv <- function(url, headers) {
  res <- httr::GET(url = url, httr::add_headers(.headers=headers))

  df <- readr::read_csv(rawToChar(res$content))

  return(df)
}

#' Add together two numbers
#'
#' @param url url corresponding to datasets s3 location
#' @param headers headers containing a ludis api token
#' @returns a dataframe `df`
#' @export
list_ludis_datasets <- function(url, headers) {
  url <- gsub("ludisurl","listfiles", url)
  res <- httr::GET(url = url, httr::add_headers(.headers=headers))
  df <- jsonlite::fromJSON(rawToChar(res$content))

  return(df$contents)
}
