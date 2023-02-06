## ---- include = FALSE---------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>"
)

## ----setup--------------------------------------------------------------------
library(worldmet)

## ----getmeta------------------------------------------------------------------
getMeta()

## ----getmetai, echo=FALSE, out.width="100%"-----------------------------------
worldmet::getMeta(returnMap = TRUE)

## ----getmeta2-----------------------------------------------------------------
getMeta(lat = 51.5, lon = 0)

## ----getmeta2i, echo=FALSE, out.width="100%"----------------------------------
worldmet::getMeta(lat = 51.5, lon = 0, returnMap = TRUE)

