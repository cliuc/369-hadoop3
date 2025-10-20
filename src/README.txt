NAME: CHRIS LIU

We have three MapReduce Hadoop tasks for Apache logs and CSV file with hostnames. Our input files are in the input folder, which contain access.log and hostname_country.csv files necessary for the program to run. Then each task procedure will have a separate object directory with key-value pairs.

In the first task, the map reads access.log and then emits the key-value pair and country names are loaded in to map hostnames to countries. The reducer then aggregates counts by country, sorted in descending order.

In the second task,we again read and emit key-value pairs where the key is coountry & URL, and the value is 1. We do a map-side join. The reducer then aggregates counts by pair sorted alphabetically.

The first task reads and emits key-value pairs where key is URL and values are country of visitor. The reducer collects country names for each URl and outputs URL with a list of countries.