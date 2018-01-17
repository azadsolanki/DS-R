# ---------------------------- Spark R installation -------------------------------------

# Clear environment

rm(list = ls())

# installing sparklyr

install.packages("sparklyr")
library(sparklyr)

# connect for map-reducing jobs, how?
# connects to a new Spark instance for map-reducing. Let's use the commands now:

spark_install() #optional install for the latest spark version
sc <- spark_connect(master = "local")

###
# Success! You’re now up and running with sparklyr.
# However, let's see whats going on...
# This has set up a “local instance” of Spark on on your machine, which probably means that there won’t be much “breaking into smaller tasks”
# that can be done if your machine is a standard desktop or laptop.
# But, what if you had access to an appropriate computer cluster, how would you connect then?
# well, all you would need to do is change that master = "local" in the spark_connect command above to point to your server and you’d be good to go!

# ---------------------------- Getting spark cluster ready -------------------------------------

# install the package for nycflight dataset and then load it

install.packages(nycflights13)
library(nycflights13)
data(flights)

# copy the data into the spark cluster
# Our cluster is on our local machine, hence the master = "local" in the command when we set it up, but this could be elsewhere - for example a big supercomputer somewhere else.

library(dplyr) 
flights_tbl <- copy_to(sc, nycflights13::flights, "flights")

# Let's put some light on the command above...
# we have copied the data set flights in (::) the nycflights13 package into our cluster sc, and then we have called the table flights. Finally we saved a link to this as flights_tbl.


# optional check - do we have the tables in our cluster?
src_tbls(sc)
# now, we are setup, we can use all the dplyr functions to explore the data


# ---------------------------- Data manipulation -------------------------------------
# apply filter
flights_tbl %>% filter(year == 2013)

# Console output
## Source: lazy query [?? x 19]
# Database: spark_connection
#year month   day dep_t… sched… dep_d… arr_… sche… arr_d… carr… flig… tail… orig… dest  air_… dist…
#<int> <int> <int>  <int>  <int>  <dbl> <int> <int>  <dbl> <chr> <int> <chr> <chr> <chr> <dbl> <dbl>
#1  2013     1     1    517    515   2.00   830   819  11.0  UA     1545 N142… EWR   IAH   227    1400
#2  2013     1     1    533    529   4.00   850   830  20.0  UA     1714 N242… LGA   IAH   227    1416
#3  2013     1     1    542    540   2.00   923   850  33.0  AA     1141 N619… JFK   MIA   160    1089
#4  2013     1     1    544    545  -1.00  1004  1022 -18.0  B6      725 N804… JFK   BQN   183    1576
#5  2013     1     1    554    600  -6.00   812   837 -25.0  DL      461 N668… LGA   ATL   116     762
#6  2013     1     1    554    558  -4.00   740   728  12.0  UA     1696 N394… EWR   ORD   150     719
#7  2013     1     1    555    600  -5.00   913   854  19.0  B6      507 N516… EWR   FLL   158    1065
#8  2013     1     1    557    600  -3.00   709   723 -14.0  EV     5708 N829… LGA   IAD    53.0   229
#9  2013     1     1    557    600  -3.00   838   846 - 8.00 B6       79 N593… JFK   MCO   140     944
#10  2013     1     1    558    600  -2.00   753   745   8.00 AA      301 N3AL… LGA   ORD   138     733
# ... with more rows, and 3 more variables: hour <dbl>, minute <dbl>, time_hour <dbl>

# view of year and origin colums
flights_tbl %>% select(year, origin)

# Condole output
## Source: lazy query [?? x 2]
# Database: spark_connection
#year origin
#<int> <chr>
#1  2013 EWR
#2  2013 LGA
#3  2013 JFK
#4  2013 JFK
#5  2013 LGA
#6  2013 EWR
#7  2013 EWR
#8  2013 LGA
#9  2013 JFK
#10  2013 LGA
# ... with more rows

# mean of delayed flights
flights_tbl %>% 
  group_by(origin) %>% 
  summarise(mean_delay = mean(dep_delay))

# Source: lazy query [?? x 2]
# Database: spark_connection
#origin mean_delay
#<chr>       <dbl>
#1 JFK          12.1
#2 LGA          10.3
#3 EWR          15.1

# creating a dataframe  
delay <- flights_tbl %>% 
  group_by(tailnum) %>%
  summarise(count = n(), dist = mean(distance), delay = mean(arr_delay)) %>%
  filter(count > 20, dist < 2000, !is.na(delay)) %>% 
  collect
  #noticed something? 
  # what is that collect command doing here? 
  #Well, Up to that point, all the work is done on the “cluster” – i.e., in Spark. We have a pointer to it, 
  #but no dataframe on our “system” – i.e., in RStudio. Once we run collect it grabs a copy back to the 
  #system for the ggplot to work on.

#lets plot  
library(ggplot2)
ggplot(delay, aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area(max_size = 2)



# ---------------------------- Data manipulation 1.1 -------------------------------------
#Dealing with Logical and Categorical variables in sparklyR

# what's the difference between sparklyr and dplyr? Well, in sparklyr there is only one type of variable - numeric variable.
# that means we will have to find a new ways to deal with categorical variables. That's why sparklyr has a series of functions called feature transform functions.
# these all starts with FT and have an underscore (FT_).

# optional check of the data
flights_tbl

# Let’s say that we want to have a variable called long_flight that tells us if a flight in our NYC flights dataset is long or not.
# We will define “long” as greater than 1500 miles and “short” as less than 1500 miles.

# calling ft_binarizer() function. This takes the following form:
# ft_binarizer("variable to convert", "name of new variable", cutoff value)

# represented long as 1 and short as 0
flights_tbl %>%
ft_binarizer("distance", "long_flight", threshold = 1500) %>%
select(distance, long_flight)

# Console output
## Source: lazy query [?? x 2]
# Database: spark_connection
#distance long_flight
#<dbl>       <dbl>
#1     1400        0
#2     1416        0
#3     1089        0
#4     1576        1.00
#5      762        0
#6      719        0
#7     1065        0
#8      229        0
#9      944        0
#10      733        0
# ... with more rows

# If we drag this back to RStudio with collect(), then we will have to convert the numbers to long/short like this

# dragging back to Rstudio
flights_tbl %>%
ft_binarizer("distance", "long_flight", threshold = 1500) %>%
select(distance, long_flight) %>%
collect() %>%
mutate(long_flight2 = ifelse(long_flight == 0, "short", "long"))

# Console output
## A tibble: 336,776 x 3
#distance long_flight long_flight2
#<dbl>       <dbl> <chr>
#1     1400        0    short
#2     1416        0    short
#3     1089        0    short
#4     1576        1.00 long
#5      762        0    short
#6      719        0    short
#7     1065        0    short
#8      229        0    short
#9      944        0    short
#10      733        0    short
# ... with 336,766 more rows


# how about multivariables instead of binary?
# use ft_bucketizer and give splits

flights_tbl %>%
ft_bucketizer("distance", "distance_cat",
splits = c(0,500, 1500, Inf)) %>%
select(distance, distance_cat)

# Console output
# Source: lazy query [?? x 2]
# Database: spark_connection
#distance distance_cat
#<dbl>        <dbl>
#1     1400         1.00
#2     1416         1.00
#3     1089         1.00
#4     1576         2.00
#5      762         1.00
#6      719         1.00
#7     1065         1.00
#8      229         0
#9      944         1.00
#10      733         1.00
# ... with more rows





