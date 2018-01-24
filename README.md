Map Matching for SANSA using Spark
=============================

How to use
----------

```
git clone https://github.com/PabloJabat/SC4.git
cd SC4

mvn clean package
````

After that you just to deploy it in a spark cluster:

```
spark-submit 
--class MapMatching 
--master <master URL> 
target/MapMatching-0.1.jar 
-m /home/pablo/DE/DataSets/osm_data.nt 
-i /home/pablo/DE/DataSets/taxi_gps_10000000.txt

````
As you can see, you need to include the path for the jar and two 
additional parameters. `-m` allows us to specify the location of 
the open street map data and after `-i` we have to include the path
to the gps data. The gps data must have latitude, longitude information. 