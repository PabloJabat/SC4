Map Matching for SANSA using Spark
=============================

How to use
----------

To get the project just go to a folder and open a terminal on it. After that you can copy the following
commands and execute them in that terminal. 

```
git clone https://github.com/PabloJabat/SC4.git
cd SC4

```

Now that you already have the project you need to go to the downloaded file and go to 
src/main/scala/geo/algorithms/App. Open that file and go to line 90 to change the path in which you want to get
your results. Otherwise you can comment from 90 to 92 and uncomment from 94 to 98 just to check
if the code works. 

```
mvn clean package
````

After that you just to deploy it in a spark cluster:

```
spark-submit 
--class App 
--master <master URL> 
target/MapMatching-0.1.jar 
-m <path to osm data>
-i <path to the gps data>.txt

````
As you can see, you need to include the path for the jar and two 
additional parameters. `-m` allows us to specify the location of 
the open street map data and after `-i` we have to include the path
to the gps data.
