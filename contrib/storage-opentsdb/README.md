# drill-storage-openTSDB

Implementation of TSDB storage plugin. Plugin uses REST API to work with TSDB. 

For more information about openTSDB follow this link <http://opentsdb.net>

There is list of required params:

* metric
* start
* aggregator

optional param is: 

* downsample

List of supported aggregators

<http://opentsdb.net/docs/build/html/user_guide/query/aggregators.html>

List of supported time 

<http://opentsdb.net/docs/build/html/user_guide/query/dates.html>

Params must be specified in FROM clause of the query separated by commas. For example

`openTSDB.(metric=metric_name, start=4d-ago, aggregator=sum)`

Supported queries for now are listed below:

```
SELECT * FROM openTSDB. `(metric=warp.speed.test, start=47y-ago, aggregator=sum)` 
```
Return aggregated elements from `warp.speed.test` table since 47y-ago 

```
SELECT * FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago)`
```
Return aggregated elements from `warp.speed.test` table

```
SELECT `timestamp`, sum(`aggregated value`) FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago)` GROUP BY `timestamp`
```
Return aggregated and grouped value by standard drill functions from `warp.speed.test table`, but with the custom aggregator

```
SELECT * FROM openTSDB.`(metric=warp.speed.test, aggregator=avg, start=47y-ago, downsample=5m-avg)`
```
Return aggregated data limited by downsample