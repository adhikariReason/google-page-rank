## Reason Adhikari
## Custom Beam Java Program that uses Google page ranking algorithm to rank subset of custom pages.

#### Project Main Repo: [Team Main Repo](https://github.com/washingdone/big-data-project")
#### Adhikari's project wiki page: [Adhikari - Wiki Page](https://github.com/washingdone/big-data-project/wiki/Reason's-Comments")
#### Link To My Folder(Workspace): [adhikari - directory](https://github.com/washingdone/big-data-project/tree/main/adhikari")

## Description
The program assigns page ranks to each page in custom pages inside folder "web04". This is done in three steps (jobs) and each estep has a mapper and reducer. 
* Job1
  * Mapper: Extracts links from each pages store it as KV pair, key being page name and value being page voted for. Mapper Output is input for Reducer.
  * Reducer: Groups by Key into KV pair, key being page that is voted for and value being list of pages that has voted to the key-page. Reducer's output is input for Job2 mapper
* Job2
  * Mapper: Assigns page rank value to each pages. Mapper's output is input for reducer.
  * Reducer: Updates the value accordingly using formula. Reducer's output should match the format of job2 mapper's initial input so that the job2 can be iterated multiple times.
* Job3
  * Finalizes the process above and make sure results can be implemented.



#### Pipeline:
```
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.springbeam.adhikari.PageRankReason ` -D exec.args="--inputFile=README.md --output=adhikariOutput" -P direct-runner
```
