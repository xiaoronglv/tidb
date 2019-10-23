
# Proposal: Optimizer Dynamic Sampling

- Authors: [Jian Yang](https://github.com/PiotrNewt) and [Ryan Lv](https://github.com/xiaoronglv)
- Mentor: [Haibin Xie](https://github.com/lamxTyler)
- Last updated:  2019-10-23
- Discussion at: [Google Doc](https://docs.google.com/document/d/18RcL3PmuBiCP463cUZ-WRYeQCT0YS4hj50mOdD2G6w0/)

## Abstract

Dynamic sampling provides the optimizer with statistics through sampling.

## Background

Statistics is the heart of optimizer, TiDB uses two strategies to maintain it. First, it can be triggered by  the SQL statement table `analyze table`. Second, TiDB automatically triggers the analyze table when some conditions are met[1].

Histogram and Count Min Sketch have been introduced in TiDB as a mechanism to help the optimizer to choose the query plan.

1. Histograms can be used for range selection or equality selection. 
	
	```sql
	// range selection
	select * 
	from employees 
	where birth_date > '1953-09-02';
	
	// equality selection
	select * 
	from employees 
	where birth_date = '1953-09-02';
	```

2. And Count Min Sketch is used for equality selection.

	```sql
	select * 
	from employees 
	where birth_date = '1953-09-02'; 
	```

> Note: The previous SQL statements are based on MySQL Employees Sample Database, you can download it [here](https://dev.mysql.com/doc/employee/en/).

However, histogram and count-min sketch may be missing or stale, take the following edge cases as examples:

- A new table may not get a chance to be analyzed, therefore optimizer has to use default statistic values[2], which are typically not accurate.
- More than 1/10 rows of a table have been updated/inserted/deleted. A `analyze table` is waiting to be performed.

Dynamic sampling is a perfect complement to cover those edge cases. 

## Proposal

This proposal is to implement dynamic sampling. The design contains three parts:

- Scenarios to use dynamic sampling
- Way to collect sample
- The dynamic sampling levels 


### Scenarios to use dynamic sampling

Dynamic sampling is fit in the following scenarios:

1. A table has been created, but not yet analyzed. The statistics are not available for the optimizer.

2. More than 1/10 rows have been updated/inserted. Statistics are stale, and the table is waiting to be analyzed. During this time window, it doesn't make sense to use stale statistics.

3. The Column in an expression is not isolated. "Isolating" the column means it should not be part of an expression or be inside a function in the query. [3]  

	For example, the following query can't take advantage of histograms or Count-Min sketches.

	```SQL
	// example1: column is part of expression
	select * 
	from employees 
	where emp_no + 1 = 10002;
	
	// example2: column is in the function
	select * 
	from employees 
	where mod(emp_no) = 3;
	```

4. Expressions in a query contain correlated columns[4]. Consider the following query:

	```
	select * 
	from cities 
	where name = "Shanghai" and zip_code = "20000"
	```  

### Way to collect samples

Regarding collecting samples, we borrow the idea from Fast Analyze.

1. Go through each region of a table.
2. Randomly pick rows from each region. 
3. Store the sample as a chunk.

### The dynamic sampling levels

Dynamic sampling has 12 levels(0-11), and is controlled by parameter `tidb_optimizer_dynamic_sampling`.

| Level | Description |
|:--|:--|
| 0 | Do not use dynamic sampling. |
| 1 | Apply dynamic sampling to all unanalyzed tables. |
| 2 | To be determined |
| 3 | To be determined |
| 4 | To be determined |
| 5 | To be determined |
| 6 | To be determined |
| 7 | To be determined |
| 8 | To be determined |
| 9 | To be determined |
| 10 | To be determined |
| 11 | To be determined |


## Rationale

### How other systems solve the same issue?

Oracle enables dynamic sampling by default, and specify the value as 2. It means when one or more tables in the expression have no statistics, dynamic sampling will be chosen.


| Database | Support dynamic sampling |
|--|--|
| MySQL | No |
| MariaDB | No |
| MySQL | No |
| Oracle | Yes |
| OceanBase | Yes |

### What is the disadvantage of this design?

Sampling takes time, which may slow down the whole process of optimization. 

### What is the impact of not doing this?

If we don't do this, the optimizer may make an unrealistic estimate when statistics are missing, invalid or stale.


## Compatibility

This proposal does not affect the compatibility.

## Implementation

### Who will do it?

Break down this feature into more concreted tasks and assign them to the following developer.

1. Add a parameter to turn on/off dynamic sampling. [\#12861](https://github.com/pingcap/tidb/pull/12861)(Ryan Lv)
2. Collect samples (Jian Yang)
3. Use samples to estimate the selectivity. (Ryan Lv)

### When to do it?

TiDB Hackathon. ( From 2019-10-25 to 2019-10-26  Shanghai Time)

---

1. [TiDB: Statistics](https://tidb.ebh.net/myroom/mycourse/438387.html)

2. [TiDB Source code: default statistic value](https://github.com/pingcap/tidb/blob/6d51ad33fd861c39b64a2f5d99db045d1fa0fb1d/statistics/table.go#L249-L251)

3. Book: High Performance MySQL: Chapter5. Index for High Performance P159

4. [Oracle: Dynamic sampling and its impact on the Optimizer](https://blogs.oracle.com/optimizer/dynamic-sampling-and-its-impact-on-the-optimizer)