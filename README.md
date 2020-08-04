# Mapreduce Count Average Bytes per request by IP-and Total Bytes by IP
-----------------------

## MapReduce

* Data processing layer in Hadoop. 
* Processing structured & Unstructured Data in Hadoop.

## Pros

* Best Performance 

## Cons 

* Hard to Extend 
* Lack of management tools
* Not suitable for real time processing
* very small community

## Input Directory

Input directory contains input files that will be processed by MapReduce to count average bytes per request by IP and total bytes by IP.

<img src="screenshots/input_directory.png"> <br/>

In the above screenshot, we can see an input directory (in) contains input file – 000000 provided with problem statement. <br/>


## 000000
 
<img src="screenshots/000000.png"> <br/>
 
In the above screenshot, we can see the content of the input file. <br/>

## Counters usage
 
<img src="screenshots/counters_usage.png"> <br/>

In the above screenshot, we can see that 65 counters are used with read and written bytes.

## Counters Output

<img src="screenshots/counters_output.png"> <br/>
 
In the above screenshot, we can see the output of the counters. <br/>

## Reading Compressed Content

<img src="screenshots/reading_compressed_contents.png"> <br/>

In the above screenshot, we can see the output contains IP, Average bytes & Total Bytes. </br>
Note: I used Bip2Codec because snappy is not supported by the latest versions of Hadoop. </br>


## Test Cases:

<img src="screenshots/test_cases.png"> <br/>
 
In the above screenshot, we can see that all 3 test cases have passed. <br/>

**Created by:** <br/>
**Name: Krishna Kumar Singh** <br/>
**Email: krishnaai265@gmail.com** <br/>
**Phone: +91-9368754996** 
