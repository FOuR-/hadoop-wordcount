## Hadoop WordCount  

* clone 代码   

``` 
$ git clone https://github.com/FOuR-/hadoop-wordcount.git   
```  
* mvn 打包   

```  
$ cd hadoop-wordcount   
$ mvn clean package  
```  
* 准备数据   

```  
$ vim dual.sql   
welcome the world
I'm loh
hello the world   
$ hadoop fs -mkdir /user/hadoop/wordcount/
$ hadoop fs -put dual.sql /user/hadoop/wordcount/  
```  
   
* run  

```  
$ hadoop jar hadoop-wordcount.jar com.test.App 
```  

* 结果  

```   
I'm	1
hello	1
loh	1
the	2
welcome	1
world	2  
```  


