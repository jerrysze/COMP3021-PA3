# **COMP3021 Spring 2024 Java Programming Assignment 3 (PA3)**

:warning: Please make sure you always pull the **latest** version of the PA skeleton!

:heavy_exclamation_mark: **Updates** and **Skeleton Clarification**: 

1. `testParallelExecutionWithOrder` is changed to be easier to understand and deterministic. Please update your code accordingly.

## **Python AST Management System**

[AST (Abstract Syntax Tree)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) is a tree representation that represents the syntactic structure of source code. It is widely used in compilers and interpreters to reason about relationships between program elements. In this project, you are required to implement your own management system, named ASTManager, to parse and analyze Python ASTs.

### **Grading System**

PA3 aims to practice multithreading and parallel programming. Specifically, the goal of PA3 is to familiarize you with dividing and scheduling tasks for speed-up and avoiding redundant computation during loading and analyzing ASTs. **ASTManager** should be enhanced to support the following additional functionalities:

- Task 1: Parallel importing of XML files
- Task 2: Efficient Query Processing
- Task 3: Mixture XML Importing and Query
- Bonus Task: High-Efficient XML Manipulations

Similar to PA1 and PA2, each input is an XML file that represents a Python AST. The XML files used to test queries on nodes reside in `resources/pythonxmlPA1` while those for classes and methods are located in `resources/pythonxml`. Before task specification, we first explain the grading policy as follows for your reference so that you will get it.

| Item                                            | Ratio | Notes                                                        |
| ----------------------------------------------- | ----- | ------------------------------------------------------------ |
| Keeping your GitHub repository private          | 5%    | You must keep your repository **priavte** at all times.      |
| Having at least three commits on different days | 5%    | You should commit three times during different days in your repository |
| Code style                                      | 10%   | You get 10% by default, and every 5 warnings from CheckStyle deducts 1%. |
| Public test cases (Task 1-3 + Bonus Task)       | 30%   | (# of passing tests / # of provided tests) * 30%             |
| Hidden test cases (Task 1-3 + Bonus Task)       | 50%   | (# of passing tests / # of provided tests) * 50%             |

Note that in PA3, we would check 1) the correctness, 2) the performance, as well as 3) the number of active threads for your implementation. Without finishing the task within the given time or if the number of threads is not correct, you will not get full marks even with correct results.

### Task Description

The specifications of each task are shown below. 

#### Preface

In PA2, we have mentioned 14 kinds of queries on AST, including 4 queries on AST node (`QueryOnNode`), 5 queries on methods (`QueryOnMethod`) and 5 queries on class (`QueryOnClass`). In PA3, we are still working on these 14 kinds of queries. To cope with multi-thread programming, we have slightly modified their implementations but functionalities are consistent, which will be detailed later. You can also refer to `ASTManagerEngine` to check their usage.

The implementations of these 14 queries are given in `lib/ASTQuery.jar` package. You do not need to re-implement them. The focus of PA3 is to build a framework to parallelize existing functionalities, thus you can treat given implementations as black-box. 

In PA3, we establish a new parallel framework `RapidASTManagerEngine` to replace the original `ASTManagerEngine`. The class has two fields, `id2ASTModules` organizes the mapping between the ID to the corresponding parsed ASTs and `allResults` stores the query results you need to produce in parallel.

Note that you can only use `synchronized`, `notifyAll`, `wait`, `Thread`, `semaphore`, `ReentrantLock` and `ExecutorService` to synchronize the threads in PA3.

#### Task 1: Build a Parallel Framework and Support Parallel Import of XML Files

In task 1, to support parallel import of XML files, we use the class `ParserWorkers` under the directory `parallel` to organize essential information for XML loading, including the ID of AST to be loaded `xmlID`, the absolute path of XML file directory `xmlDirPath`, and the mapping to store the loaded AST `id2ASTModules`.

You can notice `ParserWorker` is a subclass of interface `Runnable`. Please implement the method `run` of `Runnable` interface to load AST of the given ID and store the results to `id2ASTModules`. You can invoke `ASTParser.parser` but please caution on the concurrent writing to the global mapping `id2ASTModules`. 

```Java
public class ParserWorker implements Runnable {
    @Override
    public void run() {
        // Loading XML file in Parallel
    }
}
```

After finishing the `ParserWorder`, please implement `processXMLParsingPool` and `processXMLParsingDivide` methods of `RapidASTManagerEngine` to launch specific numbers of threads to load XML files. You must ensure that all the threads have equal access to the shared data structures.

For `processXMLParsingPool`, you can manage threads with a threading pool using package `ExecutorService`. For `processXMLParsingDivide`, you can only use `Thread` and need to manually distribute the XML files to be loaded for each thread. Try to use all the threads effectively to perform the search operations with high performance. 


#### Task 2: Support Customized Parallelization on Query Processing

In the previous PAs, each time you need to process on command once a time for three kinds of queries, i.e., `queryOnNode`, `queryOnMethod` and `queryOnClass`. In task 2, you are requested to process a stream of queries in parallel.

We use `QueryWorker` under `parallel` to universally manage the different commands and their inputs/outputs. The meaning of each field of `QueryWorker` is outlined below.
- `id2ASTModules`: global mapping managing all loaded AST so far
- `queryID`: the index of the current query inside all given queries
- `astID`: the ID of AST to be queried
- `queryName`: the name of the query, including the 14 queries you wrote in PA2, from `findFuncWithArgGtN` to `findClassesWithMain`.
- `args`: the inputs of query, its size depends on the specific query to be conducted, for instance, `answerIfACalledB` query has two inputs.
- `result`: universal structure to store the query results, which depends on the specific query to be conducted, for instance, `answerIfACalledB` returns boolean value.
- `mode`: query mode, which will be elaborated on later.

To perceive the efficiency benefit from parallelism straightforwardly, you are requested to implement 3 query modes in task 2. As you can see, the `run` methods check the current query mode and invoke corresponding methods. 

```Java
public class QueryWorker implements Runnable {
    public void run() {
        // Implement the following three modes
        if (mode == 0) {
            runSerial();
        } else if (mode == 1) {
            runParallel();
        } else if (mode == 2) {
            runParallelWithOrder();
        }
    }
}
```
**Mode `0` Sequential Execution**. You need to implement `runSerial` to achieve specific query `queryName`. Later mode 0 will be invoked sequentially on a list of queries.

For queries on methods and classes, the results are computed on AST `astID` while for queries on nodes, the results are computed based on all ASTs inside `id2ASTModules`. You need to first parse `args` to prepare correct arguments, then invoke the corresponding query function based on the `queryName`. Finally, remember to store the query results in `result`.

**Mode `1` Parallel Execution**. You need to implement `runParallel` to achieve specific queries in parallel. Later mode 1 will be invoked by multiple threads on a list of queries. 

Note that except for `queryOnNode`, all remaining queries are already performed on the single AST. Thus, the difference between mode `0` and mode `1` is that you need to distribute the `queryOnNode` task to multiple threads where each thread handles one AST, then you assemble their results to form the final one. Here, you must invoke the original methods in `queryOnNode` but you can modify their inputs to make each thread only handle one AST.

**Mode `2` Optimized Parallel Execution.** You need to implement `runParallelWithOrder` to achieve specific queries in parallel, and the execution orders of queries are not random anymore. As you have observed, one query on class could rely on the result of another query on class. For instance, `findOverridingMethods` depends on the class inheritance hierarchy computed by `findSuperClasses`. 

In given implementations of query on classes, the calling dependence is shown below. Please consider implementing `runParallelWithOrder` in a way that queries are well-ordered so that to prevent repeat computations.

```
haveSuperClass invokes findSuperClasses
findOverridingMethods invokes findSuperClasses
findAllMethods invokes findSuperClasses
findClassesWithMain invokes findAllMethods
```

We have designed an internal static field inside `queryOnClass` to memorize all query results computed so far. You only need to consider the execution order and how to synchronize threads. We would keep the track of the invocation times of each method in `queryOnClass` to test the correctness of your code.

Once all methods of `QueryWorker` are finished, please finish the `processCommands` method of `RapidASTManagerEngine`. Specifically, the method transforms the given query commands into a list of `QueryWorker` objects and schedules these workers based on the execution modes. 

```Java
public List<Object> processCommands(List<Object[]> commands, int executionMode) {
    // schedule workers based on commands and execution mode
    return allResults;
}
```

An example of a command list is below:

```Java
// query id, ast id, query name, query args
1, 18, findClassesWithMain, {}
2, 19, findClassesWithMain, {}
...
```

Based on the `exeutionMode`, you also need to implement the following three methods to process a list of `QueryWorker` objects and store their results into `allResults`. Note that orders of results should be consistent with queries.

- `executeCommandsSerial` processes a list of `QueryWorker` objects one by one 
- `executeCommandsParallel` launches unlimited threads to process `QueryWorker` in parallel.
- `executeCommandsParallelWithOrder` schedules `QueryWorker` workers based on their dependence and launches unlimited threads to process them in parallel.

#### Task 3: Interleaved XML Import and Query

In tasks 1 and 2, the XML import and query are handled separately. In task 3, you will receive a list of commands containing both XML import and query. Note that for each AST, its loading commands are not guaranteed to be issued earlier than its query.
You should correctly schedule these commands to prevent AST not found exceptions. For instance, for the following example command list, command 3 should be executed before command 1. Besides, the order of queries should be consistent to that in given commands.

More importantly, if there are no corresponding AST loading commands for a specific query, the query should be skipped.

```Java
// command id, ast id, command name, command args
1, 18, findClassesWithMain, {}
2, 19, findClassesWithMain, {}
3, 18, processXMLParsering, {"resources/pythonxml/"}
...
```

You are requested to achieve two versions. Still, remember to store the results in the shared variable `allResults`.

- `processCommandsInterLeaved` can launch unlimited threads
- `processCommandsInterLeavedTwoThread` can only launch two threads, one for AST import and another for AST query.

#### Bonus Task: High-Efficient Command Process

In the bonus task, you need to implement the same functionality as Task 3 using the given number of threads. The function to be implemented is shown below.

```java
public List<Object> processCommandsInterLeavedFixedThread(List<Object[]> commands, int numThread) {
    // TODO: Bonus: interleaved parsing and query with given number of threads
    // TODO: separate parser tasks and query tasks with the goal of efficiency
    return allResults;
}
```

Please carefully manage the tasks each thread should handle based on their orders. The number of commands given is arbitrary. You are encouraged to achieve as high efficiency as possible. We will sort the time costs of your implementations and give bonuses to the top 3 students who implement bonus tasks.

### What YOU need to do

We have marked the methods you need to implement using `TODO` in the skeleton. Specifically, please

- Fully implement the TODOs in the class `RapidASTManagerEngine`.
- Fully implement the TODOs in the class `ParserWorker`.
- Fully implement the TODOs in the class `QueryWorkder`.

**Note**: You can add more methods into the above three classes in your solution, but **DO NOT** modify other classes (except for spaces or new lines to pass the style check).

You need to follow the comments on the methods to be implemented in the provided skeleton. We have provided detailed descriptions and even several hints for these methods. 

For PA3, you cannot interact with `RapidASTManagerEngine` via the command line anymore. To convenience the testing and debugging, you can try to debug the test cases directly or add more debug information.

### How to TEST

We use JUnit test to validate the correctness of individual methods that you need to implement. 

Public test cases are released in `src/test/java/hk.ust.comp3021/parallel`. The mapping between public test cases and methods to be tested is shown below. 

Please try to test your code with `./gradlew test` before submission to ensure your implementation can pass all public test cases.

| Test Case                         | Target Method                                                |
| --------------------------------- | ------------------------------------------------------------ |
| `testParallelLoadingPool`         | `processXMLParsingPool` in Task 1                            |
| `testParallelLoadingDivide`       | `processXMLParsingDivide` in Task 1                          |
| `testSerialExecution`             | `processCommands` with mode 0 (`executeCommandsSerial`) in Task 2 |
| `testParallelExecution`           | `processCommands` with mode 1 (`executeCommandsParallel`) in Task 2 |
| `testParallelExecutionWithOrder`  | `processCommands` with mode 2 (`executeCommandsParallelWithOrder`) in Task 2 |
| `testInterleavedImportQuery`      | `processCommandsInterLeaved` in Task 3                       |
| `testInterleavedImportQueryTwo`   | `processCommandsInterLeavedTwoThread` in Task 3              |
| `testInterleavedImportQueryBonus` | `processCommandsInterLeavedFixedThread` in Task 3 (Bonus Task Only) |

You can fix the problem of your implementation based on the failed test cases.


### Submission Policy

Please submit your code on Canvas before the deadline **May 11, 2024, 23:59:59.** You should submit a single text file specified as follows:

- A file named `<itsc-id>.txt` containing the URL of your private repository at the first line. We will ask you to add the TAs' accounts as collaborators near the deadline.

For example, a student CHAN, Tai Man with ITSC ID `tmchanaa` having a repository at `https://github.com/tai-man-chan/COMP3021-PA3` should submit a file named `tmchanaa.txt` with the following content:

```txt
https://github.com/tai-man-chan/COMP3021-PA3
```

Note that we are using automatic scripts to process your submission on test cases rather than testing via the console manually. **DO NOT add extra explanation** to the file; otherwise, they will prevent our scripts from correctly processing your submission. 

**We will grade your submission based on the latest committed version before the deadline.** Please make sure all the amendments are made before the deadline and do not make changes after the deadline.

We have pre-configured a gradle task to check the style for you. You can run `./gradlew checkstyleMain` in the integrated terminal of IntelliJ to check style.

Before submission, please make sure that: 

1. Your code can be complied with successfully. Please try to compile your code with `./gradlew build` before submission. You will not get any marks for public/hidden test cases if your code cannot be successfully compiled.

2. Your implementation can pass the public test cases we provided in `src/test`.

3. Your implementation should not yield too many errors when running `./gradlew checkstyleMain`.

### Academic Integrity

We trust that you are familiar with the Honor Code of HKUST. If not, refer to [this page](https://course.cse.ust.hk/comp3021/#policy).

### Contact US

If you have any questions on the PA3, please email TA Wei Chen via wei.chen@connect.ust.hk

---

Last Update: April 21, 2024

