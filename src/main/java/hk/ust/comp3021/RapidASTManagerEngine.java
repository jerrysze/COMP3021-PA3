package hk.ust.comp3021;

import hk.ust.comp3021.parallel.*;
import hk.ust.comp3021.utils.*;
import java.util.concurrent.*;
import java.util.*;


public class RapidASTManagerEngine {
    private final HashMap<String, ASTModule> id2ASTModules = new HashMap<>();
    private final List<Object> allResults = new ArrayList<>();

    public HashMap<String, ASTModule> getId2ASTModule() {
        return id2ASTModules;
    }

    public List<Object> getAllResults() {
        return allResults;
    }

    /**
     * TODO: Implement `processXMLParsingPool` to load a list of XML files in parallel
     *
     * @param xmlDirPath the directory of XML files to be loaded
     * @param xmlIDs a list of XML file IDs
     * @param numThread the number of threads you are allowed to use
     *
     * Hint1: you can use thread pool {@link ExecutorService} to implement the method
     * Hint2: you can use {@link ParserWorker#run()} 
     */

    public void processXMLParsingPool(String xmlDirPath, List<String> xmlIDs, int numThread) {
        ExecutorService executorService = Executors.newFixedThreadPool(numThread);

        for (String xmlID : xmlIDs) {
            ParserWorker parserWorker = new ParserWorker(xmlID, xmlDirPath);
            executorService.submit(parserWorker);
        }

        executorService.shutdown();

        try {
            executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * TODO: Implement `processXMLParsingDivide` to load a list of XML files in parallel
     *
     * @param xmlDirPath the directory of XML files to be loaded
     * @param xmlIDs a list of XML file IDs
     * @param numThread the number of threads you are allowed to use
     *
     * Hint1: you can **only** use {@link Thread} to implement the method
     * Hint2: you can use {@link ParserWorker#run()}
     * Hint3: please distribute the files to be loaded for each thread manually and try to achieve high efficiency
     */
    public void processXMLParsingDivide(String xmlDirPath, List<String> xmlIDs, int numThread) {
        
    }

    /**
     * TODO: Implement `processCommands` to conduct a list of queries on ASTs based on execution mode
     *
     * @param commands a list of queries, you can refer to test cases to learn its format
     * @param executionMode mode 0 to mode 2
     *
     * Hint1: you need to invoke {@link RapidASTManagerEngine#executeCommandsSerial(List)}
     *                           {@link RapidASTManagerEngine#executeCommandsParallel(List)} 
     *                      and {@link RapidASTManagerEngine#executeCommandsParallelWithOrder(List)}
     */
    public List<Object> processCommands(List<Object[]> commands, int executionMode) {
        return allResults;
    }

    /**
     * TODO: Implement `executeCommandsSerial` to handle a list of `QueryWorker`
     *
     * @param workers a list of workers that should be executed sequentially
     */
    private void executeCommandsSerial(List<QueryWorker> workers) {
       
    }

    /**
     * TODO: Implement `executeCommandsParallel` to handle a list of `QueryWorker`
     *
     * @param workers a list of workers that should be executed in parallel
     * 
     * Hint1: you can **only** use {@link Thread} to implement the method
     * Hint2: you can use unlimited number of threads
     */
    private void executeCommandsParallel(List<QueryWorker> workers) {
        
    }

    /**
     * TODO: Implement `executeCommandsParallelWithOrder` to handle a list of `QueryWorker`
     *
     * @param workers a list of workers that should be executed in parallel with correct order
     *
     * Hint1: you can invoke {@link RapidASTManagerEngine#executeCommandsParallel(List)} to reuse its logic
     * Hint2: you can use unlimited number of threads
     * Hint3: please design the order of queries running in parallel based on the calling dependence of method
     *                in queryOnClass
     */
    private void executeCommandsParallelWithOrder(List<QueryWorker> workers) {
       
    }

    /**
     * TODO: Implement `processCommandsInterLeaved` to handle a list of commands
     *
     * @param commands a list of import and query commands that should be executed in parallel 
     *
     * Hint1: you can **only** use {@link Thread} to create threads
     * Hint2: you can use unlimited number of threads
     * Hint3: please design the order of commands, where for specific ID, AST load should be executed before query
     * Hint4: threads would write into/read from {@link RapidASTManagerEngine#id2ASTModules} at the same time, please
     *                 synchronize them carefully
     * Hint5: you can invoke {@link QueryWorker#run()} and {@link ParserWorker#run()}
     * Hint6: order of queries should be consistent to that in given commands, no need to consider 
     *                 redundant computation now
     */
    public List<Object> processCommandsInterLeaved(List<Object[]> commands) {
        
        return allResults;
    }


    /**
     * TODO: Implement `processCommandsInterLeavedTwoThread` to handle a list of commands
     *
     * @param commands a list of import and query commands that should be executed in parallel 
     *
     * Hint1: you can **only** use {@link Thread} to create threads
     * Hint2: you can only use two threads, one for AST load, another for query
     * Hint3: please design the order of commands, where for specific ID, AST load should be executed before query
     * Hint4: threads would write into/read from {@link RapidASTManagerEngine#id2ASTModules} at the same time, please
     *                 synchronize them carefully
     * Hint5: you can invoke {@link QueryWorker#run()} and {@link ParserWorker#run()}
     * Hint6: order of queries should be consistent to that in given commands, no need to consider 
     *                      redundant computation now
     */
    public List<Object> processCommandsInterLeavedTwoThread(List<Object[]> commands) {
        
        return allResults;
    }

    /**
     * TODO: (Bonus) Implement `processCommandsInterLeavedTwoThread` to handle a list of commands
     *
     * @param commands a list of import and query commands that should be executed in parallel 
     * @param numThread number of threads you are allowed to use
     *                 
     * Hint1: you can only distribute commands on your need
     * Hint2: please design the order of commands, where for specific ID, AST load should be executed before query
     * Hint3: threads would write into/read from {@link RapidASTManagerEngine#id2ASTModules} at the same time, please
     *                 synchronize them carefully
     * Hint4: you can invoke {@link QueryWorker#run()} and {@link ParserWorker#run()}
     */
    public List<Object> processCommandsInterLeavedFixedThread(List<Object[]> commands, int numThread) {
        // TODO: Bonus: interleaved parsing and query with given number of threads
        // TODO: separate parser tasks and query tasks with the goal of efficiency
        return allResults;
    }
}
