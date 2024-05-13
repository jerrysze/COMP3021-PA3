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
        ExecutorService executor = Executors.newFixedThreadPool(numThread);
        for (String xmlID : xmlIDs) {
            ParserWorker worker = new ParserWorker(xmlID, xmlDirPath, id2ASTModules);
            executor.execute(worker);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
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
        List<Thread> threads = new ArrayList<>();
        ConcurrentLinkedQueue xmlIDsQueue = new ConcurrentLinkedQueue<>(xmlIDs);
        Runnable threadfunc = () -> {
            while (!xmlIDsQueue.isEmpty()) {
                String xmlID = (String) xmlIDsQueue.poll();
                ParserWorker worker = new ParserWorker(xmlID, xmlDirPath, id2ASTModules);
                worker.run();
            }
        };
        for (int i = 0; i < numThread; i++) {
            Thread thread = new Thread(threadfunc);
            threads.add(thread);
        }
        threads.forEach(Thread::start);
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
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
        // List<Object> results = new ArrayList<>();
        List<QueryWorker> workers = new ArrayList<>();
        // HashMap<String, ASTModule> id2ASTModules,
        //                       String queryID, String astID,
        //                       String queryName, Object[] args, int mode
        for (Object[] command : commands) {
            workers.add(new QueryWorker(id2ASTModules, (String)command[0], (String)command[1],
                    (String)command[2], (Object[])command[3], executionMode));
        }
        switch (executionMode) {
            case 0:
                executeCommandsSerial(workers);
                break;
            case 1: {
                executeCommandsParallel(workers);
                break;
            }

            case 2: {
                executeCommandsParallelWithOrder(workers);
                break;
            }
            default: break;
        }

        return allResults;
    }

    /**
     * TODO: Implement `executeCommandsSerial` to handle a list of `QueryWorker`
     *
     * @param workers a list of workers that should be executed sequentially
     */
    private void executeCommandsSerial(List<QueryWorker> workers) {
        for (QueryWorker worker : workers) {
            worker.run();
        }
        for (QueryWorker worker : workers) {
            allResults.add(worker.getResult());
        }
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
        for (QueryWorker worker : workers) {
            worker.run();
        }
        for (QueryWorker worker : workers) {
            try {
                worker.thread.join();
            } catch (InterruptedException e) {

            }
        }
        for (QueryWorker worker : workers) {
            allResults.add(worker.getResult());
        }
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
        CyclicBarrier cb = new CyclicBarrier(workers.size());
        for (QueryWorker worker : workers) {
            worker.barrier = cb;
        }
        for (QueryWorker worker : workers) {
            worker.run();
        }
        for (QueryWorker worker : workers) {
            try {
                worker.thread.join();
            } catch (InterruptedException e) {

            }
        }
        for (QueryWorker worker : workers) {
            allResults.add(worker.getResult());
        }
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
        Map<String, Object[]> loadCommand = new HashMap<>();
        List<Thread> threads = new ArrayList<>();
        int qsize = 0;
        for (Object[] command : commands) {
            if (command[2].equals("processXMLParsing")) {
                loadCommand.put((String)command[1], command);
            } else {
                qsize++;
            }
        }
        List<QueryWorker> queryWorkers = new ArrayList<>();
        CyclicBarrier cb = new CyclicBarrier(qsize);
        List<Object []> newOrder = new ArrayList<>();
        for (Object[] command : commands) {
            if (!command[2].equals("processXMLParsing")) {
                if (loadCommand.containsKey(command[1])) {
                    Object []loadArgs = loadCommand.get(command[1]);
                    CountDownLatch latch = new CountDownLatch(1);
                    newOrder.add(loadCommand.get(command[1]));
                    ParserWorker parserWorker = new ParserWorker((String)command[1], (String)((Object[])loadArgs[3])[0], id2ASTModules);
                    parserWorker.latch = latch;
                    threads.add(new Thread(parserWorker));
                    newOrder.add(command);
                    QueryWorker queryWorker = new QueryWorker(id2ASTModules, (String)command[0], (String)command[1],
                            (String)command[2], (Object[])command[3], 2);
                    queryWorker.latch = latch;
                    queryWorker.barrier = cb;
                    queryWorkers.add(queryWorker);
                }
            }
        }
        for (Thread thread : threads) {
            thread.start();
        }

        for (QueryWorker worker : queryWorkers) {
            worker.run();
            threads.add(worker.thread);
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {

            }
        }
        for (QueryWorker worker : queryWorkers) {
            allResults.add(worker.getResult());
        }


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
        Map<String, Object[]> loadCommand = new HashMap<>();
        for (Object[] command : commands) {
            if (command[2].equals("processXMLParsing")) {
                loadCommand.put((String)command[1], command);
            }
        }
        List<QueryWorker> queryWorkers = new ArrayList<>();
        List<ParserWorker> parserWorkers = new ArrayList<>();
        List<Object []> newOrder = new ArrayList<>();
        for (Object[] command : commands) {
            if (!command[2].equals("processXMLParsing")) {
                if (loadCommand.containsKey(command[1])) {
                    Object []loadArgs = loadCommand.get(command[1]);
                    CountDownLatch latch = new CountDownLatch(1);
                    newOrder.add(loadCommand.get(command[1]));
                    ParserWorker parserWorker = new ParserWorker((String)command[1], (String)((Object[])loadArgs[3])[0], id2ASTModules);
                    parserWorker.latch = latch;
                    parserWorkers.add(parserWorker);

                    newOrder.add(command);
                    QueryWorker queryWorker = new QueryWorker(id2ASTModules, (String)command[0], (String)command[1], (String)command[2],
                            (Object[])command[3], 0);
                    queryWorker.latch = latch;
                    queryWorkers.add(queryWorker);
                }
            }
        }
        Thread parse = new Thread(new Runnable() {
            @Override
            public void run() {
                for (ParserWorker worker : parserWorkers) {
                    worker.run();
                }
            }
        });
        Thread query = new Thread(new Runnable() {
            @Override
            public void run() {
                for (QueryWorker worker : queryWorkers) {
                    worker.run();
                }
            }
        });
        parse.start();
        query.start();
        try {
            parse.join();
            query.join();
        } catch (InterruptedException e) {

        }
        for (QueryWorker worker : queryWorkers) {
            allResults.add(worker.getResult());
        }
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
        Map<String, Object[]> loadCommand = new HashMap<>();
        for (Object[] command : commands) {
            if (command[2].equals("processXMLParsing")) {
                loadCommand.put((String)command[1], command);
            }
        }
        List<QueryWorker> queryWorkers = new ArrayList<>();
        List<ParserWorker> parserWorkers = new ArrayList<>();
        List<Object []> newOrder = new ArrayList<>();
        for (Object[] command : commands) {
            if (!command[2].equals("processXMLParsing")) {
                if (loadCommand.containsKey(command[1])) {
                    Object []loadArgs = loadCommand.get(command[1]);
                    CountDownLatch latch = new CountDownLatch(1);
                    newOrder.add(loadCommand.get(command[1]));
                    ParserWorker parserWorker = new ParserWorker((String)command[1], (String)((Object[])loadArgs[3])[0], id2ASTModules);
                    parserWorker.latch = latch;
                    parserWorkers.add(parserWorker);

                    newOrder.add(command);
                    QueryWorker queryWorker = new QueryWorker(id2ASTModules, (String)command[0], (String)command[1],
                            (String)command[2], (Object[])command[3], 0);
                    queryWorker.latch = latch;

                    queryWorkers.add(queryWorker);
                }
            }
        }
        ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue();
        for (ParserWorker worker : parserWorkers) {
            queue.add(worker);
        }
        for (QueryWorker worker : queryWorkers) {
            queue.add(worker);
        }
        List<Thread> threads = new ArrayList<>();
        Runnable threadFunc = () -> {
            while (!queue.isEmpty()) {
                Object obj = queue.poll();
                if (obj instanceof ParserWorker) {
                    ((ParserWorker)obj).run();
                } else {
                    QueryWorker queryWorker = (QueryWorker)obj;
                    queryWorker.run();
                }
            }
        };
        for (int i = 0; i < numThread; i++) {
            Thread thread = new Thread(threadFunc);
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {

            }
        }


        for (QueryWorker worker : queryWorkers) {
            allResults.add(worker.getResult());
        }
        return allResults;
    }
}
