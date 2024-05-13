package hk.ust.comp3021.parallel;

import hk.ust.comp3021.query.*;
import hk.ust.comp3021.utils.*;

import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;


public class QueryWorker implements Runnable {
    public final HashMap<String, ASTModule> id2ASTModules;
    public String queryID;
    public String astID;
    public String queryName;
    public Object[] args;
    public int mode;
    public CountDownLatch latch;
    private Object result;

    public Thread thread;

    public QueryWorker(HashMap<String, ASTModule> id2ASTModules,
                       String queryID, String astID,
                       String queryName, Object[] args, int mode) {
        this.id2ASTModules = id2ASTModules;
        this.queryID = queryID;
        this.astID = astID;
        this.queryName = queryName;
        this.args = args;
        this.mode = mode;
    }

    public Object getResult() {
        return result;
    }

    public void run() {
        if (mode == 0) {
            runSerial();
        } else if (mode == 1) {
            runParallel();
        } else if (mode == 2) {
            runParallelWithOrder();
        }
    }

    /**
     * TODO: Implement `runSerial` to process current query command and store the results in `result`
     *
     * Hint1: you must invoke the methods in {@link QueryOnNode}, {@link QueryOnMethod} and {@link QueryOnClass}
     * to achieve the query
     */
    private void runSerial() {
        if (latch != null) {
            try {
                latch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        synchronized (id2ASTModules) {
            if (!id2ASTModules.containsKey(astID) && !(queryName.equals("calculateOp2Nums") || queryName.equals("processNodeFreq"))) {
                return;
            }
            result = switch (queryName) {
                case "findFuncWithArgGtN" ->
                         new QueryOnNode(id2ASTModules).findFuncWithArgGtN.apply(Integer.parseInt(astID));
                case "calculateOp2Nums" ->  new QueryOnNode(id2ASTModules).calculateOp2Nums.get();
                case "calculateNode2Nums" ->  new QueryOnNode(id2ASTModules).calculateNode2Nums.apply(astID);
                case "processNodeFreq" ->  new QueryOnNode(id2ASTModules).processNodeFreq.get();
                case "findEqualCompareInFunc" ->
                         new QueryOnMethod(id2ASTModules.get(astID)).findEqualCompareInFunc.apply((String) args[0]);
                case "findFuncWithBoolParam" ->
                         new QueryOnMethod(id2ASTModules.get(astID)).findFuncWithBoolParam.get();
                case "findUnusedParamInFunc" ->
                         new QueryOnMethod(id2ASTModules.get(astID)).findUnusedParamInFunc.apply((String) args[0]);
                case "findDirectCalledOtherB" ->
                         new QueryOnMethod(id2ASTModules.get(astID)).findDirectCalledOtherB.apply((String) args[0]);
                case "answerIfACalledB" ->
                        new QueryOnMethod(id2ASTModules.get(astID)).answerIfACalledB.test((String) args[0], (String) args[1]);
                case "findSuperClasses" ->
                         new QueryOnClass(id2ASTModules.get(astID)).findSuperClasses.apply((String) args[0]);
                case "haveSuperClass" ->
                         new QueryOnClass(id2ASTModules.get(astID)).haveSuperClass.apply((String) args[0], (String) args[1]);
                case "findOverridingMethods" ->
                        new QueryOnClass(id2ASTModules.get(astID)).findOverridingMethods.get();
                case "findAllMethods" ->
                         new QueryOnClass(id2ASTModules.get(astID)).findAllMethods.apply((String) args[0]);
                case "findClassesWithMain" ->  new QueryOnClass(id2ASTModules.get(astID)).findClassesWithMain.get();
                default->  "Unknown query: " + queryName;
            };
        }


    }

    /**
     * TODO: Implement `runParallel` to process current query command and store the results in `result` where
     * queryOnNode should be conducted with multiple threads
     *
     * Hint1: you must invoke the methods in {@link QueryOnNode}, {@link QueryOnMethod} and {@link QueryOnClass}
     * to achieve the query
     * Hint2: you can let methods in queryOnNode to work on single AST by changing the arguments when creating
     * {@link QueryOnNode} object
     * Hint3: please use {@link Thread} to achieve multi-threading
     * Hint4: you can invoke {@link QueryWorker#runSerial()} to reuse its logic
     */


    private void runParallel() {
        thread = new Thread(() -> {
            runSerial();
        });
        thread.start();
    }


    /**
     * TODO: Implement `runParallelWithOrder` to process current query command and store the results in `result` where
     * the current query should wait until the prerequisite has been computed
     *
     * Hint1: you must invoke the methods in {@link QueryOnNode}, {@link QueryOnMethod} and {@link QueryOnClass}
     * to achieve the query
     * Hint2: you can invoke {@link QueryWorker#runParallel()} to reuse its logic
     * Hint3: please use {@link Thread} to achieve multi-threading
     * Hint4: you can add new methods or fields in current class
     */
    /*
            haveSuperClass invokes findSuperClasses
            findOverridingMethods invokes findSuperClasses
            findAllMethods invokes findSuperClasses
            findClassesWithMain invokes findAllMethods
     */
    static final Set<String> T2NAMES = new HashSet<>(Set.of("HAVE_SUPERCLASS", "FIND_OVERRIDING_METHODS",
            "FIND_ALL_METHODS","FIND_CLASSES_WITH_MAIN"));
    public CyclicBarrier barrier;

    private void runParallelWithOrder() {
        thread = new Thread(() -> {
            if (T2NAMES.contains(queryName)) {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
                runSerial();
            } else {
                runSerial();
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        thread.start();
    }


}
