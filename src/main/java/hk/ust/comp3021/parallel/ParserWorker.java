package hk.ust.comp3021.parallel;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import hk.ust.comp3021.utils.*;


public class ParserWorker implements Runnable {
    public CountDownLatch latch;
    private String xmlID;
    private String xmlDirPath;
    private final HashMap<String, ASTModule> id2ASTModules;

    public ParserWorker(String xmlID, String xmlDirPath, HashMap<String, ASTModule> id2ASTModules) {
        this.xmlID = xmlID;
        this.xmlDirPath = xmlDirPath;
        this.id2ASTModules = id2ASTModules;
    }

    public String getXmlID() {
        return xmlID;
    }

    @Override
    public void run() {
        ASTParser parser = new ASTParser(xmlDirPath + "python_" + xmlID + ".xml");
        parser.parse();
        if (!parser.isErr()) {
            synchronized (id2ASTModules) {
                id2ASTModules.put(xmlID, parser.getASTModule());
            }
        }

        if (latch != null) {
            latch.countDown();
        }
    }
}
