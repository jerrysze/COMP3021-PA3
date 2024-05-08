package hk.ust.comp3021.parallel;

import java.util.*;

import hk.ust.comp3021.utils.*;


public class ParserWorker implements Runnable {
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

    /**
     * TODO: Implement {@link ParserWorker#run()} to load the AST whose ID is {@link ParserWorker#xmlID} from XML file 
     * and store the results in {@link ParserWorker#id2ASTModules}
     * 
     * Hint1: you can invoke {@link ASTParser#parse()} to load the file but notice that 
     * {@link ParserWorker#id2ASTModules} will be written by multiple threads simultaneously
     *
     * Hint2: the methods will be used in both Task 1 and Task 3, please carefully design it
     * to be compatible with both tasks
     */
    @Override
    public void run() {
        ASTParser astParser = new ASTParser();
        String xmlFilePath = xmlDirPath + "/" + xmlID + ".xml";

        ASTModule astModule = astParser.parse(xmlFilePath);

        synchronized (id2ASTModules) {
            id2ASTModules.put(xmlID, astModule);
        }
    }
}
