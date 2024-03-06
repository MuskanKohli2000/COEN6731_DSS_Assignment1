package org.example;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Collectors;

public class InvertedIndexServiceImpl extends UnicastRemoteObject implements InvertedIndexService {
    private ExecutorService executorService;

    public InvertedIndexServiceImpl() throws RemoteException {
        super();
        executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public Map<String, List<Integer>> getInvertedIndex(String fileName) throws RemoteException {
    	executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        Map<String, List<Integer>> index = new ConcurrentHashMap<>();
        List<Future<Void>> futures = new ArrayList<>();
        
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            String line;
            int lineNumber = 1;
            while ((line = reader.readLine()) != null) {
                final String currentLine = line;
                final int currentLineNumber = lineNumber;
                Callable<Void> task = () -> {
                    String[] words = currentLine.split("\\s+");
                    for (String word : words) {
                        index.computeIfAbsent(word, k -> new ArrayList<>()).add(currentLineNumber);
                    }
                    return null;
                };
                Future<Void> future = executorService.submit(task);
                futures.add(future);
                lineNumber++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        
        

        executorService.shutdown();
        return getTop5Tokens(index);
        
       
    }
    
    private Map<String, List<Integer>> getTop5Tokens(Map<String, List<Integer>> index) {
        List<Entry<String, List<Integer>>> sortedEntries = new ArrayList<>(index.entrySet());
        // Sort entries by the number of occurrences in descending order
        sortedEntries.sort((entry1, entry2) -> entry2.getValue().size() - entry1.getValue().size());

        Map<String, List<Integer>> top5Tokens = new LinkedHashMap<>();
        int count = 0;
        for (Entry<String, List<Integer>> entry : sortedEntries) {
            if (count >= 5) break;
            top5Tokens.put(entry.getKey(), entry.getValue());
            count++;
        }
        return top5Tokens;
        
    }

    public static void main(String[] args) {
        try {
            InvertedIndexServiceImpl server = new InvertedIndexServiceImpl();
            LocateRegistry.createRegistry(8090);
            Naming.rebind("rmi://127.0.0.1:8090/InvertedIndexService", server);
            System.out.println("InvertedIndexService ready...");
            
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}