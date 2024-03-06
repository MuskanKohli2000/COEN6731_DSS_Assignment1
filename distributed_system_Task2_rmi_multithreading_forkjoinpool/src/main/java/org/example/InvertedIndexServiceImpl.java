package org.example;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class InvertedIndexServiceImpl extends UnicastRemoteObject implements InvertedIndexService {
    private ForkJoinPool pool;

    public InvertedIndexServiceImpl() throws RemoteException {
        super();
        // Initialize the pool with the number of available processors
        pool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
    }

    @Override
    public Map<String, List<Integer>> getInvertedIndex(String fileName) throws RemoteException {
        Map<String, List<Integer>> index = new HashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
            List<String> lines = new ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            // Submit a ForkJoinTask to handle processing of lines
            pool.invoke(new InvertedIndexTask(lines, 0, lines.size(), index));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getTop5Tokens(index);
    }

    // RecursiveAction to process lines and update index
    private static class InvertedIndexTask extends RecursiveAction {
        private static final int THRESHOLD = 100; // threshold for lines per task
        private List<String> lines;
        private int start;
        private int end;
        private Map<String, List<Integer>> index;

        public InvertedIndexTask(List<String> lines, int start, int end, Map<String, List<Integer>> index) {
            this.lines = lines;
            this.start = start;
            this.end = end;
            this.index = index;
        }

        @Override
        protected void compute() {
            if (end - start <= THRESHOLD) {
                processLines();
            } else {
                int mid = (start + end) / 2;
                InvertedIndexTask leftTask = new InvertedIndexTask(lines, start, mid, index);
                InvertedIndexTask rightTask = new InvertedIndexTask(lines, mid, end, index);
                invokeAll(leftTask, rightTask);
            }
        }

        private void processLines() {
            for (int i = start; i < end; i++) {
                String line = lines.get(i);
                String[] words = line.split("\\s+");
                int lineNumber = i + 1;
                for (String word : words) {
                    synchronized (index) {
                        index.computeIfAbsent(word, k -> new ArrayList<>()).add(lineNumber);
                    }
                }
            }
        }
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
            // Creating an instance of the service implementation
            InvertedIndexServiceImpl server = new InvertedIndexServiceImpl();

            // Create and start the RMI registry on port 8099
            LocateRegistry.createRegistry(8090);

            // Bind the remote object to the registry
            Naming.rebind("rmi://127.0.0.1:8090/InvertedIndexService", server);
            System.out.println("InvertedIndexService ready...");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}