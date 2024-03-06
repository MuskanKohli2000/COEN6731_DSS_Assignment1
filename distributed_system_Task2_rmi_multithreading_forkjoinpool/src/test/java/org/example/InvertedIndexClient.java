package org.example;



import java.rmi.Naming;

import java.util.List;

import java.util.Map;



public class InvertedIndexClient{

    public static void main(String[] args) {

        try {

            // Look up for the remote service

            InvertedIndexService service = (InvertedIndexService) Naming.lookup("rmi://127.0.0.1:8090/InvertedIndexService");



            // Call the remote method with the filename

            Map<String, List<Integer>> invertedIndex = service.getInvertedIndex("C:\\\\Users\\\\Dell\\\\Desktop\\\\Distributed Software\\\\Midterm\\\\distributed_system_Task2_rmi_multithreading_forkjoinpool\\\\src\\\\main\\\\resources\\\\sample_data.txt");
            
            System.out.println("Inverted index Forkjoinpool:");
            
            for(Map.Entry<String,List<Integer>> entry: invertedIndex.entrySet()) {
            	System.out.println(entry.getKey() +" :" +entry.getValue());
            }
            

            // Process the result and display Top-5 tokens with the most frequent appearance and their locations

            // You need to implement this part based on your requirement



        } catch (Exception e) {

            e.printStackTrace();

        }

    }

}




