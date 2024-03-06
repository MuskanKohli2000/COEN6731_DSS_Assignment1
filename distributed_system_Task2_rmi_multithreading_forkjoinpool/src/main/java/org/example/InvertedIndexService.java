package org.example;



////RMI interface

//import java.rmi.Remote;

//import java.rmi.RemoteException;

//import java.util.Map;

//

//public interface InvertedIndexService extends Remote {

// // Returns the inverted index of words for a given text

// Map<String, Integer> getInvertedIndex(String text) throws RemoteException;

//}

import java.rmi.Remote;

import java.rmi.RemoteException;

import java.util.List;

import java.util.Map;



public interface InvertedIndexService extends Remote {

    Map<String, List<Integer>> getInvertedIndex(String fileName) throws RemoteException;

}

