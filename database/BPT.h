#pragma once
#include "common.h"
#include <iostream>
#include <mutex>

using namespace std;
#define BPTORDER    5       //the order of B+ TEMP_FAILURE_RETRY

class Node;
class BPtree;

class In_Node {
public:
    long long val;      // the value
    off_t off;      // the offset of this key in data files
    off_t idxoff;       // the offset ot this key in index files
    Node * master;      // the Node which this In_Node belong to

    In_Node();
    ~In_Node();
};

class Node {
public:
    int keynum ;        // the number of this Node if number >= BPTORDER it must be spilted
    In_Node * key[BPTORDER];       // the keys of this Node
    Node *childrens[BPTORDER + 1];      // the childrens of this Node
    Node *parent;           // the parent of this Node

    bool is_leaf;           // if true the childrens will be NULL if not the Next and Prev will be NULL
    Node *Next_Node;        // the Next Node
    Node *Prev_Node;        // the previous Node
    bool is_root;
    BPtree *ptree;

    Node();
    ~Node();

    // split (if keynum >= BPTORDER)
    void Node_split();

    // insert the new index which created by Node_split to this Node
    void insert_Index(long long key , off_t off , Node * left , Node * right);

    // borrow node from brother
    void borrow_Node();

    // merge two node when none of brothers has enough nodes  (always merge with left)
    void merge();

    // index node 's borrow function
    void index_borrow();

    // index node's merge
    void index_merge();

    // destroy node
    void destroy_node();
};

class BPtree {
public:
    Node * root;        // root Node
    int height;         // this tree's height
    std::mutex mu;       // the mutex that protect the tree

    BPtree();
    ~BPtree();

    In_Node * search_Node(Node * node , long long key);    

    // Find the node with the most similar value
    In_Node * search_Sim(Node * node , long long key);     

    // Insert the node
    void insert_Node(long long key , off_t off , off_t idxoff);    

    // Insert (when root is NULL)
    void insert_NULL(long long key , off_t off , off_t idxoff);
    
    // remove the node      return 0 success 1 not find -1 error
    int remove_Node(long long key);

    // delete the BPtree
    void destory_tree();
};
