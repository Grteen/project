#include "BPT.h"
using namespace std;

FILE * bptlog;
extern In_Node *p;

BPtree::BPtree() {
    this->height = 0;
    this->root = NULL;
}

BPtree::~BPtree() {}

Node::Node() {
    for (int i = 0 ; i < BPTORDER ; i++) {
        this->childrens[i] = NULL;
        this->key[i] = NULL;
    }
    this->childrens[BPTORDER] = NULL;
    this->is_leaf = false;
    this->Next_Node = NULL;
    this->parent = NULL;
    this->Prev_Node = NULL;
    this->is_root = false;
    this->ptree = NULL;
}

Node::~Node() {}

In_Node::In_Node() {
    this->master = NULL;
    this->off = 0;
    this->val = 0;
}

In_Node::~In_Node() {}

In_Node * BPtree::search_Node(Node * node , long long key) {
    int i = 0;
    if (node->is_leaf == true) {        // is leaf node
        for (i = 0 ; i < node->keynum ; i++) {
            if (key == node->key[i]->val) {        // find the target
                In_Node * res = new In_Node();
                res = node->key[i];
                p = node->key[i];
                return res;         // success
            }
        }
        return NULL;        // not find
    }
    else {      // not the leaf node
        i = 0;
        while (i < node->keynum) {
            if (key >= node->key[i]->val)
                i++;
            else if (key < node->key[i]->val) {
                In_Node * res = this->search_Node(node->childrens[i] , key);
                return res;     // success or return NULL
            }
        }
        if (i == node->keynum) {
            In_Node * res = this->search_Node(node->childrens[i] , key);
            return res;     // success or return NULL
        }
    }
    return NULL;        // not find
}

In_Node * BPtree::search_Sim(Node * node , long long key) {
    int i = 0;
    if (node->is_leaf == true) {        // is leaf node
        for (i = 0 ; i < node->keynum ; i++) {
            if (key < node->key[i]->val) {
                In_Node * res = new In_Node();
                if (i == 0) {
                    res = node->key[i];
                    return res;
                }
                else {
                    res = node->key[i - 1];
                    return res;
                }
            }
        }
        In_Node * res = new In_Node();
        res = node->key[i - 1];
        return res;
    }
    else {      // not the leaf node
        i = 0;
        while (i < node->keynum) {
            if (key > node->key[i]->val)
                i++;
            else if (key <= node->key[i]->val) {
                In_Node * res = this->search_Sim(node->childrens[i] , key);
                if (res == NULL) {
                    fprintf(bptlog , "search_Sim : search_Node not find\n");
                    exit(-1);
                }
                return res;     // success
            }
        }
        if (i == node->keynum) {        // this key is bigger than other ones in this npde
            In_Node * res = this->search_Sim(node->childrens[i] , key);
            if (res == NULL) {
                fprintf(bptlog , "search_Sim : search_Node not find\n");
                exit(-1);
            }
            return res;     // success
        }
    }
    return NULL;        // not find
}      

void BPtree::insert_Node(long long key , off_t off , off_t idxoff) {
    std::unique_lock<std::mutex> locker(this->mu);
    if (this->height == 0) {     // none node
        this->insert_NULL(key , off , idxoff);
        return;
    }
    // find the most similar value
    In_Node * target = this->search_Sim(this->root , key);
    if (target == NULL) {
        delete target;
        fprintf(bptlog , "insert_Node : search_Sim not find\n");
        exit(-1);
    };
    In_Node * res = new In_Node();
    res->val = key;
    res->off = off;
    res->idxoff = idxoff;
    if (key <= target->val) {
        for (int i = target->master->keynum - 1; i >= 0 ; i--)
            target->master->key[i + 1] = target->master->key[i];
        target->master->key[0] = res;           // insert success (the smallest)
    }
    else {
        for (int i = 0 ; i < target->master->keynum ; i++) {
            if (target->val == target->master->key[i]->val) {
                for (int k = target->master->keynum - 1 ; k >= i + 1 ; k--)
                    target->master->key[k + 1] = target->master->key[k];
                target->master->key[i + 1] = res;
                break;
            }
        }
    }
    res->master = target->master;
    target->master->keynum++;
    target->master->Node_split();
}

void BPtree::insert_NULL(long long key , off_t off , off_t idxoff) {
    Node * node = new Node();
    node->is_leaf = true;
    In_Node * innode = new In_Node();
    innode->val = key;
    innode->off = off;
    innode->idxoff = idxoff;
    innode->master = node;
    node->key[0] = innode;
    node->keynum++;
    node->ptree = this;
    this->root = node;
    this->root->is_leaf = true;
    this->root->is_root = true;
    this->height++;
}

void Node::Node_split() {
    if (this->keynum >= BPTORDER) {     // satify the split requirements
        Node * left = new Node();
        Node * right = new Node();
        if (this->is_leaf == false) {       // not the leaf node
            left->keynum = BPTORDER / 2;
            right->keynum = BPTORDER / 2;
            for (int i = 0 ; i < left->keynum ; i++) {
                left->key[i] = this->key[i];
                left->key[i]->master = left;
                left->childrens[i] = this->childrens[i];
                left->childrens[i]->parent = left;
            }
            left->ptree = this->ptree;
            left->childrens[left->keynum] = this->childrens[left->keynum];
            left->childrens[left->keynum]->parent = left;
            left->parent = this->parent;
            for (int i = 0 ; i < right->keynum ; i++) {
                right->key[i] = this->key[i + left->keynum + 1];
                right->key[i]->master = right;
                right->childrens[i] = this->childrens[i + left->keynum + 1];
                right->childrens[i]->parent = right;
            }
            right->ptree = this->ptree;
            right->childrens[right->keynum] = this->childrens[right->keynum + left->keynum + 1];
            right->childrens[right->keynum]->parent = right;
            right->parent = this->parent;
        }
        else {          // is the leaf node
            left->keynum = BPTORDER / 2;
            right->keynum = BPTORDER / 2 + 1;
            for (int i = 0 ; i < left->keynum ; i++) {
                left->key[i] = this->key[i];
                left->key[i]->master = left;
            }
            for (int i = 0 ; i < right->keynum ; i++) {
                right->key[i] = this->key[i + left->keynum];
                right->key[i]->master = right;
            }
            left->ptree = this->ptree;
            right->ptree = this->ptree;
            left->is_leaf = true;
            right->is_leaf = true;
            left->parent = this->parent;
            right->parent = this->parent;

            left->Next_Node = right;
            left->Prev_Node = this->Prev_Node;
            if (this->Prev_Node != NULL)
                this->Prev_Node->Next_Node = left;

            right->Prev_Node = left;
            right->Next_Node = this->Next_Node;
            if (this->Next_Node != NULL)
                this->Next_Node->Prev_Node = right;
        }

        int middle = BPTORDER / 2;
        // if this node is root
        if (this->is_root == true) {
            Node * newroot = new Node();
            newroot->key[0] = this->key[middle];
            newroot->keynum = 1;
            newroot->key[0]->master = newroot;
            newroot->childrens[0] = left;
            newroot->childrens[1] = right;
            newroot->childrens[0]->parent = newroot;
            newroot->childrens[1]->parent = newroot;
            newroot->is_root = true;
            newroot->ptree = this->ptree;
            newroot->ptree->height++;
            newroot->ptree->root = newroot;
        }
        else                // the middle key go to the parent to be the index
            this->parent->insert_Index(this->key[middle]->val , this->key[middle]->off , left , right);
        // this node is useless now
        delete this;
    }
}

void Node::insert_Index(long long key , off_t off , Node *left , Node *right) {
    In_Node * index = new In_Node();
    index->val = key;
    index->off = off;
    int i = 0;
    for (i = 0 ; i < this->keynum ; i++) {          // find the suitable location
        if (key < this->key[i]->val) {
            break;
        }
    }   
    for (int k = this->keynum - 1 ; k >= i ; k--) {         // move to create room
        this->key[k + 1] = this->key[k];
        this->childrens[k + 2] = this->childrens[k + 1];
    }
    // change the points
    this->key[i] = index;
    this->childrens[i] = left;
    this->childrens[i + 1] = right;
    index->master = this;
    this->keynum++;
    // if keynum >= BPTORDER if must be split too
    this->Node_split();
}

int BPtree::remove_Node(long long key) {
    // find the target
    std::unique_lock<std::mutex> locker(this->mu);
    In_Node * target = this->search_Node(this->root , key);
    if (target == NULL) {
        fprintf(bptlog , "remove_Node : search_Node not find key : %lld" , key);
        delete target;
        return 1;       // not find
    }

    for (int i = 0 ; i < target->master->keynum ; i++) {
        if (target == target->master->key[i]) {      // find it
            for (int k = i ; k < target->master->keynum ; k++)
                target->master->key[k] = target->master->key[k + 1];
        }
    }

    target->master->keynum--;
    // After deletion, if the conditions are not met , it must borrow node from it's brothers
    target->master->borrow_Node();
    return 0;       // success
}


int condition = (BPTORDER + 1) / 2 - 1;
void Node::borrow_Node() {
    if (this->keynum >= condition || this->is_root == true)
        return;         // OK
    Node * leftb = NULL;
    Node * rightb = NULL;
    int nodei = 0;
    for (int i = 0 ; i <= this->parent->keynum ; i++) {
        if (this->parent->childrens[i] == this) {
            if (i != 0)        // has the leftbrother
                leftb = this->parent->childrens[i - 1];
            if (i != this->parent->keynum)          // has the rightbrother
                rightb = this->parent->childrens[i + 1];
            nodei = i;
            break;
        }
    }

    if (leftb != NULL && leftb->keynum > condition) {       // has the enough node to be borrowed
        for (int i = this->keynum - 1 ; i >= 0 ; i--) {       // make room for left
            this->key[i + 1] = this->key[i];
        }
        this->key[0] = leftb->key[leftb->keynum - 1];       // borrow the node
        this->keynum++;
        leftb->key[leftb->keynum - 1] = NULL;
        leftb->keynum--;
        this->parent->key[nodei - 1] = this->key[0];        // change the parent key
    }
    else if (rightb != NULL && rightb->keynum > condition) {
        this->key[this->keynum] = rightb->key[0];       // borrow the node
        this->keynum++;
        for (int i = 0 ; i < rightb->keynum ; i++)      // rightb's key move to right location
            rightb->key[i] = rightb->key[i + 1];
        rightb->keynum--;
        this->parent->key[nodei] = rightb->key[0];      // change the parent key
    }
    else {              // none of them has enough nodes
        this->merge();
    }
}

void Node::merge() {
    Node * leftb = NULL;
    Node * rightb = NULL;
    int nodei = 0;
    for (int i = 0 ; i <= this->parent->keynum ; i++) {
        if (this->parent->childrens[i] == this) {
            if (i != 0)        // has the leftbrother
                leftb = this->parent->childrens[i - 1];
            if (i != this->parent->keynum)          // has the rightbrother
                rightb = this->parent->childrens[i + 1];
            nodei = i;
            break;
        }
    }

    if (leftb != NULL) {        // always merge with left
        Node * res = new Node();
        for (int i = 0 ; i < leftb->keynum ; i++) {
            res->key[i] = leftb->key[i];
            res->key[i]->master = res;
        }
        for (int i = 0 ; i < this->keynum ; i++) {
            res->key[i + leftb->keynum] = this->key[i];
            res->key[i + leftb->keynum]->master = res;
        }
        for (int i = nodei - 1 ; i < this->parent->keynum ; i++) {
            this->parent->key[i] = this->parent->key[i + 1];        // delete the parent in_node
        }
        for (int i = nodei ; i <= this->parent->keynum ; i++) {
            this->parent->childrens[i] = this->parent->childrens[i + 1];
        }
        this->parent->keynum--;

        this->parent->childrens[nodei - 1] = res;
        res->parent = this->parent;
        res->keynum = this->keynum + leftb->keynum;
        res->is_leaf = true;
        res->Prev_Node = leftb->Prev_Node;
        res->Next_Node = this->Next_Node;
        res->ptree = this->ptree;
        if (leftb->Prev_Node != NULL)
            leftb->Prev_Node->Next_Node = res;
        if (this->Next_Node != NULL)
            this->Next_Node->Prev_Node = res;

        if (this->parent->keynum >= condition)
            return;         // over
        else 
            res->parent->index_borrow();
    }
    else {          // leftb is NULL
        Node * res = new Node();
        for (int i = 0 ; i < this->keynum ; i++) {
            res->key[i] = this->key[i];
            res->key[i]->master = res;
        }
        for (int i = 0 ; i < rightb->keynum ; i++) {
            res->key[i + this->keynum] = rightb->key[i];
            res->key[i + this->keynum]->master = res;
        }
        for (int i = nodei ; i < this->parent->keynum ; i++) {
            this->parent->key[i] = this->parent->key[i + 1];
        }
        for (int i = nodei + 1 ; i <= this->parent->keynum ; i++) {
            this->parent->childrens[i] = this->parent->childrens[i + 1];
        }
        this->parent->keynum--;
        this->parent->childrens[nodei] = res;
        res->parent = this->parent;
        res->keynum = this->keynum + rightb->keynum;
        res->is_leaf = true;
        res->ptree = this->ptree;
        res->Prev_Node = this->Prev_Node;
        res->Next_Node = rightb->Next_Node;
        if (rightb->Next_Node != NULL)
            rightb->Next_Node->Prev_Node = res;
        if (this->Prev_Node != NULL)
            this->Prev_Node->Next_Node = res;

        if (this->parent->keynum >= condition)
            return;         // over
        else 
            res->parent->index_borrow();
    }
}

void Node::index_borrow() {
    if (this->keynum >= condition || this->is_root == true)
        return;         // OK;

    Node * leftb = NULL;
    Node * rightb = NULL;
    int nodei = 0;
    for (int i = 0 ; i <= this->parent->keynum ; i++) {
        if (this->parent->childrens[i] == this) {
            if (i != 0)        // has the leftbrother
                leftb = this->parent->childrens[i - 1];
            if (i != this->parent->keynum)          // has the rightbrother
                rightb = this->parent->childrens[i + 1];
            nodei = i;
            break;
        }
    }

    if (leftb != NULL && leftb->keynum > condition) {      // make room for new innode
        this->childrens[this->keynum + 1] = this->childrens[this->keynum];
        for (int i = this->keynum - 1 ; i >= 0 ; i--) {     
            this->key[i + 1] = this->key[i];        
            this->childrens[i + 1] = this->childrens[i];
        }
        this->key[0] = this->parent->key[nodei - 1];
        this->childrens[0] = leftb->childrens[leftb->keynum];
        this->parent->key[nodei - 1] = leftb->key[leftb->keynum - 1];
        leftb->key[leftb->keynum - 1] = NULL;
        leftb->childrens[leftb->keynum] = NULL;
        leftb->keynum--;
        this->keynum++;
    }
    else if (rightb != NULL && rightb->keynum > condition) {
        this->key[this->keynum] = this->parent->key[nodei];
        this->childrens[this->keynum + 1] = rightb->childrens[0];
        this->parent->key[nodei] = rightb->key[0];
        for (int i = 0 ; i < rightb->keynum ; i++) {
            rightb->key[i] = rightb->key[i + 1];
            rightb->childrens[i] = rightb->childrens[i + 1];
        }
        rightb->childrens[rightb->keynum] = rightb->childrens[this->keynum + 1];
        rightb->keynum--;
        this->keynum++;
    }           // none of them has enought nodes
    else {
        this->index_merge();
    }
}

void Node::index_merge() {
    Node * res = new Node();
    Node * leftb = NULL;
    Node * rightb = NULL;
    int nodei = 0;
    for (int i = 0 ; i <= this->parent->keynum ; i++) {
        if (this->parent->childrens[i] == this) {
            if (i != 0)        // has the leftbrother
                leftb = this->parent->childrens[i - 1];
            if (i != this->parent->keynum)          // has the rightbrother
                rightb = this->parent->childrens[i + 1];
            nodei = i;
            break;
        }
    }

    if (leftb != NULL) {        // always merge with left
        for (int i = 0 ; i < leftb->keynum ; i++) {
            res->key[i] = leftb->key[i];
            res->key[i]->master = res;
            res->childrens[i] = leftb->childrens[i];
            res->childrens[i]->parent = res;
        }
        res->childrens[leftb->keynum] = leftb->childrens[leftb->keynum];
        res->childrens[leftb->keynum]->parent = res;

        // parent node down
        res->key[leftb->keynum] = leftb->parent->key[nodei - 1];
        res->key[leftb->keynum]->master = res;

        for (int i = 0 ; i < this->keynum ; i++) {
            res->key[leftb->keynum + 1 + i] = this->key[i];
            res->key[leftb->keynum + 1 + i]->master = res;
            res->childrens[leftb->keynum + 1 + i] = this->childrens[i];
            res->childrens[leftb->keynum + 1 + i]->parent = res; 
        }
        res->childrens[leftb->keynum + 1 + this->keynum] = this->childrens[this->keynum];
        res->childrens[leftb->keynum + 1 + this->keynum]->parent = res;

        res->keynum = leftb->keynum + this->keynum + 1;
        
        for (int i = nodei - 1 ; i < this->parent->keynum ; i++) {
            this->parent->key[i] = this->parent->key[i + 1];
            this->parent->childrens[i + 1] = this->parent->childrens[i + 2]; 
        }
        this->parent->keynum--;
        res->ptree = this->ptree;
    }
    else if (rightb != NULL) {
        for (int i = 0 ; i < this->keynum ; i++) {
            res->key[i] = this->key[i];
            res->key[i]->master = res;
            res->childrens[i] = this->childrens[i];
            res->childrens[i]->parent = res;
        }
        res->childrens[this->keynum] = this->childrens[this->keynum];
        res->childrens[this->keynum]->parent = res;

        // parent node down
        res->key[this->keynum + 1] = this->parent->key[nodei];
        res->key[this->keynum + 1]->master = res;

        for (int i = 0 ; i < rightb->keynum ; i++) {
            res->key[this->keynum + 1 + i] = rightb->key[i];
            res->key[this->keynum + 1 + i]->master = res;
            res->childrens[this->keynum + 1 + i] = rightb->childrens[i];
            res->childrens[this->keynum + 1 + i]->parent = res;
        }
        res->childrens[this->keynum + rightb->keynum + 1] = rightb->childrens[rightb->keynum];
        res->childrens[this->keynum + rightb->keynum + 1]->parent = res;

        res->keynum = this->keynum + rightb->keynum + 1;

        for (int i = nodei ; i < this->parent->keynum ; i++) {
            this->parent->key[i] = this->parent->key[i + 1];
            this->parent->childrens[i + 1] = this->parent->childrens[i + 2];
        }
        this->parent->keynum--;
        res->ptree = this->ptree;
        nodei++;        // deal with the following questions
    }

    // is root and keynum is 0
    // tree down
    if (this->parent->keynum == 0 && this->parent->is_root == true) {
        this->ptree->height--;
        delete this->ptree->root;
        this->ptree->root = res;
        res->is_root = true;
    }
    else {
        this->parent->childrens[nodei - 1] = res;
        res->parent = this->parent;
        // check the parent
        res->parent->index_borrow();
    }
}

void BPtree::destory_tree() {
    std::unique_lock<std::mutex> locker(this->mu);
    this->root->destroy_node();
}

void Node::destroy_node() {
    if (this->is_leaf) {
        for (int i = 0 ; i < this->keynum ; i++)
            delete this->key[i];
    }
    else {
        for (int i = 0 ; i <= this->keynum ; i++)
            this->childrens[i]->destroy_node();
        for (int i = 0 ; i < this->keynum ; i++)
            delete this->key[i];
    }
}