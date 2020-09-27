/**********************************************************/
/*              Programmed by Gerard Monells              */
/*               gerardmonells@hotmail.com                */
/**********************************************************/

#ifndef _AVLTREE_HPP
#define _AVLTREE_HPP
#include "wdbi.h"
#include <math.h>
//#include <iostream>
#include <list>
using namespace std;

typedef unsigned int uint;
class AVLtree {
public:
  AVLtree(DataTable *dt);
  AVLtree(const AVLtree& a);
  AVLtree& operator=(const AVLtree& a);
  ~AVLtree();
  void add_item(int x);
  void DeleteNode(int x);
  void FixItem(int x);
  int *search(int x);
  int *search(void **ptr);
  uint size();
//  void preorder();
  void inorder(int *obf);
//  void postorder();

private:
  struct node {
    node() {};
    node(int x, node* l, node* r, uint h) {
      item = x;
      left = l;
      right = r;
      height = h;
    }
    int item;
    node* left;
    node* right;
    uint height;
  };
#ifdef AVL_TREE_FIX_MEM
node *nodearr;
int arrtop;
#endif
  DataTable *dt;
  node* root;
  uint num_nodes;
/*
  static AVLtree<T>::node* AVLtree<T>::copy_nodes(node *n);
  static void AVLtree<T>::destroy_nodes(node *n);
  static AVLtree<T>::node* AVLtree<T>::add_node(node* n, T x);
  static uint AVLtree<T>::max(uint a, uint b);
  static uint AVLtree<T>::height(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate_left(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate2_left(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate_right(node* n);
  static AVLtree<T>::node* AVLtree<T>::rotate2_right(node* n);
  static bool AVLtree<T>::search(node* n, T x);
  static void AVLtree<T>::preorder(node* n);
  static void AVLtree<T>::inorder(node* n);
  static void AVLtree<T>::postorder(node* n);
  */
  node* copy_nodes(node *n);
  node* add_node(node* n, int x);
  void del_node(node* RtNode,node* n,node *n_father);
  int  deleteFromNode(node *pRtDel);
  int  DeleteNode(node* n, int x);
  node* del_fixup(node* n);
  void FixItem(node *n,int x);
  void destroy_nodes(node *n);
  //uint max(uint a, uint b);
  uint height(node* n);
  node* rotate_left(node* n);
  node* rotate2_left(node* n);
  node* rotate_right(node* n);
  node* rotate2_right(node* n);
  int * search(node* n, int x);
  int * search(node* n, void **ptr);
  //void preorder(node* n);
  void inorder(node* n,int *obf,int &off);
  //void postorder(node* n);
  int compare(int p1,int p2) {
	  return dt->CompareSortRow(p1,p2);
//	  if(p1>p2) return 1;
//	  if(p1==p2) return 0;
//	  return -1;
  };
  int compare(int p1,void **ptr) {
	  return dt->CompareSortedColumn(p1,ptr);
//	  if(p1>p2) return 1;
//	  if(p1==p2) return 0;
//	  return -1;
  };
  
#ifdef AVL_TREE_FIX_MEM
  list<node*>  m_listDelNode;   
  node *n_new(int x, node* l, node* r, uint h) { 
    node *p = NULL;
    if(m_listDelNode.size() > 0)
    {
      node *pn = m_listDelNode.front();
      m_listDelNode.pop_front();
      p = pn;
    }
    else{
     p=nodearr+arrtop;
     arrtop++;
    }
  	p->item=x;p->left=l;p->right=r;p->height=h;
  	return p;
  }
  void n_delete(node *p) {
    p->left = NULL;
    p->right = NULL;
    p->height = 0;
    p->item = -1;
    m_listDelNode.push_back(p);
    /*empty now.*/
  };
#endif

public:
#ifdef AVL_TREE_FIX_MEM
  int GetAVLTreeLen(int &nUseNum)
  {
    int n = arrtop - m_listDelNode.size();
    nUseNum = n;
    return n*sizeof(node);
  }

  int GetAVLTree(char *pBuff,int nLen)
  {
    list<node*>::iterator It;
    It = m_listDelNode.begin();

    int nSize = m_listDelNode.size();
    node *p = nodearr;
    unsigned long  nSTArr = (unsigned long)nodearr;
    unsigned long  offset = 0;
    node *pSaveNode = NULL;
    int l = 0;
    bool bFindRoot = false;
    int nRootPos = 0;
    int nSkipNode = 0;
    for(int i=0;i<arrtop;i++)
    {
       if(l >= nLen)
       {
          return -1;
       }
       if(nSize == 0 || p != *It)
       {
          memcpy(pBuff+l,p,sizeof(node));
          pSaveNode = (node *)(pBuff + l);
          l = l + sizeof(node);
          offset = (unsigned long)pSaveNode->left - nSTArr - nSkipNode*sizeof(node);
          pSaveNode->left  =(node *)offset;
          offset = (unsigned long)pSaveNode->right - nSTArr - nSkipNode*sizeof(node);
          pSaveNode->right  =(node *)offset;
          if(bFindRoot == false && p != root)
          {
            nRootPos++;
          }
          else if(p == root){
            bFindRoot = true;
          }
          p++;
       }
       else{
          if(It != m_listDelNode.end())It++;
          nSize--;
          nSkipNode++;
       }
     
    }
    return nRootPos;
  }

  bool SetAVLTreeData(char *pBuff,int nNum,int nRootPos)
  {
      if(nNum  > dt->GetMaxRows())
      {
        return false;
      }
      node *p = nodearr;
      node *pAddr =  nodearr;
      memcpy(p,pBuff,nNum*sizeof(node));
      node *pReadNode = NULL;
      for(int i=0;i<nNum;i++)
      {
         pReadNode = p + i;
         pReadNode->left = (node *)((unsigned long)pAddr +  (unsigned long)pReadNode->left);
         pReadNode->right =(node *)((unsigned long)pAddr + (unsigned long)pReadNode->right);
      }
      root = p + nRootPos;
      arrtop = nNum;
      num_nodes = nNum;
      m_listDelNode.clear();
      return true;
  }
#else
  int GetAVLTreeLen(int &nUseNum)
  {
    return -1;
  }

  int GetAVLTree(char *pBuff,int nLen)
  {
    return false;
  }

  bool SetAVLTreeData(char *pBuff,int nNum,int nRootPos)
  {
    return false;
  }
#endif

};




AVLtree::AVLtree(DataTable *dt) {
  this->dt=dt;
  root = NULL;
  num_nodes = 0;
#ifdef AVL_TREE_FIX_MEM
  nodearr=new node[dt->GetMaxRows()];
  arrtop=0;
  m_listDelNode.clear();
#endif
}

AVLtree::AVLtree(const AVLtree& a) {
  root = copy_nodes(a.root);
  num_nodes = a.num_nodes;
  dt=a.dt;
}

AVLtree& AVLtree::operator=(const AVLtree& a) {
  destroy_nodes(root);
  root = copy_nodes(a.root);
  num_nodes = a.num_nodes;
  dt=a.dt;
  m_listDelNode = a.m_listDelNode;
  return *this;
}

AVLtree::~AVLtree() {
  destroy_nodes(root);
#ifdef AVL_TREE_FIX_MEM
  m_listDelNode.clear();
  delete [] nodearr;
#endif
}

void AVLtree::add_item(int x) {
  root = add_node(root, x);
  num_nodes++;
}

void AVLtree::DeleteNode(int x)
{
   DeleteNode(root,x);
   num_nodes--;
   FixItem(x);
}


void AVLtree::FixItem(int x)
{
  FixItem(root,x);
}

int *AVLtree::search(int x) {
  return search(root, x);
}

int *AVLtree::search(void **ptr) {
  return search(root, ptr);
}

uint AVLtree::size() {
  return num_nodes;
}
/*
void AVLtree::preorder() {
  preorder(root);
}
*/
void AVLtree::inorder(int *obf) {
  int off=0;
  inorder(root,obf,off);
}
/*
void AVLtree::postorder() {
  postorder(root);
}
*/
AVLtree::node* AVLtree::copy_nodes(node *n) {
  if (n == NULL) return NULL;
  node* p;
  node* l;
  node* r;
  l = copy_nodes(n->left);
  r = copy_nodes(n->right);
//  p = new node(n->item, l, r, n->height);
#ifdef AVL_TREE_FIX_MEM
  p = n_new(n->item, l, r, n->height);
#else 
  p = new node(n->item, l, r, n->height);
#endif
  return p;
}


#ifdef AVL_TREE_FIX_MEM
void AVLtree::destroy_nodes(node *n) 
{
    m_listDelNode.clear();
    arrtop = 0;
}
#else
void AVLtree::destroy_nodes(node *n) 
{
   if (n != NULL) {
    destroy_nodes(n->left);
    destroy_nodes(n->right);
    delete n;
  }
}
#endif


AVLtree::node* AVLtree::add_node(node* n, int x) {
  if (n == NULL) {
    node* p;

#ifdef AVL_TREE_FIX_MEM
    p = n_new(x, NULL, NULL, 1);
#else
    p = new node(x, NULL, NULL, 1);
#endif
    return p;
  } 
  else {
//    if (x < n->item) {
      if (compare(x,n->item)<0) {
      	n->left = add_node(n->left, x);
      	if (height(n->left) - height(n->right) == 2) {
//        if (x < n->left->item) {
	  if (compare(x,n->left->item)<0) {
          	n = rotate_right(n);
          } else {
          	n = rotate2_right(n);
          }
        }
      }
      else {
        n->right = add_node(n->right, x);
        if (height(n->right) - height(n->left) == 2) {
//        if (x >= n->right->item) {
	  if (compare(x,n->right->item)>=0) {
          	n = rotate_left(n);
          } else {
          	n = rotate2_left(n);
          } 
        }
      }
      n->height = 1 + max(height(n->left), height(n->right));
      return n;
  }
}

AVLtree::node* AVLtree::del_fixup(node* n)
{
     if(height(n->left) > height(n->right)){                    //如果左儿子高度大于右儿子高度
     if(height(n->left->left) >= height(n->left->right))//并且左儿子的左子树高度大于左儿子的右子树高度
            n=rotate_right(n);                        //那么使用 对应单旋转
        else
            n = rotate2_right(n);    //否则使用 双旋转
    }
    else{                                                                    //如果右儿子高度大于左儿子，操作是对应的
        if(height(n->right->right) >= height(n->right->left))  
            n= rotate_left(n);
        else
            n = rotate2_left(n);
    } 
    return n;
}


void AVLtree::del_node(node* RtNode,node* n,node *n_father)
{
   if(RtNode==NULL || n == NULL)
   {
     return;
   }
   if(n->left)
   {
     del_node(RtNode,n->left,n);
     if (abs(height(n->left) - height(n->right)) >= 2)
     {
        n = del_fixup(n);
     }
     n->height = 1 + max(height(n->left), height(n->right));
   }
   else{
      RtNode->item = n->item;
      if(n->right)
      {
         node* p = n->right;
         n->item =p->item;     
#ifdef AVL_TREE_FIX_MEM
         n_delete(p);
#else
         delete p;
#endif
        n->right = NULL;
      } 
      else{
         node* p = n;
#ifdef AVL_TREE_FIX_MEM
        n_delete(p);
#else
        delete p;
#endif
        n == NULL;
        n_father->left = NULL;
      }
      return;
   }
}

int AVLtree::deleteFromNode(node *pRtDel)
{
    if(pRtDel->right!=NULL)
    {
       del_node(pRtDel,pRtDel->right,pRtDel);
       if (abs(height(pRtDel->left) - height(pRtDel->right)) >= 2)
       {
          pRtDel = del_fixup(pRtDel);
       }
       pRtDel->height = 1 + max(height(pRtDel->left), height(pRtDel->right));
       return 2;
    }
    else if(pRtDel->left != NULL)
    {
       node *p = pRtDel->left;
       pRtDel->item = p->item;
#ifdef AVL_TREE_FIX_MEM
       n_delete(p);
#else
       delete p;
#endif
      pRtDel->left = NULL;
      if (abs(height(pRtDel->left) - height(pRtDel->right)) >= 2)
      {
        pRtDel = del_fixup(pRtDel);
      }
      pRtDel->height = 1 + max(height(pRtDel->left), height(pRtDel->right));
      return  1;
    }
    else{
      return 0;
   }
}



//uint AVLtree::max(uint a, uint b) {
//  if (a > b) return a;
//  return b;
//}

uint AVLtree::height(node* n) {
  if (n == NULL) return 0;
  return n->height;
}

AVLtree::node* AVLtree::rotate_left(node* n) {
  node* p;
  p = n->right;
  n->right = p->left;
  p->left = n;
  n->height = 1 + max(height(n->left), height(n->right));
  p->height = 1 + max(height(p->left), height(p->right));
  return p;
}

AVLtree::node* AVLtree::rotate2_left(node* n) {
  n->right = rotate_right(n->right);
  n = rotate_left(n);
  return n;
}

AVLtree::node* AVLtree::rotate_right(node* n) {
  node* p;
  p = n->left;
  n->left = p->right;
  p->right = n;
  n->height = 1 + max(height(n->left), height(n->right));
  p->height = 1 + max(height(p->left), height(p->right));
  return p;
}

AVLtree::node* AVLtree::rotate2_right(node* n) {
  n->left = rotate_left(n->left);
  n = rotate_right(n);
  return n;
}

int *AVLtree::search(node* n, int x) {
  if (n == NULL) return NULL;
//  if (n->item == x) return &n->item;
  int rt=compare(n->item ,x);
  if (rt==0) return &n->item;
//  if (n->item > x) {
  if (rt>0) {
    return search(n->left, x);
  } else {
    return search(n->right, x);
  }
}

int *AVLtree::search(node* n, void **ptr) {
  if (n == NULL) return NULL;
//  if (n->item == x) return &n->item;
  int rt=compare(n->item ,ptr);
  if (rt==0) return &n->item;
//  if (n->item > x) {
  if (rt>0) {
    return search(n->left, ptr);
  } else {
    return search(n->right, ptr);
  }
}

void AVLtree::FixItem(node *n,int x)
{
    if(n==NULL)
    {
       return;
    }
    if(n->item > x)
    {
      n->item--;
    }
    FixItem(n->left,x);
    FixItem(n->right,x);
}


int AVLtree::DeleteNode(node* n, int x) {
  if (n == NULL) return NULL;
//  if (n->item == x) return &n->item;
  int rt=compare(n->item ,x);
  if (rt==0) 
  {
     int nRet = deleteFromNode(n);
     return nRet;
  }
//  if (n->item > x) {
  if (rt>0) {
    if(n->left == NULL)
    {
       return 3;
    }
    int nRet = DeleteNode(n->left, x);
    if(nRet==0)
    {
      node *p = n->left;
      #ifdef AVL_TREE_FIX_MEM
        n_delete(p); 
      #else
        delete p;
      #endif
      n->left = NULL;
    }
    else if(nRet == 3)
    {
       return 3;
    }
    
  } 
  else {
     if(n->left == NULL)
     {
       return 3;
     }
     int nRet = DeleteNode(n->right, x);
     if(nRet==0)
     {
       node *p = n->right;
       #ifdef AVL_TREE_FIX_MEM
          n_delete(p);
       #else
          delete p;
       #endif
       n->right= NULL;
     }
     else if(nRet == 3)
     {
       return 3;
     }
    }
    if (abs(height(n->left) - height(n->right)) >= 2)
    {
       n = del_fixup(n);
    }
    n->height = 1 + max(height(n->left), height(n->right));
    return 2;
}



/*
void AVLtree::preorder(node* n) {
  if (n != NULL) {
    cout << n->item << " ";
    preorder(n->left);
    preorder(n->right);
  }
}
*/
void AVLtree::inorder(node* n,int *obf,int &off) {
  if (n != NULL) {
    inorder(n->left,obf,off);
	obf[off++]=n->item;
//    cout << n->item << " ";
    inorder(n->right,obf,off);
  }
}
/*
void AVLtree::postorder(node* n) {
  if (n != NULL) {
    postorder(n->left);
    postorder(n->right);
    cout << n->item << " ";
  }
}
*/
#endif
