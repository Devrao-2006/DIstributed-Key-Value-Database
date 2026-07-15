package memtable

import (
	"sync"
)

type AVLNode struct {
	Key string
	Value string
	Left *AVLNode
	Right *AVLNode
	Height int
	Tombstone bool
}

type AVLTree struct {
	lock sync.RWMutex
	Root *AVLNode
	Size int
}

type KV struct {
	Key string
	Value string
	Tombstone bool
}

func (avl *AVLTree) GetAll() []KV {
	avl.lock.RLock()
	defer avl.lock.RUnlock()
	var result []KV

	avl.inOrder(avl.Root, &result)
	return result
}

func (avl *AVLTree) inOrder(node *AVLNode, result *[]KV) {
	if node == nil {
		return
	}
	avl.inOrder(node.Left, result)
	*result = append(*result, KV{
		Key: node.Key,
		Value: node.Value,
		Tombstone: node.Tombstone,
	})
	avl.inOrder(node.Right, result)
}

func NewAVLTree() *AVLTree {
	return &AVLTree{}
}

func (avl *AVLTree) GetCount() int {
	return avl.Size
}

func height(n *AVLNode) int {
	if n == nil {
		return 0
	}
	return n.Height
}

func getBalanceFactor(n *AVLNode) int {
	if n == nil {
		return 0
	}
	return height(n.Left) - height(n.Right)
}

func rightRotate(y *AVLNode) *AVLNode {
    x := y.Left
    T2 := x.Right

    x.Right = y
    y.Left = T2

    y.Height = max(height(y.Left), height(y.Right)) + 1
    x.Height = max(height(x.Left), height(x.Right)) + 1

    return x
}

func LeftRotate(x *AVLNode) *AVLNode {
	y := x.Right
	T2 := y.Left
	
	y.Left = x
	x.Right = T2

	y.Height = max(height(y.Left), height(y.Right)) + 1
	x.Height = max(height(x.Left), height(x.Right)) + 1

	return y
}

func (avl *AVLTree) Insert(key, value string) {
	avl.lock.Lock()
	defer avl.lock.Unlock()
	avl.Root = avl.insertRec(avl.Root, key, value, false)
}

func (avl *AVLTree) insertRec(node *AVLNode, key, value string, isTombStone bool) *AVLNode {
	if node == nil {
		avl.Size++
		return &AVLNode{
			Key: key,
			Value: value,
			Height: 1,
			Tombstone: isTombStone,
		}
	}
	if key < node.Key {
		node.Left = avl.insertRec(node.Left, key, value, isTombStone)
	} else if key > node.Key {
		node.Right = avl.insertRec(node.Right, key, value, isTombStone)
	} else {
		node.Tombstone = isTombStone
		node.Value = value
	}

	node.Height = 1 + max(height(node.Left), height(node.Right))

	balance := getBalanceFactor(node)

	// Left Left Case
	if balance > 1 && key < node.Left.Key {
		return rightRotate(node)
	}

	// Right Right Case
	if balance < -1 && key > node.Right.Key {
		return LeftRotate(node)
	}

	// Left Right Case
	if balance > 1 && key > node.Left.Key {
		node.Left = LeftRotate(node.Left)
		return rightRotate(node)
	}

	// Right Left Case
	if balance < -1 && key < node.Right.Key {
		node.Right = rightRotate(node.Right)
		return LeftRotate(node)
	}

	return node
}

func (avl *AVLTree) Get(key string) (string, bool, bool) {
	avl.lock.RLock()
	defer avl.lock.RUnlock()
	val, ok, isTombStone := avl.getRec(avl.Root, key)
	return val, ok, isTombStone
}

func (avl *AVLTree) getRec(node *AVLNode, key string) (string, bool, bool) {
	if node == nil {
		return "", false, false
	}
	if key < node.Key {
		return avl.getRec(node.Left, key)
	} else if key > node.Key {
		return avl.getRec(node.Right, key)
	} else {
		if node.Tombstone == true {
			return "", true, true
		}
		return node.Value, true, false
	}
}

func (avl *AVLTree) Delete(key string) {
	avl.lock.Lock()
	defer avl.lock.Unlock()
	avl.Root = avl.insertRec(avl.Root, key,"", true)
}