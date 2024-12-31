package storage

import "sync"

type LRUCache struct {
	Capacity int
	mp       map[string]*linkedList
	head     *linkedList
	mu       sync.Mutex
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		Capacity: capacity,
		mp:       make(map[string]*linkedList),
		head:     nil,
		mu:       sync.Mutex{},
	}
}

func (c *LRUCache) Get(key string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	node := c.mp[key]
	if node == nil {
		return "cache miss"
	}
	c.putToFront(node)
	return node.val
}

func (c *LRUCache) Put(key string, val string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	node := c.mp[key]
	if node != nil {
		node.val = val
		c.putToFront(node)
		return
	}
	if len(c.mp) == c.Capacity {
		if c.Capacity == 1 {
			delete(c.mp, c.head.key)
			c.head = nil
		} else {
			ptr := c.head
			for ptr.next != nil && ptr.next.next != nil {
				ptr = ptr.next
			}
			if ptr.next != nil {
				delete(c.mp, ptr.next.key)
			}
			ptr.next = nil
		}
	}

	newNode := &linkedList{
		key:  key,
		val:  val,
		next: nil,
		prev: nil,
	}
	newNode.next = c.head
	if c.head != nil {
		c.head.prev = newNode
	}
	c.head = newNode
	c.mp[key] = newNode
	return
}

type linkedList struct {
	key  string
	val  string
	next *linkedList
	prev *linkedList
}

func (c *LRUCache) putToFront(node *linkedList) {
	if node == nil || node.prev == nil {
		return
	}
	prev := node.prev
	prev.next = node.next
	if node.next != nil {
		node.next.prev = prev
	}
	c.head.prev = node
	node.prev = nil
	node.next = c.head
	c.head = node
}
