BUFFER_POOL_SIZE = 1000

class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.next = None
        self.prev = None

class LRU:
    def __init__(self, capacity=2000):
        self.capacity = capacity
        self.num = 0
        self.head = None
        self.tail = None
        self.double_list_map = {}

    def deleteNode(self, node: Node):
        if node.prev is None:
            self.head = node.next
            self.head.prev = None
        elif node.next is None:
            self.tail = node.prev
            self.tail.next = None
        else:
            # If the node is in the middle
            node.next.prev = node.prev
            node.prev.next = node.next
        del self.double_list_map[node.key]

    def addNode(self, key, value):
        newNode = Node(key, value)
        if self.num == 0:
            self.head = newNode
            self.tail = newNode
            self.head.next = self.tail
            self.tail.prev = self.head
        elif self.num == 1:
            # If there is only one node in the cache
            newNode.next = self.tail
            self.tail.prev = newNode
            self.head = newNode
        else:
            # If there are multiple nodes, add the new node to the front
            newNode.next = self.head
            self.head.prev = newNode
            self.head = newNode
        self.num += 1
        self.double_list_map[key] = newNode

    def moveToHead(self, node: Node):
        self.deleteNode(node)
        self.addNode(node.key, node.value)

    def deleteTail(self):
        pre_node = self.tail.prev
        self.deleteNode(pre_node)
        return pre_node.key

    def get(self, key):
        if key in self.double_list_map:
            # If key is found, move the corresponding node to the front
            self.moveToHead(self.double_list_map[key])
            return self.double_list_map[key].value
        return None

    # Put a new key-value pair into the cache
    def put(self, key, value):
        if key in self.double_list_map:
            # If the key is already in the cache, update its value and move to the front
            self.double_list_map[key].value = value
            self.moveToHead(self.double_list_map[key])
        else:
            # If the key is not in the cache, add a new node to the front
            self.addNode(key, value)
            # If the cache exceeds the capacity, delete the least recently used node
            if self.num > BUFFER_POOL_SIZE:
                old_key = self.deleteTail()
                del self.double_list_map[old_key]
                self.num -= 1

    # def printLRU(self): # for debugging
    #     for key in self.double_list_map:
    #         print(str(key) + " " + str(self.double_list_map[key].value))
