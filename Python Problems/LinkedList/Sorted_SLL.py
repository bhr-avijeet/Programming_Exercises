class ListNode:
    def __init__(self, val, nxt=None):
        self.val = val
        self.nxt = nxt


class LinkedList:
    def __init__(self):
        self.root = None

    def add(self, data):
        new_node = ListNode(data)
        if self.root is None:
            self.root = new_node
            return
        elif data < self.root.val:
            self.root, new_node.nxt = new_node, self.root
            return
        last = self.root
        while last.nxt:
            if (data >= last.val) and (data < last.nxt.val):
                new_node.nxt, last.nxt = last.nxt, new_node
                return
            else:
                last = last.nxt
        last.nxt = new_node

    def remove(self, data):
        if self.root is None:
            print("Linked List is Empty")
            return
        elif self.root.val == data:
            if self.root.nxt:
                self.root = self.root.nxt
                return
            else:
                self.root = None
                return

        head = self.root

        while head.nxt:
            if (head.nxt.val == data) and (head == self.root):
                self.root.nxt = head.nxt.nxt
                return
            elif head.nxt.val == data:
                head.nxt = head.nxt.nxt
                return
            else:
                head = head.nxt
        print("Node not found")

    def print_nodes(self):
        head = self.root
        while head:
            print(head.val, end="")
            if head.nxt:
                print("-->>", end="")
            head = head.nxt

    def get_root(self):
        return self.root


if __name__ == '__main__':
    List_1 = LinkedList()
    List_2 = LinkedList()
    List_1.add(5)
    List_1.add(3)
    List_1.add(10)
    List_1.add(15)
    List_1.add(12)
    List_1.add(20)
    List_1.add(25)
    List_1.add(9)
    List_1.add(18)
    List_1.print_nodes()
    print("\n")
    List_1.remove(5)
    List_1.print_nodes()


from collections import defaultdict


