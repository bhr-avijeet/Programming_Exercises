class ListNode:
    def __init__(self, val, nxt=None):
        self.val = val
        self.nxt = nxt


class LinkedList:
    def __init__(self):
        self.root = None

    def add_to_list(self, data):
        new_node = ListNode(data)
        if self.root is None:
            self.root = new_node
            return
        last = self.root
        while last.nxt:
            last = last.nxt
        last.nxt = new_node

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
    List_1.add_to_list(5)
    List_1.add_to_list(10)
    List_1.add_to_list(15)
    List_2.add_to_list(3)
    List_2.add_to_list(6)
    List_2.add_to_list(9)
    List_1.print_nodes()
    print("\n")
    List_2.print_nodes()