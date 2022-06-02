if __name__ == '__main__':
    set_one = set([1,2,3,4,5,5,5,6,6,7,88,99,10])
    print(set_one)

    print(set_one.pop())
    print(set_one)
    set_one.discard(16)
    print(set_one)