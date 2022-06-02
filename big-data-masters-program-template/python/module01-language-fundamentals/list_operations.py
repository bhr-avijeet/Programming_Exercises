if __name__ == '__main__':
    num = int(100)
    num = 10
    list_one = [1,2,3,4,5,6]
    print(list_one[2])
    print(list_one[-2])

    # Iterate a list
    for n in list_one:
        print(n)

    # Searching elements within lists
    print(4 in list_one)
    print(14 not in list_one)

    list_one.append(1)
    print(list_one)

    print(list_one.count(1))

    # Slicing
    print(list_one[0:3])
    list_one[1:3] = 22,33
    print(list_one)

    list_two = [10,20,30]
    list_one.append(list_two)
    print(list_one)

    list_one.insert(0,0)
    print(list_one)

    del list_one[0]
    print(list_one)

    list_three = [11,1,2,3,4,5,6,7,8,9,10]
    print(min(list_three))
    print(max(list_three))
    print("{}".format(list_three.reverse()))
    print(list_three.sort())