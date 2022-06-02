if __name__ == '__main__':
    numbers = [1,2,3,4,5,6,7,8,9,10]
    required_list = []

    # Imperative Style
    for number in numbers:
        if number%2==0:
            required_list.append(number)

    print(required_list)

    # Declarative Style Anti Pattern
    required_list = filter(lambda x:x%2==0,numbers)
    print(list(required_list))

    required_list = [number for number in numbers if number%2==0]
    print(required_list)